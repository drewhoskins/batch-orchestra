from __future__ import annotations
from asyncio import Future
from dataclasses import asdict, dataclass, field
from datetime import timedelta
from enum import Enum
import logging
from typing import Any, Dict, List, Optional, Set, Type

import inflect

from internal.state import ContinueAsNewState, EnqueuedPage, PageTrackerData
from temporalio import workflow
import temporalio
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

from batch_processor import PageProcessor, get_page_processor, list_page_processors
from batch_processor import BatchPage, process_page
from batch_orchestrator_io import BatchOrchestratorInput, BatchOrchestratorProgress
from batch_tracker import track_batch_progress

#
# batch_orchestrator library
# 
# This file contains the orchestrator responsible for juggling pages of work and executing them in parallel.
# It works in conjunction with the BatchProcessor library
# 
# The BatchOrchestrator:
#   * Will run on a Temporal worker as a workflow--but you don't have to write the workflow yourself!  
#     You can start it from your client as you would any workflow.
#     See the [Temporal Python SDK docs](https://docs.temporal.io/dev-guide/python) for more details.
#   * Your main work will be to implement a @page_processor, so see     batch_processor.py for more details.
#   * All configuration and customization is passed in with BatchOrchestratorInput.

# Sandbox is off so we can import the user's @page_processor classes.  We might could make this more selective
# (and therefore safe) by allowing the user to specify modules to import.
@workflow.defn(sandboxed=False) 
class BatchOrchestrator:

    # Run this to process a batch of work with controlled parallelism and in a fault-tolerant way.
    # See the docs for BatchOrchestratorInput for customizations.
    # Then invoke it like so:
    # temporal_client = await Client.connect("localhost:7233")
    # handle: WorkflowHandle[Any, BatchOrchestratorProgress] = await BatchOrchestratorClient(temporal_client).start(    
    #   BatchOrchestratorInput(
    #     batch_id="my_batch_id", 
    #     page_processor_name=my_page_processor.__name__, 
    #     max_parallelism=5,
    #     page_size=page_size), 
    #   id=f"my_workflow_id-{str(uuid.uuid4())}", 
    #   task_queue="my-task-queue")
    # Now wait for the workflow to finish.  Or see BatchOrchestratorClient for all your other options.
    # results = await handle.result()
    @workflow.run
    async def run(self, input: BatchOrchestratorInput, state: Optional[ContinueAsNewState]) -> BatchOrchestratorProgress:
        print(f"Running batch orchestrator with input: {input} and state: {state}.")
        self._run_init(input=input, state=state)

        self.start_progress_tracker()

        self.logger.info("Starting batch.")
        if not state:
            first_page = BatchPage(input.page_processor.first_cursor_str, input.page_processor.page_size)
            self.page_queue.enqueue_page(first_page, 0)
        await self.page_queue.run()

        if self.page_processor.use_extended_retries and self.page_queue.page_tracker.stuck_page_nums:
            self.logger.info(
                f"Moving to extended retries: {len(self.page_queue.page_tracker.stuck_page_nums)} pages are stuck, " +
                f"while {self.page_queue.page_tracker.num_completed_pages} processed successfully.")

            self.page_queue.re_enqueue_stuck_pages()
            await self.page_queue.run()

        self.logger.info(f"BatchOrchestrator completed {self.page_queue.page_tracker.num_completed_pages} pages")

        self.page_queue.page_tracker.on_finished()
        await self.finalize_progress_tracker()
        return self.current_progress()

    # Query the current progress of the batch.  If you know how many records you have, you can even provide a progress bar.
    # Invoke it as you would any Temporal query.  For example
    # handle = await client.start_workflow(...) (as documented in the run method)
    # progress = await handle.query(BatchOrchestrator.current_progress)
    @workflow.query
    def current_progress(self) -> BatchOrchestratorProgress:
        return BatchOrchestratorProgress(
            num_completed_pages=self.page_queue.page_tracker.num_completed_pages,
            max_parallelism_achieved=self.page_queue.page_tracker.max_parallelism_achieved,
            num_processing_pages=self.page_queue.page_tracker.num_processing_pages,
            num_stuck_pages=len(self.page_queue.page_tracker.stuck_page_nums),
            num_failed_pages=len(self.page_queue.page_tracker.failed_page_nums),
            is_finished=self.page_queue.page_tracker.is_finished,
            _start_timestamp=self.start_time.timestamp())
    
    # Use this to pause the batch (by setting it to 0) or otherwise increase/decrease the number of 
    # @page_processors that can execute at once.
    # Also "pushes" the old parallelism onto a stack so that you can restore_max_parallelism it later.
    @workflow.signal
    async def set_max_parallelism(self, max_parallelism: int) -> None:
        self.logger.info(f"Changing max_parallelism to {max_parallelism} from {self.page_queue.page_tracker.max_parallelism}.")
        self.page_queue.page_tracker.update_max_parallelism(max_parallelism)

    # "pops" to the previous max_parallelism value, if there was one.
    @workflow.signal
    async def restore_max_parallelism(self) -> None:
        current_parallelism = self.page_queue.page_tracker.max_parallelism
        new_parallelism = self.page_queue.page_tracker.restore_max_parallelism()
        self.logger.info(f"Restoring max_parallelism to {new_parallelism} from {current_parallelism}.")

    # Receives signals that new pages are ready to process and enqueues them.
    # Don't call this directly; call context.enqueue_next_page() from your @page_processor.
    @workflow.signal
    async def _signal_add_page(self, page: BatchPage, page_num: int) -> None:
        self.logger.info(f"Enqueuing {self.logger.describe_page(page_num, page)}.")
        self.page_queue.enqueue_page(page, page_num)

    # Starts a user-specified background activity to track the progress of the batch.
    def start_progress_tracker(self) -> Optional[Future[None]]:
        self._progress_tracker: Optional[Future[None]] = None
        if self.input.batch_tracker is not None:
            self._progress_tracker = workflow.start_activity(
                track_batch_progress, 
                args=[self.input.batch_tracker.name, self.input.batch_id, self.input.batch_tracker.args], 
                start_to_close_timeout=timedelta(seconds=self.input.batch_tracker.timeout_seconds),
                retry_policy=RetryPolicy(backoff_coefficient=1, initial_interval=timedelta(seconds=self.input.batch_tracker.polling_interval_seconds))
                )
        return self._progress_tracker
    
    # Call once more when we're finished.
    async def finalize_progress_tracker(self) -> None:
        if self._progress_tracker is not None:
            # Cancel so that we don't have to wait for the next polling interval before exiting the workflow.
            self._progress_tracker.cancel()
            try:
                await self._progress_tracker
            except ActivityError as e:
                if isinstance(e.__cause__, CancelledError):
                    self.logger.info("Progress tracker was cancelled.")
                    # Now invoke the tracker one last time (without polling) so the developer can get a final update.
                    tracker_future = self.start_progress_tracker()
                    assert tracker_future is not None
                    await tracker_future
                else:
                    raise e

    class LoggerAdapter(workflow.LoggerAdapter):
        def __init__(self, input: BatchOrchestratorInput) -> None:
            self._batch_id = input.batch_id
            super().__init__(logging.getLogger(__name__), {})

        def process(self, msg, kwargs):
            msg, kwargs = super().process(msg, kwargs)
            if self._batch_id != '':
                extra_data = {'batch_id': self._batch_id}
                if 'extra' in kwargs:
                    kwargs['extra'].update(extra_data)
                else:
                    kwargs['extra'] = extra_data
            return msg, kwargs

        def describe_page(self, page_num: int, page: BatchPage) -> str:
            ordinal = inflect.engine().ordinal(page_num + 1) # type: ignore
            return f"the page with cursor {page.cursor_str}, the {ordinal} page"
    
    class PageQueue:

        # Indexes and counts for the pages we're managing
        class PageTracker:

            def __init__(self, data: PageTrackerData, *, pages_per_run: Optional[int]) -> None:
                self.data: PageTrackerData = data
                self._num_pages_enqueued_in_this_run = len(data.pending_page_nums)
                self._pages_per_run = pages_per_run

            def update_max_parallelism(self, max_parallelism: int) -> None:
                self.data.previous_max_parallelisms.append(self.data.max_parallelism)
                self.data.max_parallelism = max_parallelism
            
            # Returns the previous (and new) max parallelism.
            def restore_max_parallelism(self) -> int:
                if self.data.previous_max_parallelisms:
                    self.data.max_parallelism = self.data.previous_max_parallelisms.pop()
                return self.data.max_parallelism

            #
            # Status fields
            # 
            def work_is_complete(self) -> bool:
                return (not self.has_pending_pages or self.should_continue_as_new()) and not self.has_processing_pages

            def is_new_page_ready(self) -> bool:
                return self.num_processing_pages < self.data.max_parallelism and self.has_pending_pages > 0

            def get_next_page_num(self) -> int:
                assert self.data.pending_page_nums
                return self.data.pending_page_nums[0]

            def should_continue_as_new(self) -> bool:
                if workflow.info().is_continue_as_new_suggested():
                    return True
                workflow.logger.info(f"pages_per_run: {self._pages_per_run}, num_pages_enqueued_in_this_run: {self._num_pages_enqueued_in_this_run} {self.data}")
                if not self._pages_per_run:
                    return False
                return self._num_pages_enqueued_in_this_run > self._pages_per_run

            # 
            # Count-based properties
            # 
            @property
            def num_pages_ever_enqueued(self) -> int:
                return self.data.num_pages_ever_enqueued
            
            @property
            def num_completed_pages(self) -> int:
                return self.data.num_completed_pages

            @property
            def stuck_page_nums(self) -> Set[int]:
                return self.data.stuck_page_nums.copy()
            
            @property
            def failed_page_nums(self) -> Set[int]:
                return self.data.failed_page_nums.copy()

            @property
            def has_processing_pages(self) -> bool:
                return bool(self.data.processing_page_nums) 

            @property
            def has_pending_pages(self) -> bool:
                return bool(self.data.pending_page_nums)
            
            @property
            def num_processing_pages(self) -> int:
                return len(self.data.processing_page_nums)
            
            @property
            def max_parallelism(self) -> int:
                return self.data.max_parallelism

            @property
            def max_parallelism_achieved(self) -> int:
                return self.data.max_parallelism_achieved
            
            @property
            def is_finished(self) -> bool:
                return self.data.is_finished

            # 
            # Triggers to track changes
            #
            def on_page_enqueued(self, page_num: int) -> None:
                if not page_num in self.data.stuck_page_nums:
                    self.data.num_pages_ever_enqueued += 1
                    self._num_pages_enqueued_in_this_run += 1
                self.data.pending_page_nums.append(page_num)

            def on_page_started(self, page_num: int) -> None:
                assert page_num in self.data.pending_page_nums
                self.data.pending_page_nums.remove(page_num)
                self.data.processing_page_nums.add(page_num)
                self.data.max_parallelism_achieved = max(self.data.max_parallelism_achieved, self.num_processing_pages)

            def on_page_completed(self, page_num: int) -> None:
                assert page_num in self.data.processing_page_nums
                if page_num in self.data.stuck_page_nums:
                    self.data.stuck_page_nums.remove(page_num)
                self.data.processing_page_nums.remove(page_num)
                self.data.num_completed_pages += 1

            def on_page_got_stuck(self, page_num: int) -> None:
                assert page_num in self.data.processing_page_nums
                self.data.processing_page_nums.remove(page_num)
                self.data.stuck_page_nums.add(page_num)

            def on_page_failed(self, page_num: int) -> None:
                assert page_num in self.data.processing_page_nums
                if page_num in self.data.stuck_page_nums:
                    self.data.stuck_page_nums.remove(page_num)
                self.data.processing_page_nums.remove(page_num)
                self.data.failed_page_nums.add(page_num)

            def on_finished(self) -> None:
                assert not self.data.processing_page_nums
                assert not self.data.stuck_page_nums
                assert not self.data.pending_page_nums
                self.data.is_finished = True
                            
        def __init__(
                self, 
                *,
                input: BatchOrchestratorInput, 
                logger: Type['BatchOrchestrator.LoggerAdapter'], 
                state: Optional[ContinueAsNewState]) -> None:
            self.input = input
            self.page_processor: PageProcessor = get_page_processor(input.page_processor.name)
            if state:
                page_tracker_data = state.page_tracker_data
                # json serializesthe keys as strings.
                self.pages = {int(k): v for k, v in state.pages.items()}
            else:
                page_tracker_data = PageTrackerData(max_parallelism=self.input.max_parallelism)
                self.pages: Dict[int, EnqueuedPage] = {}
            self.page_tracker = BatchOrchestrator.PageQueue.PageTracker(page_tracker_data, pages_per_run=self.input.pages_per_run)
            self.logger = logger

        # Receive new work from a page processor
        def enqueue_page(self, page: BatchPage, page_num: int) -> None:
            if page_num < self.page_tracker.num_pages_ever_enqueued:
                self.logger.warning(
                    f"Got re-signaled for {self.logger.describe_page(page_num, page)}, but skipping because it was already signaled for. " +
                    "This should be rare, so please report an issue if it persists.",
                    {"old_cursor": self.pages[page_num].page.cursor_str, "new_cursor": page.cursor_str, page_num: page_num})
                return
            duplicate = next((enqueued_page for enqueued_page in self.pages.values() if enqueued_page.page.cursor_str == page.cursor_str), None)
            if duplicate:
                self.logger.warning(
                    f"Got re-signaled for {self.logger.describe_page(page_num, page)}, but skipping because it was already signaled for. " +
                    "While it's possible for duplicate signals to be sent, it's rare. Did you mis-compute your next cursor?",
                    {"old_page_num": duplicate.page_num, "new_page_num": page_num, "cursor": page.cursor_str})
                return

            self.pages[page_num] = EnqueuedPage(
                page=page,
                page_num=page_num)
            self.page_tracker.on_page_enqueued(page_num)
            
        def re_enqueue_stuck_pages(self) -> None:
            for page_num in self.page_tracker.stuck_page_nums:
                self.page_tracker.on_page_enqueued(page_num)
  
        def is_non_retryable(self, exception: BaseException) -> bool:
            if isinstance(exception, ApplicationError):
                if exception.non_retryable:
                    return True
                users_exception_type_str = exception.type
                return users_exception_type_str in (self.page_processor.initial_retry_policy.non_retryable_error_types or set())
            else:
                return False

        def on_page_failing(self, page: BatchPage, page_num: int, exception: BaseException) -> None:
            # If the page told us about its successor, we need to tell the page processor not to re-signal when it
            # is processed within the extended retries phase.  This will avoid extra signals filling up the workflow history.
            did_signal_next_page = (page_num + 1) in self.pages
            if did_signal_next_page:
                signaled_text = "It signaled for the next page before it got stuck."
            else:
                signaled_text = "It did not signal with the next page before the failure and may be blocking further progress."

            should_extended_retry = self.page_processor.use_extended_retries and not self.is_non_retryable(exception) and not self.pages[page_num].is_stuck
            if should_extended_retry:
                self.logger.info(
                    f"Batch orchestrator got stuck trying {self.logger.describe_page(page_num, page)}. {signaled_text} Will retry during extended retries.", 
                    {"exception": exception})
                self.page_tracker.on_page_got_stuck(page_num)
            else:
                if not self.page_processor.use_extended_retries:
                    explanation = f"Will not retry because {self.page_processor.__class__.__name__}().use_extended_retries is False."
                elif self.is_non_retryable(exception):
                    explanation = f"Will not retry because {exception.type} is non-retryable."
                else:
                    assert self.pages[page_num].is_stuck
                    explanation = "Will not retry because it hard failed within extended retries (perhaps due to a workflow timeout?)."
                self.logger.error(
                    f"BatchOrchestrator failed {self.logger.describe_page(page_num, page)}, permanently. {signaled_text} {explanation}",
                    {"exception": exception})
                self.page_tracker.on_page_failed(page_num)

            self.pages[page_num].set_processing_got_stuck(exception, did_signal_next_page)

        def on_page_processed(self, future: Future[str], page_num: int, page: BatchPage) -> None:
            exception = future.exception()
            self.logger.info(f"On page processed {page_num} {page} {exception}")
            if exception is not None:
                assert isinstance(exception, ActivityError)
                exception = exception.__cause__
                assert exception is not None
                self.on_page_failing(page, page_num, exception)
            else:
                self.logger.info(f"Batch orchestrator completed {self.logger.describe_page(page_num, page)}.")
                self.pages[page_num].set_processing_finished()
                self.page_tracker.on_page_completed(page_num)

        # Initiate processing the page and register a callback to record that it finished
        def start_page_processor_activity(self, enqueued_page: EnqueuedPage) -> None:
            page = enqueued_page.page
            page_num = enqueued_page.page_num
            already_tried = enqueued_page.is_stuck
            workflow.logger.info(f"Starting page processor for {self.logger.describe_page(page_num, page)}.  Already tried: {already_tried}.")
            future = workflow.start_activity(
                process_page, 
                args=[self.input.page_processor.name, self.input.batch_id, page, page_num, self.input.page_processor.args, enqueued_page.did_signal_next_page], 
                start_to_close_timeout=timedelta(seconds=self.input.page_processor.timeout_seconds),
                retry_policy=self._build_retry_policy(self.page_processor, already_tried)
                )
            future.add_done_callback(
                lambda future: self.on_page_processed(future, page_num, page))
            enqueued_page.set_processing_started(future)
            self.page_tracker.on_page_started(page_num)

        #
        # Seed the queue with pending entries, then run this.  
        # It will run until all pages are completed, failed, or to-retry.
        #
        async def run(self) -> None:
            while (self.page_tracker.has_pending_pages and not self.page_tracker.should_continue_as_new()) or self.page_tracker.has_processing_pages:
                # Wake up (or continue) when an activity signals us with more work, when it completes, or when 
                # we're ready to process a new page.
                await workflow.wait_condition(
                    lambda: (self.page_tracker.is_new_page_ready() and not self.page_tracker.should_continue_as_new()) or self.page_tracker.work_is_complete())
                if self.page_tracker.is_new_page_ready() and not self.page_tracker.should_continue_as_new():
                    self.start_page_processor_activity(self.pages[self.page_tracker.get_next_page_num()])
            if self.page_tracker.has_pending_pages:
                workflow.continue_as_new(
                    args=[self.input, ContinueAsNewState(page_tracker_data=self.page_tracker.data, pages=self.pages)])

        def _build_retry_policy(self, page_processor: PageProcessor, is_extended_retries: bool) -> RetryPolicy:
            if is_extended_retries:
                return RetryPolicy(
                    backoff_coefficient=1.0,
                    initial_interval=timedelta(seconds=self.page_processor.extended_retry_interval_seconds),
                    non_retryable_error_types=page_processor.initial_retry_policy.non_retryable_error_types,
                    maximum_attempts=0 # Infinite
                    )
            else:
                return page_processor.initial_retry_policy

    def _run_init(self, *, input: BatchOrchestratorInput, state: Optional[ContinueAsNewState]) -> None:
        self.input = input
        self.logger = BatchOrchestrator.LoggerAdapter(input)
        self.page_queue = BatchOrchestrator.PageQueue(input=input, logger=self.logger, state=state)
        self.start_time = workflow.now()
        self.page_processor: PageProcessor = get_page_processor(input.page_processor.name)

