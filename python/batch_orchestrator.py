from __future__ import annotations
from asyncio import Future
from dataclasses import asdict, dataclass
from datetime import timedelta
from enum import Enum
import logging
from typing import Any, Dict, List, Optional, Set

import inflect

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

from batch_processor import list_page_processors
from batch_processor import BatchPage, process_page
from batch_orchestrator_data import BatchOrchestratorInput, BatchOrchestratorProgress
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
#   * Your main work will be to implement a @page_processor, so see batch_processor.py for more details.
#   * All configuration and customization is passed in with BatchOrchestratorInput.

# Pages through a large dataset and executes it with controllable parallelism.  
@workflow.defn
class BatchOrchestrator:

    # Run this to process a batch of work with controlled parallelism and in a fault-tolerant way.
    # See the docs for BatchOrchestratorInput for customizations.
    # You must add the 
    # batch_orchestrator_data_converter to your client as follows:
    # client = await Client.connect("localhost:7233", data_converter=batch_orchestrator_data_converter)
    # Then invoke it and await for it as you would any Temporal workflow, for example: 
    # handle = await client.start_workflow(
    #   BatchOrchestrator.run, 
    #   BatchOrchestratorInput(
    #     batch_id="my_batch_id", 
    #     page_processor_name=my_page_processor.__name__, 
    #     max_parallelism=5,
    #     page_size=page_size), 
    #   id=f"my_workflow_id-{str(uuid.uuid4())}", 
    #   task_queue="my-task-queue")
    # await handle.result()
    @workflow.run
    async def run(self, input: BatchOrchestratorInput) -> BatchOrchestratorProgress:
        self._run_init(input)

        self.start_progress_tracker()

        self.logger.info("Starting batch.")
        self.page_queue.enqueue_page(BatchPage(input.page_processor.first_cursor_str, input.page_processor.page_size), 0)
        await self.page_queue.run()

        if input.page_processor.use_extended_retries and self.page_queue.page_tracker.stuck_page_nums:
            self.logger.info(
                f"Moving to extended retries: {len(self.page_queue.page_tracker.stuck_page_nums)} pages are stuck, " +
                f"while {self.page_queue.page_tracker.num_completed_pages} processed successfully.")

            self.page_queue.re_enqueue_stuck_pages()
            await self.page_queue.run()

        self.logger.info(f"Batch executor completed {self.page_queue.page_tracker.num_completed_pages} pages")

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

    @dataclass(kw_only=True)
    class EnqueuedPage:
        class Phase(Enum):
            PENDING = 0
            PROCESSING = 1
            # To avoid gumming up the queue, we won't retry it until all other pages are completed or stuck
            AWAITING_PROCESSING = 2
            PENDING_EXTENDED_RETRIES = 3
            PROCESSING_EXTENDED_RETRIES = 4
            COMPLETED = 5
            FAILED = 6

        page: BatchPage
        phase: Phase
        page_num: int
        did_signal_next_page: bool = False
        last_exception: Optional[BaseException] = None
        future: Optional[Future[str]] = None

        def set_processing_started(self, future: Future[str]) -> None:
            match self.phase:
                case BatchOrchestrator.EnqueuedPage.Phase.PENDING:
                    self.phase = BatchOrchestrator.EnqueuedPage.Phase.PROCESSING
                case BatchOrchestrator.EnqueuedPage.Phase.PENDING_EXTENDED_RETRIES:
                    self.phase = BatchOrchestrator.EnqueuedPage.Phase.PROCESSING_EXTENDED_RETRIES
                case _:
                    assert False, f"Cannot transition to processing from unexpected phase {self.phase}"
            self.future = future

        def set_processing_finished(self) -> None:
            assert self.phase in [BatchOrchestrator.EnqueuedPage.Phase.PROCESSING, BatchOrchestrator.EnqueuedPage.Phase.PROCESSING_EXTENDED_RETRIES]
            self.phase = BatchOrchestrator.EnqueuedPage.Phase.COMPLETED
            self.future = None

        def set_processing_got_stuck(self, exception: BaseException, did_signal_next_page: bool) -> None:
            assert self.phase == BatchOrchestrator.EnqueuedPage.Phase.PROCESSING
            self.phase = BatchOrchestrator.EnqueuedPage.Phase.AWAITING_PROCESSING
            self.last_exception = exception
            self.future = None
            self.did_signal_next_page = did_signal_next_page

        def set_processing_failed(self, exception: BaseException, did_signal_next_page: bool) -> None:
            assert self.phase in [BatchOrchestrator.EnqueuedPage.Phase.PROCESSING, BatchOrchestrator.EnqueuedPage.Phase.PROCESSING_EXTENDED_RETRIES]
            self.phase = BatchOrchestrator.EnqueuedPage.Phase.FAILED
            self.last_exception = exception
            self.future = None
            self.did_signal_next_page = did_signal_next_page

        def set_pending(self) -> None:
            assert self.phase in [BatchOrchestrator.EnqueuedPage.Phase.AWAITING_PROCESSING]
            self.phase = BatchOrchestrator.EnqueuedPage.Phase.PENDING_EXTENDED_RETRIES

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
            def __init__(self, max_parallelism: int) -> None:
                self._max_parallelism = max_parallelism

                self._num_pages_ever_enqueued: int = 0
                self._num_completed_pages: int = 0
                self._max_parallelism_achieved: int = 0

                self._pending_page_nums: List[int] = []
                self._processing_page_nums: Set[int] = set()
                self._stuck_page_nums: Set[int] = set()
                self._failed_page_nums: Set[int] = set()
                self._is_finished: bool = False

            #
            # Status fields
            # 
            def work_is_complete(self) -> bool:
                return not self.has_pending_pages and not self.has_processing_pages

            def is_new_page_ready(self) -> bool:
                return self.num_processing_pages < self._max_parallelism and self.has_pending_pages > 0

            def get_next_page_num(self) -> int:
                assert self._pending_page_nums
                return self._pending_page_nums[0]

            # 
            # Count-based properties
            # 
            @property
            def num_pages_ever_enqueued(self) -> int:
                return self._num_pages_ever_enqueued

            @property
            def num_completed_pages(self) -> int:
                return self._num_completed_pages

            @property
            def stuck_page_nums(self) -> Set[int]:
                return self._stuck_page_nums.copy()
            
            @property
            def failed_page_nums(self) -> Set[int]:
                return self._failed_page_nums.copy()

            @property
            def has_processing_pages(self) -> bool:
                return bool(self._processing_page_nums) 

            @property
            def has_pending_pages(self) -> bool:
                return bool(self._pending_page_nums)
            
            @property
            def num_processing_pages(self) -> int:
                return len(self._processing_page_nums)
            
            @property
            def max_parallelism(self) -> int:
                return self._max_parallelism

            @property
            def max_parallelism_achieved(self) -> int:
                return self._max_parallelism_achieved
            
            @property
            def is_finished(self) -> bool:
                return self._is_finished

            # 
            # Triggers to track changes
            #
            def on_page_enqueued(self, page_num: int) -> None:
                if not page_num in self._stuck_page_nums:
                    self._num_pages_ever_enqueued += 1
                self._pending_page_nums.append(page_num)

            def on_page_started(self, page_num: int) -> None:
                assert page_num in self._pending_page_nums
                self._pending_page_nums.remove(page_num)
                self._processing_page_nums.add(page_num)
                self._max_parallelism_achieved = max(self._max_parallelism_achieved, self.num_processing_pages)

            def on_page_completed(self, page_num: int) -> None:
                assert page_num in self._processing_page_nums
                if page_num in self._stuck_page_nums:
                    self._stuck_page_nums.remove(page_num)
                self._processing_page_nums.remove(page_num)
                self._num_completed_pages += 1

            def on_page_got_stuck(self, page_num: int) -> None:
                assert page_num in self._processing_page_nums
                self._processing_page_nums.remove(page_num)
                self._stuck_page_nums.add(page_num)

            def on_page_failed(self, page_num: int) -> None:
                assert page_num in self._processing_page_nums
                self._processing_page_nums.remove(page_num)
                self._failed_page_nums.add(page_num)

            def on_finished(self) -> None:
                assert not self._processing_page_nums
                assert not self._stuck_page_nums
                assert not self._pending_page_nums
                self._is_finished = True
                            
        def __init__(self, input: BatchOrchestratorInput, logger: BatchOrchestrator.LoggerAdapter) -> None:
            self.input = input
            self.page_tracker = BatchOrchestrator.PageQueue.PageTracker(self.input.max_parallelism)
            self.pages: Dict[int, BatchOrchestrator.EnqueuedPage] = {}
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

            self.pages[page_num] = BatchOrchestrator.EnqueuedPage(
                page=page,
                phase=BatchOrchestrator.EnqueuedPage.Phase.PENDING,
                page_num=page_num)
            self.page_tracker.on_page_enqueued(page_num)
            
        def re_enqueue_stuck_pages(self) -> None:
            for page_num in self.page_tracker.stuck_page_nums:
                self.pages[page_num].set_pending()
                self.page_tracker.on_page_enqueued(page_num)
  
        def is_non_retryable(self, exception: BaseException) -> bool:
            if isinstance(exception, ApplicationError):
                if exception.non_retryable:
                    return True
                users_exception_type_str = exception.type
                return users_exception_type_str in (self.input.page_processor.initial_retry_policy.non_retryable_error_types or set())
            else:
                return False

        def on_page_failed(self, page: BatchPage, page_num: int, exception: BaseException) -> None:

            # If the page told us about its successor, we need to tell the page processor not to re-signal when it
            # is processed within the extended retries phase.  This will avoid extra signals filling up the workflow history.
            did_signal_next_page = (page_num + 1) in self.pages
            if did_signal_next_page:
                signaled_text = "It signaled for the next page before it got stuck."
            else:
                signaled_text = "It did not signal with the next page before the failure and may be blocking further progress."

            if self.is_non_retryable(exception):
                self.logger.error(
                    f"Batch executor failed {self.logger.describe_page(page_num, page)}, with a non-retryable error.",
                    {"exception": exception})
                self.page_tracker.on_page_failed(page_num)
                self.pages[page_num].set_processing_failed(exception, did_signal_next_page)
            else:
                self.logger.info(
                    f"Batch orchestrator got stuck trying {self.logger.describe_page(page_num, page)}.  {signaled_text}  Will retry.", 
                    {"exception": exception})
                self.page_tracker.on_page_got_stuck(page_num)
                self.pages[page_num].set_processing_got_stuck(exception, did_signal_next_page)

        def on_page_processed(self, future: Future[str], page_num: int, page: BatchPage) -> None:
            exception = future.exception()
            if exception is not None:
                assert isinstance(exception, ActivityError)
                exception = exception.__cause__
                assert exception is not None
                self.on_page_failed(page, page_num, exception)
            else:
                self.logger.info(f"Batch orchestrator completed {self.logger.describe_page(page_num, page)}.")
                self.pages[page_num].set_processing_finished()
                self.page_tracker.on_page_completed(page_num)

        # Initiate processing the page and register a callback to record that it finished
        def start_page_processor_activity(self, enqueued_page: BatchOrchestrator.EnqueuedPage) -> None:
            page = enqueued_page.page
            page_num = enqueued_page.page_num
            already_tried = enqueued_page.phase == BatchOrchestrator.EnqueuedPage.Phase.PENDING_EXTENDED_RETRIES
            future = workflow.start_activity(
                process_page, 
                args=[self.input.page_processor.name, self.input.batch_id, page, page_num, self.input.page_processor.args, enqueued_page.did_signal_next_page], 
                start_to_close_timeout=timedelta(seconds=self.input.page_processor.timeout_seconds),
                retry_policy=self._build_retry_policy(already_tried)
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
            while not self.page_tracker.work_is_complete():
                # Wake up (or continue) when an activity signals us with more work, when it completes, or when 
                # we're ready to process a new page.
                await workflow.wait_condition(
                    lambda: self.page_tracker.is_new_page_ready() or self.page_tracker.work_is_complete())
                if self.page_tracker.is_new_page_ready():
                    self.start_page_processor_activity(self.pages[self.page_tracker.get_next_page_num()])

        def _build_retry_policy(self, is_extended_retries: bool) -> RetryPolicy:
            if is_extended_retries:
                return RetryPolicy(
                    backoff_coefficient=1.0,
                    initial_interval=timedelta(seconds=self.input.page_processor.extended_retry_interval_seconds),
                    non_retryable_error_types=self.input.page_processor.initial_retry_policy.non_retryable_error_types,
                    maximum_attempts=0 # Infinite
                    )
            else:
                return self.input.page_processor.initial_retry_policy

    def _run_init(self, input: BatchOrchestratorInput) -> None:
        self.input = input
        self.logger = BatchOrchestrator.LoggerAdapter(input)
        self.page_queue = BatchOrchestrator.PageQueue(input, self.logger)
        self.start_time = workflow.now()
