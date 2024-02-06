from __future__ import annotations
from asyncio import Future
from dataclasses import asdict, dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set

import inflect

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from batch_processor import list_page_processors, page_processor_registry
from batch_processor import BatchPage, process_page
from batch_orchestrator_data import BatchOrchestratorInput, BatchOrchestratorResults

#
# batch_orchestrator library
# 
# This file contains the orchestator responsible for juggling pages of work and executing them in parallel.
# It works in conjunction with the BatchProcessor library
# 
# The BatchOrchestrator:
#   * Will run on a Temporal worker as a workflow--but you don't have to write the workflow yourself!  
#     You can start it from your client as you would any workflow.
#     See the [Temporal Python SDK docs](https://docs.temporal.io/dev-guide/python) for more details.
#   * Your main work will be to implement a @page_processor, so see batch_processor.py for more details.
#   * All configuration and customization is passed in with BatchOrchestratorInput.

# Paginates through a large dataset and executes it with controllable parallelism.  
@workflow.defn
class BatchOrchestrator:

    @dataclass(kw_only=True)
    class EnqueuedPage:
        class Phase(Enum):
            PENDING = 0
            PROCESSING = 1
            EXTENDED_RETRIES = 2
            COMPLETED = 3
            FAILED = 4

        page: BatchPage
        phase: Phase
        page_num: int
        did_signal_next_page: bool = False
        last_exception: Optional[BaseException] = None
        future: Optional[Future[str]] = None

        def set_processing_started(self, future: Future[str]) -> None:
            assert self.phase == BatchOrchestrator.EnqueuedPage.Phase.PENDING
            self.phase = BatchOrchestrator.EnqueuedPage.Phase.PROCESSING
            self.future = future

        def set_processing_finished(self) -> None:
            assert self.phase in [BatchOrchestrator.EnqueuedPage.Phase.PROCESSING, BatchOrchestrator.EnqueuedPage.Phase.EXTENDED_RETRIES]
            self.phase = BatchOrchestrator.EnqueuedPage.Phase.COMPLETED
            self.future = None

        def set_processing_failed_initially(self, exception: BaseException, did_signal_next_page: bool) -> None:
            assert self.phase == BatchOrchestrator.EnqueuedPage.Phase.PROCESSING
            self.phase = BatchOrchestrator.EnqueuedPage.Phase.EXTENDED_RETRIES
            self.last_exception = exception
            self.future = None
            self.did_signal_next_page = did_signal_next_page

        def set_processing_failed_permanently(self, exception: BaseException, did_signal_next_page: bool) -> None:
            assert self.phase in [BatchOrchestrator.EnqueuedPage.Phase.PROCESSING, BatchOrchestrator.EnqueuedPage.Phase.EXTENDED_RETRIES]
            self.phase = BatchOrchestrator.EnqueuedPage.Phase.FAILED
            self.last_exception = exception
            self.future = None
            self.did_signal_next_page = did_signal_next_page

        def set_pending(self) -> None:
            assert self.phase in [BatchOrchestrator.EnqueuedPage.Phase.EXTENDED_RETRIES]
            self.phase = BatchOrchestrator.EnqueuedPage.Phase.PENDING


    class PageQueue:

        # Indexes and counts for the pages we're managing
        class PageTracker:
            def __init__(self) -> None:
                self._num_pages_ever_enqueued: int = 0
                self._num_pages_processed: int = 0

                self._pending_page_nums: List[int] = []
                self._processing_page_nums: Set[int] = set()
                self._to_retry_page_nums: Set[int] = set()
                self._failed_page_nums: Set[int] = set()

            @property
            def num_pages_ever_enqueued(self) -> int:
                return self._num_pages_ever_enqueued

            @property
            def num_pages_processed(self) -> int:
                return self._num_pages_processed

            @property
            def to_retry_page_nums(self) -> Set[int]:
                return self._to_retry_page_nums.copy()
            
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
            
            def get_next_page_num(self) -> int:
                assert self._pending_page_nums
                return self._pending_page_nums[0]

            def on_page_enqueued(self, page_num: int) -> None:
                if page_num in self._to_retry_page_nums:
                    self._to_retry_page_nums.remove(page_num)
                else:
                    self._num_pages_ever_enqueued += 1
                self._pending_page_nums.append(page_num)

            def on_page_started(self, page_num: int) -> None:
                assert page_num in self._pending_page_nums
                self._pending_page_nums.remove(page_num)
                self._processing_page_nums.add(page_num)

            def on_page_completed(self, page_num: int) -> None:
                self._num_pages_processed += 1
                self._processing_page_nums.remove(page_num)

            def on_page_failed_initially(self, page_num: int) -> None:
                self._to_retry_page_nums.add(page_num)
                self._processing_page_nums.remove(page_num)

            def on_page_failed_permanently(self, page_num: int) -> None:
                self._processing_page_nums.remove(page_num)
                self._failed_page_nums.add(page_num)
                
            
        def __init__(self, input: BatchOrchestratorInput) -> None:
            self.input = input
            self.tracker = BatchOrchestrator.PageQueue.PageTracker()
            self.pages: Dict[int, BatchOrchestrator.EnqueuedPage] = {}
            self.max_parallelism_achieved: int = 0

        # Receiving new work
        def enqueue_page(self, page: BatchPage, page_num: int) -> None:
            if page_num < self.tracker.num_pages_ever_enqueued:
                ordinal = inflect.engine().ordinal(page_num + 1) # type: ignore
                workflow.logger.warning(
                    f"Got re-signaled for the {ordinal} page, but skipping because it was already signaled for. This should be rare, " +
                    "so please report an issue if it persists.",
                    {"old_cursor": self.pages[page_num].page.cursor_str, "new_cursor": page.cursor_str, page_num: page_num})
                return
            duplicate = next((enqueued_page for enqueued_page in self.pages.values() if enqueued_page.page.cursor_str == page.cursor_str), None)
            if duplicate:
                workflow.logger.warning(
                    f"Got re-signaled for the page with cursor {page.cursor_str}, but skipping because it was already signaled for. While it's possible " +
                    "for duplicate signals to be sent, it's rare.  Did you mis-compute your next cursor?",
                    {"old_page_num": duplicate.page_num, "new_page_num": page_num, "cursor": page.cursor_str})
                return

            self.pages[page_num] = BatchOrchestrator.EnqueuedPage(
                page=page,
                phase=BatchOrchestrator.EnqueuedPage.Phase.PENDING,
                page_num=page_num)
            self.tracker.on_page_enqueued(page_num)
            
        def re_enqueue_pages_to_retry(self) -> None:
            for page_num in self.tracker.to_retry_page_nums:
                self.pages[page_num].set_pending()
                self.tracker.on_page_enqueued(page_num)

        #
        # Page management
        #   
            
        def work_is_complete(self) -> bool:
            return not self.tracker.has_pending_pages and not self.tracker.has_processing_pages
        
        def is_new_page_ready(self) -> bool:
            return self.tracker.num_processing_pages < self.input.max_parallelism and self.tracker.has_pending_pages > 0

        def is_non_retryable(self, exception: BaseException) -> bool:
            if isinstance(exception, ApplicationError):
                if exception.non_retryable:
                    return True
                users_exception_type_str = exception.type
                return users_exception_type_str in (self.input.initial_retry_policy.non_retryable_error_types or set())
            else:
                # I think all other error types (e.g. timeouts) are retryable.
                return False

        def on_page_processed(self, future: Future[str], page_num: int, page: BatchPage) -> None:
            exception = future.exception()
            ordinal = inflect.engine().ordinal(page_num + 1) # type: ignore
            workflow.logger.info(f"Finished processing the {ordinal} page.  Exception {exception}")
            if exception is not None:
                assert isinstance(exception, ActivityError)
                exception = exception.__cause__
                assert exception is not None

                # If the page told us about its successor, we need to tell the page processor not to re-signal when it
                # is processed within the extended retries phase.  This will avoid extra signals filling up the workflow history.
                did_signal_next_page = (page_num + 1) in self.pages
                if did_signal_next_page:
                    signaled_text = "It signaled for more work before the failure"
                else:
                    signaled_text = "It did not signal for more work before the failure and may be blocking further progress."

                if self.is_non_retryable(exception):
                    workflow.logger.error(
                        f"Batch executor failed {page.cursor_str} page, the {ordinal} page, with a non-retryable error.",
                        {"exception": exception})
                    self.tracker.on_page_failed_permanently(page_num)
                    self.pages[page_num].set_processing_failed_permanently(exception, did_signal_next_page)
                else:
                    workflow.logger.info(
                        f"Batch executor failed to complete {page.cursor_str} page, the {ordinal} page.  {signaled_text}  Will retry.", 
                        {"exception": exception})
                    self.tracker.on_page_failed_initially(page_num)
                    self.pages[page_num].set_processing_failed_initially(exception, did_signal_next_page)

            else:
                workflow.logger.info(f"Batch executor completed {page.cursor_str}, the {ordinal} page")
                self.pages[page_num].set_processing_finished()
                self.tracker.on_page_completed(page_num)


        # Initiate processing the page and register a callback to record that it finished
        def start_page_processor_activity(self, enqueued_page: BatchOrchestrator.EnqueuedPage) -> None:
            page = enqueued_page.page
            page_num = enqueued_page.page_num
            already_tried = enqueued_page.phase == BatchOrchestrator.EnqueuedPage.Phase.PROCESSING
            future = workflow.start_activity(
                process_page, 
                args=[self.input.page_processor, page, page_num, self.input.page_processor_args, enqueued_page.did_signal_next_page], 
                start_to_close_timeout=timedelta(seconds=self.input.page_timeout_seconds),
                retry_policy=self._get_retry_policy(already_tried)
                )
            future.add_done_callback(
                lambda future: self.on_page_processed(future, page_num, page))
            enqueued_page.set_processing_started(future)
            self.tracker.on_page_started(page_num)

        #
        # Main algorithm
        #

        async def run(self) -> None:
            while not self.work_is_complete():
                # Wake up (or continue) when an activity signals us with more work, when it completes, or when 
                # we're ready to process a new page.
                await workflow.wait_condition(
                    lambda: self.is_new_page_ready() or self.work_is_complete())
                if self.is_new_page_ready():
                    self.start_page_processor_activity(self.pages[self.tracker.get_next_page_num()])
                self.max_parallelism_achieved = max(self.max_parallelism_achieved, self.tracker.num_processing_pages)

        def _get_retry_policy(self, is_extended_retries: bool) -> RetryPolicy:
            if is_extended_retries:
                return RetryPolicy(
                    backoff_coefficient=1.0,
                    initial_interval=timedelta(seconds=self.input.extended_retry_interval_seconds),
                    non_retryable_error_types=self.input.initial_retry_policy.non_retryable_error_types
                    )
            else:
                return self.input.initial_retry_policy

    #
    # Getting signals when new pages are queued for processing
    #
    @workflow.signal
    async def signal_add_page(self, page: BatchPage, page_num: int) -> None:
        now = workflow.now().strftime('%H:%M:%S.%f')

        workflow.logger.info(f"{now} Enqueuing page request for cursor {page.cursor_str}")
        self.page_queue.enqueue_page(page, page_num)

    @workflow.run
    async def run(self, input: BatchOrchestratorInput) -> BatchOrchestratorResults:
        workflow.logger.info("Starting batch executor")

        self.page_queue = BatchOrchestrator.PageQueue(input)
        self.page_queue.enqueue_page(BatchPage(input.first_cursor_str, input.page_size), 0)
        await self.page_queue.run()

        if input.use_extended_retries and self.page_queue.tracker.failed_page_nums:
            workflow.logger.info(
                f"Moving to extended retries: {len(self.page_queue.tracker.failed_page_nums)} failed to process, " +
                f"while {self.page_queue.tracker.num_pages_processed} processed successfully.")

            self.page_queue.re_enqueue_pages_to_retry()
            await self.page_queue.run()


        workflow.logger.info(f"Batch executor completed {self.page_queue.tracker.num_pages_processed} pages")

        return BatchOrchestratorResults(
            num_pages_processed=self.page_queue.tracker.num_pages_processed,
            max_parallelism_achieved=self.page_queue.max_parallelism_achieved,
            num_failed_pages=len(self.page_queue.tracker.failed_page_nums))

    class FailedBatchPage:
        def __init__(self, page_num: int, page: BatchPage, did_signal_next_page: bool, exception: BaseException) -> None:
            self.page_num = page_num
            self.page = page
            self.did_signal_next_page = did_signal_next_page
            self.exception = exception
