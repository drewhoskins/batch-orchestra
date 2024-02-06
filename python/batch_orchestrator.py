from __future__ import annotations
from asyncio import Future
from dataclasses import asdict, dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set

import inflect

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

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

def print_exception_chain(exc):
    """Recursively prints the current exception and its cause chain."""
    current_exception = exc
    while current_exception:
        print(f"Exception: {type(current_exception)} {current_exception}")
        current_exception = current_exception.__cause__


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
        page: BatchPage
        phase: Phase
        page_num: int
        did_signal_next_page: bool = False
        last_exception: Optional[BaseException] = None

    class PageQueue:

        def __init__(self, input: BatchOrchestratorInput) -> None:
            self.input = input
            self.num_pages_enqueued = 0
            self.num_pages_processed: int = 0
            self.pages: Dict[int, BatchOrchestrator.EnqueuedPage] = {}
            self.processing_pages: Dict[int, Future[str]] = {}
            self.max_parallelism_achieved: int = 0
            self.pending_pages: List[int] = []
            self.failed_pages: List[int] = []

        # Receiving new work
        def enqueue_page(self, page: BatchPage, page_num: int) -> None:
            if page_num < self.num_pages_enqueued:
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
            self.pending_pages.append(page_num)
            self.num_pages_enqueued += 1
            
        def enqueue_failed_pages(self) -> None:
            for page_num in self.failed_pages:
                self.pages[page_num].phase = BatchOrchestrator.EnqueuedPage.Phase.PENDING

            self.pending_pages = self.failed_pages
            self.failed_pages = []

        #
        # Page management
        #   
            
        def work_is_complete(self) -> bool:
            return not self.pending_pages and not self.processing_pages
        
        def is_new_page_ready(self) -> bool:
            return len(self.processing_pages) < self.input.max_parallelism and len(self.pending_pages) > 0

        def on_page_processed(self, future: Future[str], page_num: int, page: BatchPage) -> None:
            exception = future.exception()
            ordinal = inflect.engine().ordinal(page_num + 1) # type: ignore
            workflow.logger.info(f"Finished processing the {ordinal} page.  Exception {exception}")
            if exception is not None:
                assert isinstance(exception, ActivityError)
                exception = exception.__cause__
                # If the page told us about its successor, we need to tell the page processor not to re-signal when it
                # is processed within the extended retries phase.  This will avoid extra signals filling up the workflow history.
                print(f"Did signal next page: {self.pages}, {page_num + 1}")
                did_signal_next_page = (page_num + 1) in self.pages
                if did_signal_next_page:
                    signaled_text = "It signaled for more work before the failure"
                else:
                    signaled_text = "It did not signal for more work before the failure and may be blocking further progress."
                workflow.logger.info(f"Batch executor failed to complete {page.cursor_str} page, the {ordinal} page.  {signaled_text}")

                assert self.pages[page_num].phase == BatchOrchestrator.EnqueuedPage.Phase.PROCESSING
                self.failed_pages.append(page_num)
                self.pages[page_num].phase = BatchOrchestrator.EnqueuedPage.Phase.EXTENDED_RETRIES
                self.pages[page_num].did_signal_next_page = did_signal_next_page
                self.pages[page_num].last_exception = exception
            else:
                workflow.logger.info(f"Batch executor completed {page.cursor_str}, the {ordinal} page")
                self.num_pages_processed += 1
                self.pages[page_num].phase = BatchOrchestrator.EnqueuedPage.Phase.COMPLETED
            self.processing_pages.pop(page_num)

        # Initiate processing the page and register a callback to record that it finished
        def start_page_processor_activity(self, enqueued_page: BatchOrchestrator.EnqueuedPage) -> None:
            page = enqueued_page.page
            page_num = enqueued_page.page_num
            cursor_str = page.cursor_str
            self.pages[page_num].phase = BatchOrchestrator.EnqueuedPage.Phase.PROCESSING
            self.processing_pages[page_num] = workflow.start_activity(
                process_page, 
                args=[self.input.page_processor, page, page_num, self.input.page_processor_args, enqueued_page.did_signal_next_page], 
                start_to_close_timeout=timedelta(seconds=self.input.page_timeout_seconds),
                retry_policy=self._get_retry_policy(enqueued_page.phase)
                )
            self.processing_pages[page_num].add_done_callback(
                lambda future: self.on_page_processed(future, page_num, page))

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
                    next_page_num = self.pending_pages.pop()
                    self.start_page_processor_activity(self.pages[next_page_num])
                self.max_parallelism_achieved = max(self.max_parallelism_achieved, len(self.processing_pages))

        def _get_retry_policy(self, phase: BatchOrchestrator.EnqueuedPage.Phase) -> RetryPolicy:
            match phase:
                case BatchOrchestrator.EnqueuedPage.Phase.PROCESSING:
                    workflow.logger.info(f"Retry policy: {self.input.initial_retry_policy}")
                    return self.input.initial_retry_policy
                case BatchOrchestrator.EnqueuedPage.Phase.EXTENDED_RETRIES:
                    return RetryPolicy(
                        backoff_coefficient=1.0,
                        initial_interval=timedelta(seconds=self.input.extended_retry_interval_seconds),
                        non_retryable_error_types=self.input.initial_retry_policy.non_retryable_error_types
                        )
                case _: assert False, f"Unexpected phase {phase}"

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

        print(f"{self.page_queue.failed_pages} failed pages")
        if input.use_extended_retries and self.page_queue.failed_pages:
            workflow.logger.info(
                f"Moving to extended retries: {len(self.page_queue.failed_pages)} failed to process, " +
                f"while {self.page_queue.num_pages_processed} processed successfully.")

            self.page_queue.enqueue_failed_pages()
            await self.page_queue.run()


        workflow.logger.info(f"Batch executor completed {self.page_queue.num_pages_processed} pages")


        return BatchOrchestratorResults(
            num_pages_processed=self.page_queue.num_pages_processed,
            max_parallelism_achieved=self.page_queue.max_parallelism_achieved,
            num_failed_pages=len(self.page_queue.failed_pages))

    class FailedBatchPage:
        def __init__(self, page_num: int, page: BatchPage, did_signal_next_page: bool, exception: BaseException) -> None:
            self.page_num = page_num
            self.page = page
            self.did_signal_next_page = did_signal_next_page
            self.exception = exception
