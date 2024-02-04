from __future__ import annotations
from asyncio import Future
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Set

from temporalio import workflow, activity

from temporalio import workflow
import temporalio.converter
from temporalio.common import RetryPolicy
from temporalio.api.common.v1 import Payload
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.converter import (
    CompositePayloadConverter,
    DefaultPayloadConverter,
    EncodingPayloadConverter,
)

from batch_processor import list_page_processors, page_processor_registry
from batch_processor import BatchPage, BatchProcessorRetryableError, process_page


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

@dataclass(kw_only=True)
class BatchOrchestratorInput:
    batch_name: str
    # The function, annotated with @page_processor, that will be called on your worker for each page
    page_processor: str
    # Use this to manage load on your downstream dependencies such as DBs or APIs by limiting the number of pages
    # processed simultaneously.
    max_parallelism: int
    # The number of items per page, to process in series.  Choose an amount that you can comfortably
    # process within the page_timeout_seconds.
    page_size: int
    # The start_to_close_timeout of the activity that runs your page processor.
    # This should typically be within the drain allowance of the worker that runs your page processor.  That 
    # would allow your activity to finish in case of a graceful shutdown.
    page_timeout_seconds: int = 300
    # The cursor, for example a database cursor, from which to start paginating.
    # Use this if you want to start a batch from a specific cursor such as where a previous run left off or if
    # you are dividing up a large dataset into multiple batches.
    # When sdk-python supports generics, we can add support for (serializable) cursor types here.
    first_cursor_str: str = ""
    # Global arguments to pass into each page processor, such as configuration.  Many will use json to serialize.
    # Any arguments that need to vary per page should be included in your cursor.
    page_processor_args: Optional[str] = None

@dataclass
class BatchOrchestratorResults:
    num_pages_processed: int
    # You can monitor this to ensure you are getting as much parallel processing as you hoped for.
    max_parallelism_achieved: int

# Paginates through a large dataset and executes it with controllable parallelism.  
@workflow.defn
class BatchOrchestrator:

    #
    # Getting signals when new pages are queued for processing
    #
    @workflow.signal
    async def signal_add_page(self, page: BatchPage) -> None:
        now = workflow.now().strftime('%H:%M:%S.%f')

        workflow.logger.info(f"{now} Enqueuing page request for cursor {page.cursor_str}")
        self.page_queue_orchestrator.enqueue_page(page)

    @workflow.run
    async def run(self, input: BatchOrchestratorInput) -> BatchOrchestratorResults:
        workflow.logger.info("Starting batch executor")

        self.page_queue_orchestrator = BatchOrchestrator.PageQueueOrchestrator(input)
        self.page_queue_orchestrator.enqueue_page(BatchPage(input.first_cursor_str, input.page_size))
        await self.page_queue_orchestrator.run()
        return BatchOrchestratorResults(
            num_pages_processed=self.page_queue_orchestrator.num_pages_processed,
            max_parallelism_achieved=self.page_queue_orchestrator.max_parallelism_achieved)

    class PageQueueOrchestrator:

        def __init__(self, input: BatchOrchestratorInput) -> None:
            self.input = input
            self.num_pages_processed: int = 0
            self.processing_pages: Dict[int, Future[str]] = {}
            self.pending_pages: List[BatchPage] = []
            self.pages_processed: Set[str] = set()
            self.max_parallelism_achieved: int = 0
        
        # Receiving new work
        def enqueue_page(self, page: BatchPage) -> None:
            self.pending_pages.append(page)

        #
        # Page management
        #   
            
        def work_is_complete(self) -> bool:
            return not self.pending_pages and not self.processing_pages
        
        def is_new_page_ready(self, num_launched_pages: int) -> bool:
            return len(self.processing_pages) < self.input.max_parallelism and len(self.pending_pages) > 0

        def on_page_processed(self, future: Future[str], pageNum: int, page: BatchPage) -> None:
            exception = future.exception()
            if exception:
                # TODO - real error handler
                raise exception
            workflow.logger.info(f"Batch executor completed {page.cursor_str} page at index {pageNum}")
            
            self.processing_pages.pop(pageNum)
            self.num_pages_processed += 1

        # Initiate processing the page and register a callback to record that it finished
        def start_page_processor_activity(self, *, pageNum: int, page: BatchPage) -> None:
            if page.cursor_str in self.pages_processed:
                # This should be very rare since we heartbeat within page processor to cause the activity not to re-send the signal.
                workflow.logger.warn(f"Got signaled for page {page.cursor_str}, but skipping because it was already signaled for.")
                return
            self.processing_pages[pageNum] = workflow.start_activity(
                process_page, 
                args=[self.input.page_processor, page, self.input.page_processor_args], 
                start_to_close_timeout=timedelta(seconds=self.input.page_timeout_seconds))
            self.processing_pages[pageNum].add_done_callback(
                lambda future: self.on_page_processed(future, pageNum, page))
            self.pages_processed.add(page.cursor_str)

        #
        # Main algorithm
        #

        async def run(self) -> None:
            num_launched_pages = 0
            while not self.work_is_complete():
                # Wake up (or continue) when an activity signals us with more work, when it completes, or when 
                # we're ready to process a new page.
                await workflow.wait_condition(
                    lambda: self.is_new_page_ready(num_launched_pages) or self.work_is_complete())
                if self.is_new_page_ready(num_launched_pages):
                    nextPage = self.pending_pages.pop()
                    self.start_page_processor_activity(pageNum = num_launched_pages, page = nextPage)
                    num_launched_pages += 1
                self.max_parallelism_achieved = max(self.max_parallelism_achieved, len(self.processing_pages))

        def _get_retry_policy(self, phase: BatchOrchestrator.EnqueuedPage.Phase) -> RetryPolicy:
            if phase == BatchOrchestrator.EnqueuedPage.Phase.INITIAL:
                return self.input.initial_retry_policy
            else:
                return RetryPolicy(
                    backoff_coefficient=1.0,
                    initial_interval=timedelta(seconds=self.input.extended_retry_interval_seconds),
                    non_retryable_error_types=self.input.initial_retry_policy.non_retryable_error_types
                    )


    #
    # Getting signals when new pages are queued for processing
    #
    @workflow.signal
    async def signal_add_page(self, page: BatchPage) -> None:
        now = workflow.now().strftime('%H:%M:%S.%f')

        workflow.logger.info(f"{now} Enqueuing page request for cursor {page.cursor_str}")
        self.page_queue.enqueue_page(page)

    @workflow.run
    async def run(self, input: BatchOrchestratorInput) -> BatchOrchestratorResults:
        workflow.logger.info("Starting batch executor")

        self.page_queue = BatchOrchestrator.PageQueue(input)
        self.page_queue.enqueue_page(BatchPage(input.first_cursor_str, input.page_size))
        await self.page_queue.run()

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

class BatchOrchestratorEncodingPayloadConverter(EncodingPayloadConverter):
    @property
    def encoding(self) -> str:
        return "text/batch-orchestrator-encoding"

    def to_payload(self, value: Any) -> Optional[Payload]:
        if isinstance(value, BatchOrchestratorInput):
            dict_value = asdict(value)
            if dict_value["initial_retry_policy"]["initial_interval"]:
                dict_value["initial_retry_policy"]["initial_interval"] = dict_value["initial_retry_policy"]["initial_interval"].total_seconds()
            if dict_value["initial_retry_policy"]["maximum_interval"]:
                dict_value["initial_retry_policy"]["maximum_interval"] = dict_value["initial_retry_policy"]["maximum_interval"].total_seconds()

            return Payload(
                metadata={"encoding": self.encoding.encode()},
                data=json.dumps(dict_value).encode(),
            )
        else:
            return None

    def from_payload(self, payload: Payload, type_hint: Optional[Type] = None) -> Any:
        # TODO(drewhoskins) why is this assert not working?  And how can I ensure that this dataconverter doesn't get used for other types?
        # assert not type_hint or type_hint is BatchOrchestratorInput
        decoded_results = json.loads(payload.data.decode())
        if decoded_results["initial_retry_policy"]["initial_interval"]:
            decoded_results["initial_retry_policy"]["initial_interval"] = timedelta(seconds=decoded_results["initial_retry_policy"]["initial_interval"])
        if decoded_results["initial_retry_policy"]["maximum_interval"]:
            decoded_results["initial_retry_policy"]["maximum_interval"] = timedelta(seconds=decoded_results["initial_retry_policy"]["maximum_interval"])
        decoded_results["initial_retry_policy"] = RetryPolicy(**decoded_results["initial_retry_policy"])
        return BatchOrchestratorInput(**decoded_results)


class BatchOrchestratorPayloadConverter(CompositePayloadConverter):
    def __init__(self) -> None:
        # Just add ours as first before the defaults
        super().__init__(
            BatchOrchestratorEncodingPayloadConverter(),
            # TODO(drewhoskins): Update this to make use of fix for https://github.com/temporalio/sdk-python/issues/139
            *DefaultPayloadConverter().converters.values(),
        )

# Use the default data converter, but change the payload converter.
batch_orchestrator_data_converter = dataclasses.replace(
    temporalio.converter.default(),
    payload_converter_class=BatchOrchestratorPayloadConverter,
)
