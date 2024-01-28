from asyncio import Future
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Generic, List, Optional, TypeVar

from temporalio import workflow, activity

import batch_page_processor_context 
from batch_orchestrator_page import BatchOrchestratorPage
from batch_page_processor_registry import list_page_processors, registry as batch_page_processor_registry


@dataclass(kw_only=True)
class BatchOrchestratorInput:
    batch_name: str
    # The function, annotated with @page_processor, that will be called on your worker for each page
    page_processor: str
    # Use this to manage load on your downstream dependencies such as DBs or APIs by limiting the number of pages
    # processed simultaneously.
    max_parallelism: int
    # The cursor, for example a database cursor, from which to start paginating.
    # Use this if you want to start a batch from a specific cursor such as where a previous run left off or if
    # you are dividing up a large dataset into multiple batches.
    cursor: str

# Paginates through a large dataset and executes it with controllable parallelism.  
@workflow.defn
class BatchOrchestrator:

    @dataclass
    class Results:
        num_pages_processed: int
        # You can monitor this to ensure you are getting as much parallel processing as you hoped for.
        max_parallelism_achieved: int

    def __init__(self):
        self.num_pages_processed: int = 0
        self.processing_pages: Dict[int, Future[str]] = {}
        self.pending_pages: List[BatchOrchestratorPage] = []
        self.max_pages: int = 1000 # TODO measure a good limit
        self.max_parallelism: int = 0 # initialized later

    def run_init(self, input: BatchOrchestratorInput):
        self.max_parallelism = input.max_parallelism
    

    #
    # Getting signals when new pages are queued for processing
    #
        
    def enqueue_page(self, BatchOrchestratorPage: BatchOrchestratorPage):
        workflow.logger.info(f"Enqueuing page request for cursor {BatchOrchestratorPage.cursor}")
        self.pending_pages.append(BatchOrchestratorPage)
     
    @workflow.signal
    async def signal_add_page(self, page: BatchOrchestratorPage) -> None:
        self.enqueue_page(page)

    #
    # Page management
    #   
         
    def work_is_complete(self) -> bool:
        return not self.pending_pages and not self.processing_pages
    
    def is_new_page_ready(self, num_launched_pages: int) -> bool:
        return len(self.processing_pages) < self.max_parallelism and len(self.pending_pages) > 0 and num_launched_pages < self.max_pages

    def on_page_processed(self, future: Future[str], pageNum: int, page: BatchOrchestratorPage):
        exception = future.exception()
        if exception:
            # TODO - real error handler
            raise exception
        workflow.logger.info(f"Batch executor completed {page} page at index {pageNum}")
        
        self.processing_pages.pop(pageNum)
        self.num_pages_processed += 1

    # Initiate processing the page and register a callback to record that it finished
    def process_page(self, *, input: BatchOrchestratorInput, pageNum: int, page: BatchOrchestratorPage):
        self.processing_pages[pageNum] = workflow.start_activity(
            process_page, 
            args=[input.page_processor, page], 
            start_to_close_timeout=timedelta(minutes=5))
        self.processing_pages[pageNum].add_done_callback(
            lambda future: self.on_page_processed(future, pageNum, page))

    #
    # Main algorithm
    #

    @workflow.run
    async def run(self, input: BatchOrchestratorInput) -> Results:
        workflow.logger.info("Starting batch executor")

        self.run_init(input)
        start_cursor: str = input.cursor
        
        page_size = 10
        num_launched_pages = 0
        max_parallelism_achieved = 0
        self.enqueue_page(BatchOrchestratorPage(start_cursor, page_size))

        while not self.work_is_complete():
            # Wake up (or continue) when an activity signals us with more work, when it completes, or when 
            # we're ready to process a new page.
            await workflow.wait_condition(
                lambda: self.is_new_page_ready(num_launched_pages) or self.work_is_complete())
            if self.is_new_page_ready(num_launched_pages):
                nextPage = self.pending_pages.pop()
                self.process_page(input=input, pageNum = num_launched_pages, page = nextPage)
                num_launched_pages += 1
            max_parallelism_achieved = max(max_parallelism_achieved, len(self.processing_pages))


        workflow.logger.info(f"Batch executor completed {self.num_pages_processed} pages")
        result = BatchOrchestrator.Results(num_pages_processed=self.num_pages_processed, max_parallelism_achieved=max_parallelism_achieved)
        return result


# convert batchPageProcessorName to a function and call it with the page
@activity.defn
async def process_page(batchPageProcessorName: str, page: BatchOrchestratorPage):
    context = await batch_page_processor_context.BatchPageProcessorContext(page=page, activity_info=activity.info()).async_init()
    userProvidedActivity = batch_page_processor_registry.get(batchPageProcessorName)
    if not userProvidedActivity:
        raise Exception(
            f"You passed batch processor name {batchPageProcessorName} into the BatchOrchestrator, but it was not registered on " +
            f"your activity worker.  Please annotate it with @page_processor and make sure its module is imported. " + 
            f"Available functions: {list_page_processors()}")
    return await userProvidedActivity(context)
    

