from asyncio import Future
from dataclasses import dataclass
from datetime import timedelta
import importlib
from temporalio import workflow, activity
from temporalio.client import Client
from typing import Dict, Generic, List, Optional, TypeVar
import batch_page_processor_context 

from batch_orchestrator_page import BatchOrchestratorPage, MyCursor
from batch_page_processor_registry import list_page_processors, registry as batch_page_processor_registry


@dataclass 
class BatchOrchestratorInput:
    batch_name: str
    # The function, annotated with @page_processor, that will be called on your worker for each page
    page_processor: str 
    cursor: Optional[MyCursor] = None

# Paginates through a large dataset and executes it with controllable parallelism.  
@workflow.defn
class BatchOrchestrator:

    @dataclass
    class Results:
        numPagesProcessed: int

    def __init__(self):
        self.signalAddedPages = []
        self.numPagesProcessed = 0
        self.processingPages: Dict[int, Future[MyCursor]] = {}
        self.pendingPages: List[BatchOrchestratorPage] = []
        self.max_pages: int = 1000 # TODO measure a good limit
        self.maxParallelism: int = 3

    #
    # Getting signals when new pages are queued for processing
    #
        
    def enqueue_page(self, BatchOrchestratorPage: BatchOrchestratorPage):
        workflow.logger.info(f"Enqueuing page request for cursor {BatchOrchestratorPage.cursor}")
        self.pendingPages.append(BatchOrchestratorPage)

    def was_signaled_with_page(self)-> bool:
        return bool(self.signalAddedPages)
    
    def pop_signaled_page(self):
        return self.signalAddedPages.pop()
        
    @workflow.signal
    async def signal_add_page(self, page: BatchOrchestratorPage) -> None:
        self.signalAddedPages.append(page)

    #
    # Page management
    #   
         
    def work_is_complete(self) -> bool:
        return not self.pendingPages and not self.processingPages and not self.was_signaled_with_page()
    
    def is_new_page_ready(self, num_launched_pages: int) -> bool:
        return len(self.processingPages) < self.maxParallelism and len(self.pendingPages) > 0 and num_launched_pages < self.max_pages

    def on_page_processed(self, future: Future[MyCursor], pageNum: int, page: BatchOrchestratorPage):
        exception = future.exception()
        if exception:
            # TODO - real error handler
            raise exception
        workflow.logger.info(f"Batch executor completed {page} page at index {pageNum}")
        
        self.processingPages.pop(pageNum)
        self.numPagesProcessed += 1

    # Initiate processing the page and register a callback to record that it finished
    def process_page(self, *, input: BatchOrchestratorInput, pageNum: int, page: BatchOrchestratorPage):
        self.processingPages[pageNum] = workflow.start_activity(
            process_page, 
            args=[input.page_processor, page], 
            start_to_close_timeout=timedelta(minutes=5))
        self.processingPages[pageNum].add_done_callback(
            lambda future: self.on_page_processed(future, pageNum, page))

    #
    # Main algorithm
    #

    @workflow.run
    async def run(self, input: BatchOrchestratorInput) -> Results:
        workflow.logger.info("Starting batch executor")

        start_cursor: MyCursor = input.cursor or MyCursor(0)
        
        page_size = 10
        num_launched_pages = 0
        self.enqueue_page(BatchOrchestratorPage(start_cursor, page_size))

        while not self.work_is_complete():
            # Wake up (or continue) when an activity signals us with more work, when it completes, or when 
            # we're ready to process a new page.
            await workflow.wait_condition(
                lambda: self.was_signaled_with_page() or 
                    self.is_new_page_ready(num_launched_pages) or
                    self.work_is_complete())
            if self.was_signaled_with_page():
                self.enqueue_page(self.pop_signaled_page())
            elif self.is_new_page_ready(num_launched_pages):
                nextPage = self.pendingPages.pop()
                self.process_page(input=input, pageNum = num_launched_pages, page = nextPage)
                num_launched_pages += 1

        workflow.logger.info(f"Batch executor completed {self.numPagesProcessed} pages")
        result = BatchOrchestrator.Results(numPagesProcessed=self.numPagesProcessed)
        return result


# convert batchPageProcessorName to a function and call it with the page
@activity.defn
async def process_page(batchPageProcessorName: str, page: BatchOrchestratorPage):
    context = await batch_page_processor_context.BatchPageProcessorContext(page=page, workflowInfo=activity.info()).async_init()
    userProvidedActivity = batch_page_processor_registry.get(batchPageProcessorName)
    if not userProvidedActivity:
        raise Exception(
            f"You passed batch processor name {batchPageProcessorName} into the BatchOrchestrator, but it was not registered on " +
            f"your activity worker.  Please annotate it with @page_processor and make sure its module is imported. " + 
            "Available functions: {list_page_processors()}")
    return await userProvidedActivity(context)
    

