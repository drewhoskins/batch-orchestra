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
        self.maxPages: int = 1000 # TODO measure a good limit
        self.maxParallelism: int = 3

    #
    # Getting signals when new pages are queued for processing
    #
        
    def enqueuePage(self, BatchOrchestratorPage: BatchOrchestratorPage):
        workflow.logger.info(f"Enqueuing page request for cursor {BatchOrchestratorPage.cursor}")
        self.pendingPages.append(BatchOrchestratorPage)

    def wasSignaledWithPage(self)-> bool:
        return bool(self.signalAddedPages)
    
    def popSignaledPage(self):
        return self.signalAddedPages.pop()
        
    @workflow.signal
    async def signalAddPage(self, page: BatchOrchestratorPage) -> None:
        self.signalAddedPages.append(page)

    #
    # Page management
    #   
         
    def workIsComplete(self) -> bool:
        return not self.pendingPages and not self.processingPages and not self.wasSignaledWithPage()
    
    def isNewPageReady(self, numLaunchedPages: int) -> bool:
        return len(self.processingPages) < self.maxParallelism and len(self.pendingPages) > 0 and numLaunchedPages < self.maxPages

    def onPageProcessed(self, future: Future[MyCursor], pageNum: int, page: BatchOrchestratorPage):
        exception = future.exception()
        if exception:
            # TODO - real error handler
            raise exception
        workflow.logger.info(f"Batch executor completed {page} page at index {pageNum}")
        
        self.processingPages.pop(pageNum)
        self.numPagesProcessed += 1

    # Initiate processing the page and register a callback to record that it finished
    def processPage(self, *, input: BatchOrchestratorInput, pageNum: int, page: BatchOrchestratorPage):
        self.processingPages[pageNum] = workflow.start_activity(
            process_page, 
            args=[input.page_processor, page], 
            start_to_close_timeout=timedelta(minutes=5))
        self.processingPages[pageNum].add_done_callback(
            lambda future: self.onPageProcessed(future, pageNum, page))

    #
    # Main algorithm
    #

    @workflow.run
    async def run(self, input: BatchOrchestratorInput) -> Results:
        workflow.logger.info("Starting batch executor")

        startCursor: MyCursor = input.cursor or MyCursor(0)
        
        pageSize = 10
        numLaunchedPages = 0
        self.enqueuePage(BatchOrchestratorPage(startCursor, pageSize))
        
        while not self.workIsComplete():
            # Wake up (or continue) when an activity signals us with more work, when it completes, or when 
            # we're ready to process a new page.
            await workflow.wait_condition(
                lambda: self.wasSignaledWithPage() or 
                    self.isNewPageReady(numLaunchedPages) or
                    self.workIsComplete())
            if self.wasSignaledWithPage():
                self.enqueuePage(self.popSignaledPage())
            elif self.isNewPageReady(numLaunchedPages):
                nextPage = self.pendingPages.pop()
                self.processPage(input=input, pageNum = numLaunchedPages, page = nextPage)
                numLaunchedPages += 1

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
    

