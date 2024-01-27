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

    def enqueuePage(self, pendingPages: List[BatchOrchestratorPage], BatchOrchestratorPage: BatchOrchestratorPage):
        workflow.logger.info(f"Enqueuing page request for cursor {BatchOrchestratorPage.cursor}")
        pendingPages.append(BatchOrchestratorPage)
        pass

    def workIsComplete(self, inFlightPages, pendingPages, maxPages, numLaunchedPages) -> bool:
        # Add your implementation here
        return not pendingPages and not inFlightPages and not self.wasSignaledWithPage()
    
    def wasSignaledWithPage(self)-> bool:
        return bool(self.signalAddedPages)
    
    def popSignaledPage(self):
        return self.signalAddedPages.pop()
        
    
    @workflow.signal
    async def signalAddPage(self, page: BatchOrchestratorPage) -> None:
        self.signalAddedPages.append(page)

    def isNewPageReady(
            self, 
            inFlightPages: Dict[int, Future[MyCursor]], 
            pendingPages: List[BatchOrchestratorPage], 
            maxPages: int, numLaunchedPages: int, 
            maxParallelism: int) -> bool:
        return len(inFlightPages) < maxParallelism and len(pendingPages) > 0 and numLaunchedPages < maxPages
    
    @workflow.run
    async def run(self, input: BatchOrchestratorInput) -> Results:
        workflow.logger.info("Starting batch executor")

        startCursor: MyCursor = input.cursor or MyCursor(0)
        
        pageSize = 10
        maxPages = 15
        maxParallelism = 3
        numLaunchedPages = 0
        inFlightPages : Dict[int, Future[MyCursor]] = {}
        pendingPages = []
        numFinishedPages = 0
        self.enqueuePage(pendingPages, BatchOrchestratorPage(startCursor, pageSize))
        while not self.workIsComplete(inFlightPages, pendingPages, maxPages, numLaunchedPages):
            # Wake up (or continue) when an activity signals us with more work, when it completes, or when 
            # we're ready to process a new page.
            await workflow.wait_condition(
                lambda: self.wasSignaledWithPage() or 
                    self.isNewPageReady(inFlightPages, pendingPages, maxPages, numLaunchedPages, maxParallelism) or
                    self.workIsComplete(inFlightPages, pendingPages, maxPages, numLaunchedPages))
            if self.wasSignaledWithPage():
                self.enqueuePage(pendingPages, self.popSignaledPage())
            elif self.isNewPageReady(inFlightPages, pendingPages, maxPages, numLaunchedPages, maxParallelism):
                numLaunchedPages += 1
                nextPage = pendingPages.pop()
                await workflow.execute_activity(process_page, args=[input.page_processor, nextPage], start_to_close_timeout=timedelta(minutes=5))
        workflow.logger.info(f"Batch executor completed {numLaunchedPages} pages")
        # TODO - keep track of how many pages were processed and return that
        result = BatchOrchestrator.Results(numPagesProcessed=numLaunchedPages)
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
    

