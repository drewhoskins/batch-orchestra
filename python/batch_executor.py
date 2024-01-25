from asyncio import Future
from dataclasses import dataclass
from datetime import timedelta
from temporalio import workflow, activity
from temporalio.client import Client
from typing import Dict, Generic, List, Optional, TypeVar

from batch_executor_page import BatchExecutorPage, MyCursor



@dataclass 
class BatchExecutorInput:
    batch_name: str
    cursor: Optional[MyCursor] = None

# Paginates through a large dataset and executes it with controllable parallelism.  
@workflow.defn
class BatchExecutor:

    def __init__(self):
        self.signalAddedPages = []

    def enqueuePage(self, pendingPages: List[BatchExecutorPage], BatchExecutorPage: BatchExecutorPage):
        workflow.logger.info(f"Enqueuing page request for cursor {BatchExecutorPage.cursor}")
        pendingPages.append(BatchExecutorPage)
        pass

    def workIsComplete(self, inFlightPages, pendingPages, maxPages, numLaunchedPages) -> bool:
        # Add your implementation here
        return not pendingPages and not inFlightPages and not self.wasSignaledWithPage()
    
    def wasSignaledWithPage(self)-> bool:
        return bool(self.signalAddedPages)
    
    def popSignaledPage(self):
        return self.signalAddedPages.pop()
        
    
    @workflow.signal
    async def signalAddPage(self, page: BatchExecutorPage) -> None:
        self.signalAddedPages.append(page)

    def isNewPageReady(
            self, 
            inFlightPages: Dict[int, Future[MyCursor]], 
            pendingPages: List[BatchExecutorPage], 
            maxPages: int, numLaunchedPages: int, 
            maxParallelism: int) -> bool:
        return len(inFlightPages) < maxParallelism and len(pendingPages) > 0 and numLaunchedPages < maxPages
    
    @workflow.run
    async def run(self, input: BatchExecutorInput):
        workflow.logger.info("Starting batch executor")

        startCursor: MyCursor = input.cursor or MyCursor(0)
        
        pageSize = 10
        maxPages = 15
        maxParallelism = 3
        numLaunchedPages = 0
        inFlightPages : Dict[int, Future[MyCursor]] = {}
        pendingPages = []
        self.enqueuePage(pendingPages, BatchExecutorPage(startCursor, pageSize))
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
                await workflow.execute_activity(my_process_batch, args=[nextPage], start_to_close_timeout=timedelta(minutes=5))
        workflow.logger.info(f"Batch executor completed {numLaunchedPages} pages")
        return None


@activity.defn
async def my_process_batch(pageRequest: BatchExecutorPage):
    if pageRequest.cursor.i == 0:
        workflowClient = await Client.connect("localhost:7233")
        workflow_info = activity.info()
        parentWorkflow = workflowClient.get_workflow_handle(workflow_info.workflow_id, run_id = workflow_info.workflow_run_id)
        # signal the parent workflow to start the next page
        await parentWorkflow.signal(
            BatchExecutor.signalAddPage, 
            BatchExecutorPage(MyCursor(pageRequest.cursor.i + pageRequest.pageSize), pageRequest.pageSize))
        print(f"Processing page {pageRequest}")
    print(f"Signaled the workflow {pageRequest}")
    # Sleep for a bit to simulate work