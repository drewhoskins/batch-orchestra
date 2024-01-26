from __future__ import annotations
from typing import Optional
from temporalio.client import Client

import python.batch_orchestrator

class BatchPageProcessorContext:
    def __init__(self, *, page, workflowInfo):
        self.page = page
        self.workflowInfo = workflowInfo
        self.workflowClient: Optional[Client] = None

    async def async_init(self)-> BatchPageProcessorContext:
        self.workflowClient = await Client.connect("localhost:7233")
        self.parentWorkflow = self.workflowClient.get_workflow_handle(
            self.workflowInfo.workflow_id, run_id = self.workflowInfo.workflow_run_id)

        return self

    def getPage(self):
        return self.page
    
    async def enqueueNextPage(self, page):
        assert self.parentWorkflow is not None, \
            ("BatchPageProcessorContext.async_init() was not called.  This class should only be " +
            "instantiated by the temporal-batch library.")
        await self.parentWorkflow.signal(
            python.batch_orchestrator.BatchOrchestrator.signalAddPage, 
            page
        )
    