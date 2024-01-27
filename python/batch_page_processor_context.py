from __future__ import annotations
from typing import Optional
from temporalio.client import Client

import batch_orchestrator

class BatchPageProcessorContext:
    def __init__(self, *, page, workflow_info):
        self.page = page
        self.workflow_info = workflow_info
        self.workflow_client: Optional[Client] = None

    async def async_init(self)-> BatchPageProcessorContext:
        self.workflow_client = await Client.connect("localhost:7233")
        self.parent_workflow = self.workflow_client.get_workflow_handle(
            self.workflow_info.workflow_id, run_id = self.workflow_info.workflow_run_id)

        return self

    def get_page(self):
        return self.page
    
    async def enqueue_next_page(self, page):
        assert self.parent_workflow is not None, \
            ("BatchPageProcessorContext.async_init() was not called.  This class should only be " +
            "instantiated by the temporal-batch library.")
        await self.parent_workflow.signal(
            batch_orchestrator.BatchOrchestrator.signal_add_page, 
            page
        )
    