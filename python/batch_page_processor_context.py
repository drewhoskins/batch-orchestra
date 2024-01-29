from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import temporalio.activity
from temporalio.client import Client

# In your batch jobs, you'll chunk them into pages of work that run in parallel with one another. 
# Each page, represented by this class, processes in series.
# Choose a page size that can run in under five minutes.
@dataclass
class BatchPage:
    # Your cursor serialized as a string.  You might use json for example.
    # When sdk-python supports generics, you'll be able to use a serializable cursor type directly here.
    cursor_str: str
    page_size: int
    
class BatchProcessorContext:
    def __init__(self, *, page: BatchPage, args: Optional[str], activity_info: temporalio.activity.Info):
        self.page = page
        self.activity_info = activity_info
        self.args = args
        self.workflow_client: Optional[Client] = None

    async def async_init(self)-> BatchProcessorContext:
        self.workflow_client = await Client.connect("localhost:7233")
        self.parent_workflow = self.workflow_client.get_workflow_handle(
            self.activity_info.workflow_id, run_id = self.activity_info.workflow_run_id)

        return self

    def get_page(self) -> BatchPage:
        return self.page

    # Gets user-provided args passed in BatchOrchestratorInput.page_processor_args
    def get_args(self) -> str:
        result = self.args
        if result is None:
            raise ValueError("You cannot use get_args because you did not pass any args into BatchOrchestratorInput.page_processor_args")
        return result 
    
    async def enqueue_next_page(self, page) -> None:
        assert self.parent_workflow is not None, \
            ("BatchProcessorContext.async_init() was not called.  This class should only be " +
            "instantiated by the temporal-batch library.")
        await self.parent_workflow.signal(
            'signal_add_page', # use string instead of literal to avoid upward dependency between this file and batch_orchestrator.py
            page
        )

