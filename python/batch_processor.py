from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import temporalio.activity
from temporalio.client import Client

# 
# batch_processor library
# 
# This file contains the interfaces used by the processor you write to process a page of work.  
# It works in conjunction with the batch_orchestrator library.
#
# Your page processor function:
#   * Will run on a Temporal worker as part of an activity.
#   * Must be registered with the @page_processor decorator to allow the batch orchestrator to find it safely.
#   * Must be async.
#   * Take a BatchProcessorContext as its only argument.
#   * It has two goals
#       1. To get the next page of work and enqueue it on the BatchOrchestator
#       2. To process a page of work 
#   * BatchPage represents a page.
#   * It will use the BatchProcessorContext class to access the cursor and any args passed to it.
#   * It must first call BatchProcessorContext.enqueue_next_page() to enqueue the next page of work before it processes the contents of the page.
#     this will allow pages to be executed in parallel.


# The Registry
# You must declare your page processor functions with the @page_processor to allow the batch orchestrator to find them safely.
page_processor_registry = {}

def page_processor(page_processor_function):
    page_processor_registry[page_processor_function.__name__] = page_processor_function
    return page_processor_function

def list_page_processors():
    return list(page_processor_registry.keys())

# In your batch jobs, you'll chunk them into pages of work that run in parallel with one another. 
# Each page, represented by this class, processes in series.
# Choose a page size that can run in under five minutes.
@dataclass
class BatchPage:
    # Your cursor serialized as a string.  You might use json for example.
    # When sdk-python supports generics, we can add support for (serializable) cursor types here.
    cursor_str: str
    page_size: int
    
# This class is the only argument passed to your page processor function but contains everything you need.
class BatchProcessorContext:
    def __init__(self, *, page: BatchPage, args: Optional[str], activity_info: temporalio.activity.Info):
        self._page = page
        self._activity_info = activity_info
        self._args = args
        self._workflow_client: Optional[Client] = None
        self._parent_workflow: Optional[temporalio.WorkflowHandle] = None

    async def async_init(self)-> BatchProcessorContext:
        self._workflow_client = await Client.connect("localhost:7233")
        self._parent_workflow = self._workflow_client.get_workflow_handle(
            self._activity_info.workflow_id, run_id = self._activity_info.workflow_run_id)

        return self

    def get_page(self) -> BatchPage:
        return self._page

    # Gets global, user-provided args passed in BatchOrchestratorInput.page_processor_args.  
    # Any values that can differ per page should insted go into your cursor inside BatchPage.
    # Suggested usage: use JSON and deserialize the args into a dataclass.
    def get_args(self) -> str:
        result = self._args
        if result is None:
            raise ValueError("You cannot use get_args because you did not pass any args into BatchOrchestratorInput.page_processor_args")
        return result 
    
    # Call this with your next cursor before you process the page to enqueue the next chunk on the BatchOrchestator.
    async def enqueue_next_page(self, page) -> None:
        assert self._parent_workflow is not None, \
            ("BatchProcessorContext.async_init() was not called.  This class should only be " +
            "instantiated by the temporal-batch library.")
        await self._parent_workflow.signal(
            'signal_add_page', # use string instead of literal to avoid upward dependency between this file and batch_orchestrator.py
            page
        )
    
    # Advanced: use this for low-level access to details about the temporal activity in which your page processor runs, such as 
    # for adding heartbeats to your activity.
    def get_activity_info(self) -> temporal.activity.Info:
        return self._activity_info

