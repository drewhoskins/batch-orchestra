from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional
from enum import Enum

from temporalio import activity
from temporalio.client import Client, WorkflowHandle
from temporalio.exceptions import ApplicationError

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
    # The number of records to process.
    size: int


# convert batchPageProcessorName to a function and call it with the page
# Returns whatever the page processor returns, which should be serialized or serializable (perhaps using a temporal data converter)
@activity.defn
async def process_page(batch_page_processor_name: str, page: BatchPage, page_num: int, args: Optional[str], did_signal_next_page: bool) -> Any:
    context = await BatchProcessorContext(
        page=page,
        page_num=page_num,
        args=args,
        activity_info=activity.info(),
        did_signal_next_page=did_signal_next_page).async_init()
    userProvidedActivity = page_processor_registry.get(batch_page_processor_name)
    if not userProvidedActivity:
        raise Exception(
            f"You passed batch processor name {batch_page_processor_name} into the BatchOrchestrator, but it was not registered on " +
            f"your activity worker.  Please annotate it with @page_processor and make sure its module is imported. " + 
            f"Available functions: {list_page_processors()}")
    return await userProvidedActivity(context)
    
    
# This class is the only argument passed to your page processor function but contains everything you need.
class BatchProcessorContext:

    class NextPageSignaled(Enum):
        NOT_SIGNALED = 0
        INITIAL_PHASE = 1
        THIS_RUN = 2
        PREVIOUS_RUN = 3

    def __init__(self, *, page: BatchPage, page_num: int, args: Optional[str], activity_info: activity.Info, did_signal_next_page: bool):
        self._page = page
        self._page_num = page_num
        self._activity_info = activity_info
        self._args = args
        self._workflow_client: Optional[Client] = None
        self._parent_workflow: Optional[WorkflowHandle] = None
        if did_signal_next_page:
            self._next_page_signaled = BatchProcessorContext.NextPageSignaled.INITIAL_PHASE
        elif "signaled_next_page" in activity.info().heartbeat_details:
            self._next_page_signaled = BatchProcessorContext.NextPageSignaled.PREVIOUS_RUN
        else:
            self._next_page_signaled = BatchProcessorContext.NextPageSignaled.NOT_SIGNALED 

    # Prints to the Worker's logs.  If you are developing locally and want to see the logs, run the Worker in the foreground and with debug_mode=True.
    def logger(self):
        return activity.logger

    async def async_init(self)-> BatchProcessorContext:
        # TODO add data converter just in case
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

    def _did_signal_next_page(self) -> bool:
        return self._next_page_signaled != BatchProcessorContext.NextPageSignaled.NOT_SIGNALED
    
    # Call this with your next cursor before you process the page to enqueue the next chunk on the BatchOrchestator.
    async def enqueue_next_page(self, page: BatchPage) -> None:
        assert self._parent_workflow is not None, \
            ("BatchProcessorContext.async_init() was never called.  This class should only be " +
            "instantiated by the temporal-batch library.")
        assert self._next_page_signaled != BatchProcessorContext.NextPageSignaled.THIS_RUN, \
            ("You cannot call enqueue_next_page twice in the same page_processor.  Each processed page " +
             "is responsible for enqueuing the following page.")

        # Minimize the chance of a re-signal when the activity fails and retries, by checking if we recorded that we already signaled.
        if self._did_signal_next_page():
            return
        
        print(f"Signaling page '{page.cursor_str}'")
        await self._parent_workflow.signal(
            'signal_add_page', # use string instead of literal to avoid upward dependency between this file and batch_orchestrator.py
            args=[page, self._page_num + 1]
        )

        self._next_page_signaled = BatchProcessorContext.NextPageSignaled.THIS_RUN
        activity.heartbeat("signaled_next_page")
    