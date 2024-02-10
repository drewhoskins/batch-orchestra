from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import Any, Optional
from enum import Enum

from temporalio import activity
from temporalio.client import Client, WorkflowHandle
from temporalio.exceptions import ApplicationError

from batch_processor import BatchProcessorContextBase

from batch_orchestrator_data import BatchOrchestratorResults

_batch_tracker_registry = {}
def batch_tracker(on_stuck_pages_handler_function):
    _batch_tracker_registry[on_stuck_pages_handler_function.__name__] = on_stuck_pages_handler_function
    return on_stuck_pages_handler_function

def list_batch_trackers():
    return list(_batch_tracker_registry.keys())

# This exception is part of normal operation.  It's used to signal to the BatchOrchestrator that the user's @batch_tracker 
# should keep checking on progress according to Temporal's "polling pattern."
class BatchTrackerKeepPolling(Exception):
    pass

@activity.defn
async def track_batch_progress(batch_tracker_name: str, batch_id: Optional[str], args: Optional[str], current_status: BatchOrchestratorResults) -> None:
    user_provided_batch_tracker = _batch_tracker_registry.get(batch_tracker_name)
    if not user_provided_batch_tracker:
        raise Exception(
            f"You passed on_stuck_pages_handler_name {batch_tracker_name} into the BatchOrchestrator, but it was not registered on " +
            f"your worker.  Please annotate it with @batch_tracker and make sure its module is imported. " + 
            f"Available functions: {list_batch_trackers()}")
    context = await BatchTrackerContext(
        batch_id=batch_id, 
        args=args, 
        current_status=current_status,
        activity_info=activity.info()).async_init()
    await user_provided_batch_tracker(context)
    # Ensures that we'll periodically wake up to check the batch's progress
    raise BatchTrackerKeepPolling()

class BatchTrackerContext(BatchProcessorContextBase):
    def __init__(self, *, batch_id: Optional[str], current_status: BatchOrchestratorResults, args: Optional[str], activity_info: activity.Info):
        super().__init__(activity_info)
        self._batch_id = batch_id
        self._args = args
        self._current_status = current_status

    @property
    def current_status(self) -> BatchOrchestratorResults:
        return self._current_status

    @property
    def batch_id(self) -> str:
        assert self._batch_id is not None, "You're getting the batch ID but didn't pass one in to BatchOrchestratorInput.batch_id."
        return self._batch_id

    @property
    def args_str(self) -> str:
        result = self._args
        if result is None:
            raise ValueError("You cannot use get_args because you did not pass any args into BatchOrchestratorInput.page_processor_args")
        return result 

    @property
    def logger(self):
        return logging.getLogger(__name__)

