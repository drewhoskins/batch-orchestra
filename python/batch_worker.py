from __future__ import annotations
from abc import ABC
from typing import Awaitable, Callable, Optional, Sequence

import temporalio.client
from temporalio.client import Client, WorkflowHandle
from temporalio import activity

from batch_orchestrator_io import batch_orchestrator_data_converter

from temporalio.converter import (
    DefaultPayloadConverter,
)

"""
BatchWorker class with configuration for the batch worker -- the processor and the tracker
"""
class BatchWorkerClient:
    _instance = None

    """
    You must run this method on your worker.
    If you get an error message like 'Unknown payload encoding text/batch-orchestrator-encoding', then you need to have run this function
    and pass the resulting client into the Worker constructor (on your worker) or the start_workflow call (on your client).
    """
    @staticmethod
    def augment(client: temporalio.client.Client) -> temporalio.client.Client:
        return BatchWorkerClient.get_instance().set_temporal_client(client).get_temporal_client()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._client: Optional[temporalio.client.Client] = None

    def set_temporal_client(self, client: temporalio.client.Client) -> BatchWorkerClient:
        new_config = client.config()
        new_config["data_converter"] = batch_orchestrator_data_converter
        self._client = temporalio.client.Client(**new_config)
        return self

    # For tests
    def _clear_temporal_client(self) -> BatchWorkerClient:
        self._client = None
        return self

    def get_temporal_client(self) -> Optional[temporalio.client.Client]:
        return self._client

    @staticmethod
    def get_instance():
        if BatchWorkerClient._instance is None:
            BatchWorkerClient._instance = BatchWorkerClient()
        return BatchWorkerClient._instance

"""
Used by your @page_processor and @batch_tracker.
"""
class BatchWorkerContext(ABC):
    def __init__(self, activity_info: activity.Info):
        self._activity_info = activity_info
        self._parent_workflow: Optional[WorkflowHandle] = None
        workflow_client = BatchWorkerClient.get_instance().get_temporal_client()
        if workflow_client is None:
            raise ValueError(
                f"Missing a temporal client for use by your @page_processor or @batch_tracker. " + \
                "Make sure to call BatchWorkerClient.augment(client) and pass the resulting client into your Worker.")
        self._workflow_client: Client = workflow_client

    async def async_init(self)-> BatchWorkerContext:
        self._parent_workflow = self._workflow_client.get_workflow_handle(
            self._activity_info.workflow_id, run_id = self._activity_info.workflow_run_id)
        return self
