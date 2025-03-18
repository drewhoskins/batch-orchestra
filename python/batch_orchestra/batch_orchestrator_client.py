from datetime import timedelta
from typing import Any, Mapping, Optional

import temporalio.client
from batch_orchestrator import BatchOrchestrator
from batch_orchestrator_io import BatchOrchestratorInput, BatchOrchestratorProgress


# Use this to track and manipulate your batch job as it runs.  It's a thin wrapper around the workflow handle.
class BatchOrchestratorHandle:
    def __init__(self, handle: temporalio.client.WorkflowHandle[Any, BatchOrchestratorProgress]):
        self._handle = handle

    # See how far the batch has progressed from your client.
    # You can instead track progress without the need for a client by implementing a @batch_tracker.
    async def get_progress(self) -> BatchOrchestratorProgress:
        return await self._handle.query(BatchOrchestrator.current_progress)

    # The final result.  This will block until the batch is finished.
    async def result(
        self,
        # See temporalio.client.WorkflowHandle.result for docs on these parameters
        follow_runs: bool = True,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> BatchOrchestratorProgress:
        return await self._handle.result(follow_runs=follow_runs, rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout)

    # Finishes the currently executing page processors and then pauses before any more are launched.
    # You can resume with resume() or set_max_parallelism().
    async def pause(self) -> None:
        await self._handle.signal(BatchOrchestrator.set_max_parallelism, 0)

    # Starts processing more pages after having called pause(), setting it to your original max_parallelism
    async def resume(self) -> None:
        await self._handle.signal(BatchOrchestrator.restore_max_parallelism)

    # Use this to pause the batch (by setting it to 0) or otherwise increase/decrease the number of
    # @page_processors that can execute at once.
    # Also "pushes" the old parallelism onto a stack so that you can restore_max_parallelism it later.
    async def set_max_parallelism(self, max_parallelism: int) -> None:
        await self._handle.signal(BatchOrchestrator.set_max_parallelism, max_parallelism)

    # "pops" to the previous max_parallelism value, if there was one.
    async def restore_max_parallelism(self) -> None:
        await self._handle.signal(BatchOrchestrator.restore_max_parallelism)

    # Use the workflow handle to access normal Temporal operations such as cancel and terminate.
    @property
    def workflow_handle(self) -> temporalio.client.WorkflowHandle[Any, BatchOrchestratorProgress]:
        return self._handle


class BatchOrchestratorClient:
    def __init__(self, temporal_client: temporalio.client.Client):
        self._temporal_client = temporal_client

    async def start(
        self,
        input: BatchOrchestratorInput,
        *,
        # The temporal workflow ID you can use to reference the running and completed orchestrator.
        id: str,
        # See https://docs.temporal.io/workers#task-queue
        task_queue: str,
        # See temporalio.client.Client.start_workflow for advanced options.
        **kwargs,
    ) -> BatchOrchestratorHandle:
        kwargs.update({"task_queue": task_queue, "id": id})
        handle = await self._temporal_client.start_workflow(BatchOrchestrator.run, args=[input, None], **kwargs)
        return BatchOrchestratorHandle(handle)

    # Use to get access to a running or completed batch job with just the temporal workflow ID.
    def get_handle(
        self,
        workflow_id: str,
        *,
        run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
    ) -> BatchOrchestratorHandle:
        return BatchOrchestratorHandle(
            self._temporal_client.get_workflow_handle(
                workflow_id,
                run_id=run_id,
                first_execution_run_id=first_execution_run_id,
                result_type=BatchOrchestratorProgress,
            )
        )
