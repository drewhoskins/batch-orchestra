from __future__ import annotations
import sys

try:
    from dataclasses import asdict, dataclass
    from asyncio import sleep
    import json
    import pytest
    import uuid    
    
    from temporalio.client import Client, WorkflowHandle
    from temporalio.worker import Worker
    from temporalio.common import RetryPolicy


    from batch_processor import BatchProcessorContext, page_processor
    from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput, process_page
    from batch_tracker import BatchTrackerContext, batch_tracker, track_batch_progress
except ModuleNotFoundError as e:
    print("This script requires poetry.  Try `poetry run pytest ./tests/batch_orchestrator_test.py`.")
    print(f"Original error: {e}")
    sys.exit(1)

def default_cursor():
    return MyCursor(0).to_json()

@dataclass
class MyCursor:
    i: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_json(json_str) -> MyCursor:
        return MyCursor(**json.loads(json_str))

@dataclass(kw_only=True)
class MyArgs:
    fail_until_i: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_json(json_str) -> MyArgs:
        return MyArgs(**json.loads(json_str))

def batch_worker(client: Client, task_queue_name: str):
    return Worker(
        client,
        task_queue=task_queue_name,
        workflows=[BatchOrchestrator],
        activities=[process_page, track_batch_progress],
        # So people can add breakpoints to the workflow without it timing out.
        debug_mode=True
    )

fails_first_n_tries_calls = 0

@page_processor
async def fails_first_n_tries(context: BatchProcessorContext):
    page = context.page
    args = MyArgs.from_json(context.args_str)
    await sleep(1)
    global fails_first_n_tries_calls
    if fails_first_n_tries_calls < args.fail_until_i:
        fails_first_n_tries_calls += 1
        raise ValueError(f"Intentionally failed, try {fails_first_n_tries_calls}")

@dataclass
class MyBatchTrackerArgs:
    some_value: str
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_json(json_str) -> MyBatchTrackerArgs:
        return MyBatchTrackerArgs(**json.loads(json_str))

my_tracker_progresses = []
@batch_tracker
async def my_tracker(context: BatchTrackerContext):
    args = MyBatchTrackerArgs.from_json(context.args_str)
    assert args.some_value == "thingy"
    global my_tracker_progresses
    my_tracker_progresses.append(context.progress)

@pytest.mark.asyncio
async def test_tracking_polls(client: Client):
    task_queue_name = str(uuid.uuid4())

    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            page_processor_name=fails_first_n_tries.__name__, 
            max_parallelism=3, 
            page_size=10,
            page_processor_args=MyArgs(fail_until_i=6).to_json(),
            initial_retry_policy=RetryPolicy(maximum_attempts=2),
            extended_retry_interval_seconds=2,
            first_cursor_str=default_cursor(),
            batch_tracker_name=my_tracker.__name__,
            batch_tracker_args=MyBatchTrackerArgs(some_value="thingy").to_json(),
            batch_tracker_polling_interval_seconds=1 # unreasonably short for testing
            )
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_completed_pages == 1
        assert result.num_failed_pages == 0

        # Ensure that the tracker polls, not just getting called once.
        global my_tracker_progresses
        print('--------------------------------------------------------')
        print(my_tracker_progresses)
        assert len(my_tracker_progresses) >= 2
        evidence_of_stuck_page = next((p for p in my_tracker_progresses if p.num_stuck_pages > 0), None)
        assert evidence_of_stuck_page is not None
