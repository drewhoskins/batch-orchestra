from __future__ import annotations
import sys

from batch_orchestrator_client import BatchOrchestratorClient

try:
    from dataclasses import asdict, dataclass
    from asyncio import sleep
    import json
    import pytest
    import uuid

    from temporalio.client import Client, WorkflowHandle
    from temporalio.worker import Worker
    from temporalio.common import RetryPolicy

    from batch_processor import BatchProcessorContext, page_processor, PageProcessor
    from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput, process_page
    from batch_tracker import BatchTrackerContext, batch_tracker, track_batch_progress
except ModuleNotFoundError as e:
    print("This script requires poetry.  Try `poetry run pytest ./tests/batch_orchestrator_test.py`.")
    print(
        "But if you haven't, first see Python Quick Start in python/README.md for instructions on installing and setting up poetry."
    )
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
        debug_mode=True,
    )


fails_first_n_tries_calls = 0


@page_processor
class FailsFirstNTries(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        args = MyArgs.from_json(context.args_str)
        await sleep(1)
        global fails_first_n_tries_calls
        if fails_first_n_tries_calls < args.fail_until_i:
            fails_first_n_tries_calls += 1
            raise ValueError(f"Intentionally failed, try {fails_first_n_tries_calls}")

    @property
    def retry_mode(self):
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE

    @property
    def initial_retry_policy(self):
        return RetryPolicy(maximum_attempts=2)

    @property
    def extended_retry_interval_seconds(self) -> int:
        return 1  # For a fast test


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


async def start_orchestrator(client: Client, task_queue_name: str, input: BatchOrchestratorInput) -> WorkflowHandle:
    return await BatchOrchestratorClient(client).start(input, id=str(uuid.uuid4()), task_queue=task_queue_name)


@pytest.mark.asyncio
async def test_tracking_polls(client: Client):
    task_queue_name = str(uuid.uuid4())
    global my_tracker_progresses
    my_tracker_progresses = []

    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            max_parallelism=3,
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=FailsFirstNTries.__name__,
                args=MyArgs(fail_until_i=5).to_json(),
                first_cursor_str=default_cursor(),
                page_size=10,
            ),
            batch_tracker=BatchOrchestratorInput.BatchTrackerContext(
                name=my_tracker.__name__,
                args=MyBatchTrackerArgs(some_value="thingy").to_json(),
                polling_interval_seconds=1,  # unreasonably short for testing
            ),
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.num_completed_pages == 1
        assert result.num_failed_pages == 0

        # Ensure that the tracker polls, not just getting called once at the beginning and end
        assert len(my_tracker_progresses) >= 3
        evidence_of_stuck_page = next((p for p in my_tracker_progresses if p.num_stuck_pages > 0), None)
        assert evidence_of_stuck_page is not None
        assert my_tracker_progresses[-1].is_finished


@pytest.mark.asyncio
async def test_tracker_can_be_canceled(client: Client):
    task_queue_name = str(uuid.uuid4())
    global my_tracker_progresses
    my_tracker_progresses = []

    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            max_parallelism=3,
            page_processor=BatchOrchestratorInput.PageProcessorContext(
                name=FailsFirstNTries.__name__,
                page_size=10,
                args=MyArgs(fail_until_i=0).to_json(),
                first_cursor_str=default_cursor(),
            ),
            batch_tracker=BatchOrchestratorInput.BatchTrackerContext(
                name=my_tracker.__name__,
                args=MyBatchTrackerArgs(some_value="thingy").to_json(),
                polling_interval_seconds=60,  # longer than the test should take, cancel should interrupt
            ),
        )
        handle = await start_orchestrator(client, task_queue_name, input)
        result = await handle.result()
        assert result.is_finished
        assert result.num_completed_pages == 1
        assert result.num_failed_pages == 0

        # Ensure that the tracker gets called on startup and teardown, but we should
        # cancel before it retries after polling.
        assert len(my_tracker_progresses) == 2
        assert not my_tracker_progresses[0].is_finished
        assert my_tracker_progresses[1].is_finished
