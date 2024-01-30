from __future__ import annotations
from asyncio import sleep
from dataclasses import asdict, dataclass
import json
import pytest
import uuid

from batch_processor import BatchPage
from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput, process_page
from temporalio.client import Client
from temporalio.worker import Worker
from batch_processor import BatchProcessorContext, page_processor

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
    num_items_to_process: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_json(json_str) -> MyArgs:
        return MyArgs(**json.loads(json_str))

def default_cursor():
    return MyCursor(0).to_json()

@page_processor
async def processes_one_page(context: BatchProcessorContext):
    return context.get_page().cursor_str

def batch_worker(client: Client, task_queue_name: str):
    return Worker(
        client,
        task_queue=task_queue_name,
        workflows=[BatchOrchestrator],
        activities=[process_page],
    )

@pytest.mark.asyncio
async def test_one_page(client: Client):
    task_queue_name = str(uuid.uuid4())

    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_name='my_batch', 
            page_processor=processes_n_items.__name__, 
            max_parallelism=3, 
            page_size=10,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=1).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 1
        assert result.max_parallelism_achieved == 1

# Testing with spawns_second_page will ensure that the workflow is signaled and that it processes the second page
@pytest.mark.asyncio
async def test_two_pages(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_name='my_batch', 
            page_processor=processes_n_items.__name__, 
            max_parallelism=10, 
            page_size=10,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=19).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 2 # 10 first page, 9 second
        assert result.max_parallelism_achieved >= 1

@page_processor
async def processes_n_items(context: BatchProcessorContext):
    page = context.get_page()
    args = MyArgs.from_json(context.get_args())
    cursor = MyCursor.from_json(page.cursor_str)
    # Simulate whether we get an incomplete page.  If not, start a new page.
    if cursor.i + page.page_size < args.num_items_to_process - 1:
        await context.enqueue_next_page(
            BatchPage(MyCursor(cursor.i + page.page_size).to_json(), page.page_size)
        )
        print(f"Signaled the workflow {page}")
    print(f"Processing page {page}")
    # pretend to do some processing
    await sleep(0.1)
    return cursor.i


@pytest.mark.asyncio
async def test_max_parallelism(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        max_parallelism = 3
        input = BatchOrchestratorInput(
            batch_name='my_batch', 
            page_processor=processes_n_items.__name__, 
            max_parallelism=max_parallelism, 
            page_size=10,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=59).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 6 # 5 pages * 10 + 1 page * 9
        # While not a rock-hard guarantee, max_parallelism should be achieved in practice because I'm sleeping within the activity
        assert result.max_parallelism_achieved == max_parallelism

@pytest.mark.asyncio
async def test_page_size(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_name='my_batch', 
            page_processor=processes_n_items.__name__, 
            max_parallelism=3, 
            page_size=5,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=49).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 10 # 9 pages * 5 items + 1 page with 4

@page_processor
async def asserts_timeout(context: BatchProcessorContext):
    timeout = context.get_activity_info().start_to_close_timeout
    assert timeout is not None
    assert timeout.seconds == 123

@pytest.mark.asyncio
async def test_timeout(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput(
            batch_name='my_batch', 
            page_processor=asserts_timeout.__name__, 
            max_parallelism=3, 
            page_size=10,
            page_timeout_seconds=123,
            first_cursor_str=default_cursor(),
            page_processor_args=MyArgs(num_items_to_process=49).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 1
