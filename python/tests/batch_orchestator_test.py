from __future__ import annotations
from asyncio import sleep
from dataclasses import asdict, dataclass
import json
import pytest
import uuid

from batch_orchestrator_page import BatchOrchestratorPage
from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput, process_page
from temporalio.client import Client
from temporalio.worker import Worker
from batch_page_processor_context import BatchPageProcessorContext
from batch_page_processor_registry import page_processor

@dataclass
class MyCursor:
    i: int

    def to_json(self):
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_json(json_str) -> MyCursor:
        return MyCursor(**json.loads(json_str))
    
@dataclass(kw_only=True)
class MyArgs:
    num_pages_to_process: int

    def to_json(self):
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_json(json_str) -> MyArgs:
        return MyArgs(**json.loads(json_str))

def default_cursor():
    return MyCursor(0).to_json()

@page_processor
async def processes_one_page(context: BatchPageProcessorContext):
    return context.get_page().cursor

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
            page_processor=processes_n_pages.__name__, 
            max_parallelism=3, 
            first_cursor=default_cursor(),
            page_processor_args=MyArgs(num_pages_to_process=1).to_json())
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
            page_processor=processes_n_pages.__name__, 
            max_parallelism=10, 
            first_cursor=default_cursor(),
            page_processor_args=MyArgs(num_pages_to_process=2).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 2
        assert result.max_parallelism_achieved >= 1

@page_processor
async def processes_n_pages(context: BatchPageProcessorContext):
    page = context.get_page()
    args = MyArgs.from_json(context.get_args())
    cursor = MyCursor.from_json(page.cursor)
    if cursor.i < (args.num_pages_to_process - 1) * page.page_size:
        await context.enqueue_next_page(
            BatchOrchestratorPage(MyCursor(cursor.i + page.page_size).to_json(), page.page_size)
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
            page_processor=processes_n_pages.__name__, 
            max_parallelism=max_parallelism, 
            first_cursor=default_cursor(),
            page_processor_args=MyArgs(num_pages_to_process=6).to_json())
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 6
        # While not a rock-hard guarantee, max_parallelism should be achieved in practice because I'm sleeping within the activity
        assert result.max_parallelism_achieved == max_parallelism
