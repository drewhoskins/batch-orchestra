import pytest
import uuid

from batch_orchestrator_page import BatchOrchestratorPage, MyCursor
from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput, process_page
from temporalio.client import Client
from temporalio.worker import Worker
from batch_page_processor_context import BatchPageProcessorContext
from batch_page_processor_registry import page_processor

@page_processor
async def returns_cursor(context: BatchPageProcessorContext):
    return context.get_page().cursor.i

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
        input = BatchOrchestratorInput('my_batch', 'returns_cursor', MyCursor(0))
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 1

@page_processor
async def spawns_second_page(context: BatchPageProcessorContext):
    page = context.get_page()
    if page.cursor.i == 0:
        await context.enqueue_next_page(
            BatchOrchestratorPage(MyCursor(page.cursor.i + page.page_size), page.page_size)
        )
        print(f"Signaled the workflow {page}")
    print(f"Processing page {page}")
    return context.get_page().cursor.i

# Testing with spawns_second_page will ensure that the workflow is signaled and that it processes the second page
@pytest.mark.asyncio
async def test_two_pages(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with batch_worker(client, task_queue_name):
        input = BatchOrchestratorInput('my_batch', 'spawns_second_page', MyCursor(0))
        handle = await client.start_workflow(
            BatchOrchestrator.run, id=str(uuid.uuid4()), arg=input, task_queue=task_queue_name
        )
        result = await handle.result()
        assert result.num_pages_processed == 2
