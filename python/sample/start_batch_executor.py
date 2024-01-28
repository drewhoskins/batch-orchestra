import asyncio
from dataclasses import dataclass
from typing import Generic
from temporalio.client import Client
import sys, os
from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput

from sample.process_fakedb_page import process_fakedb_page, FakeDBCursor


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Execute the batch executor workflow
    result = await client.execute_workflow(
        BatchOrchestrator.run, 
        BatchOrchestratorInput(batch_name="my name", page_processor=process_fakedb_page.__name__, first_cursor=FakeDBCursor(0).to_json(), max_parallelism=3), 
        id="my-workflow-id", 
        task_queue="my-task-queue")

    print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())