import asyncio
from dataclasses import dataclass
from typing import Generic
from temporalio.client import Client
import sys, os
from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Execute the batch executor workflow
    result = await client.execute_workflow(BatchOrchestrator.run, BatchOrchestratorInput("my name", 'process_fakedb_page'), id="my-workflow-id", task_queue="my-task-queue")

    print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())