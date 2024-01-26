import asyncio
from dataclasses import dataclass
from typing import Generic
from temporalio.client import Client
import sys, os
print(os.path.join(os.path.dirname(sys.path[0]), '../'))
sys.path.append(os.path.join(os.path.dirname(sys.path[0]), '../'))

from python.batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Execute the batch executor workflow
    result = await client.execute_workflow(BatchOrchestrator.run, BatchOrchestratorInput("my name"), id="my-workflow-id", task_queue="my-task-queue")

    print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())