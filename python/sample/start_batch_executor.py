import asyncio
from dataclasses import dataclass
from typing import Generic
from temporalio.client import Client

from batch_executor import BatchExecutor, BatchExecutorInput


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Execute the batch executor workflow
    result = await client.execute_workflow(BatchExecutor.run, BatchExecutorInput("my name"), id="my-workflow-id", task_queue="my-task-queue")

    print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())