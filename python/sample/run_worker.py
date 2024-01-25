import asyncio
import concurrent.futures
from temporalio.client import Client
from temporalio.worker import Worker

# Import the activity and workflow from our other files
from batch_executor import BatchExecutor, my_process_batch

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

interrupt_event = asyncio.Event()


async def main():
    # Connect client
    client = await Client.connect("localhost:7233")

    # Run a worker for the activities and workflow
    async with Worker(
        client,
        task_queue="my-task-queue",
        activities=[my_process_batch],
        workflows=[BatchExecutor],
    ):
        # Wait until interrupted
        logging.info("Worker started, ctrl+c to exit")
        await interrupt_event.wait()
        logging.info("Shutting down")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        interrupt_event.set()
        loop.run_until_complete(loop.shutdown_asyncgens())
