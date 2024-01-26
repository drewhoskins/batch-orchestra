import asyncio
import sys, os
print(os.path.join(os.path.dirname(sys.path[0]), '../'))
sys.path.append(os.path.join(os.path.dirname(sys.path[0]), '../'))

from temporalio.client import Client
from temporalio.worker import Worker

import logging

from python.batch_orchestrator import BatchOrchestrator, process_batch
# Import our registry of page processors
import process_fakedb_page

interrupt_event = asyncio.Event()


async def main():
    # Connect client
    client = await Client.connect("localhost:7233")

    # Run a worker for the activities and workflow
    async with Worker(
        client,
        task_queue="my-task-queue",
        activities=[process_batch],
        workflows=[BatchOrchestrator],
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
