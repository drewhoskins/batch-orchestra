# Having set up a temporal server at localhost:7233,
# you can start this sample worker with this script:
#    poetry run python sample/run_worker.py
import asyncio
import sys, os

from temporalio.client import Client
from temporalio.worker import Worker

import logging

from batch_orchestrator import BatchOrchestrator, process_page
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
        activities=[process_page],
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
