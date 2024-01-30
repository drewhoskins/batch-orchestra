# Having set up a temporal server at localhost:7233,
# you can start this sample worker with this script:
#    poetry run python sample/run_worker.py
import asyncio
import multiprocessing
import signal
import sys, os

from temporalio.client import Client
from temporalio.worker import Worker

import logging

from batch_orchestrator import BatchOrchestrator, process_page
# Import our registry of page processors
import inflate_product_prices_page_processor

interrupt_event = asyncio.Event()


async def worker_async():
    logging.basicConfig(level=logging.INFO)
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

def worker():
    asyncio.run(worker_async())

def main():
    processes = []

    for _ in range(5): 
        p = multiprocessing.Process(target=worker)
        p.start()
        processes.append(p)

    return processes

if __name__ == "__main__":
    processes = main()

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating processes")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
