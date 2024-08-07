# Having set up a temporal server,
# you can start 5 sample workers with this script:
#    poetry run python samples/run_workers.py
import sys

from batch_worker import BatchWorkerClient

try:
    import asyncio
    import multiprocessing
    import logging

    from temporalio.client import Client
    from temporalio.worker import Worker

    from batch_orchestrator import BatchOrchestrator, process_page

    # Import our registry of page processors which are registered with @page_processor.
    # Without importing this, they will not be registered.
    import samples.lib.inflate_product_prices_page_processor # noqa: F401
except ModuleNotFoundError as e:
    print(f"""
This script requires poetry.  `poetry run python samples/perform_sql_batch_migration.py`.
But if you haven't, first see Python Quick Start in python/README.md for instructions on installing and setting up poetry.
Original error: {e}
        """)
    sys.exit(1)

interrupt_event = asyncio.Event()


async def worker_async():
    logging.basicConfig(level=logging.INFO)
    # Set up the connection to temporal-server.
    host = "localhost:7233"
    try:
        temporal_client = await Client.connect(host)
        temporal_client = BatchWorkerClient.register(temporal_client)
    except RuntimeError as e:
        print(f"""
Could not connect to temporal-server at {host}.  Check the README.md Python Quick Start if you need guidance.
Original error: {e}
           """)
        sys.exit(1)

    # Run a worker for the activities and workflow
    async with Worker(
        temporal_client,
        task_queue="my-task-queue",
        activities=[process_page],
        workflows=[BatchOrchestrator],
        debug_mode=True,
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
