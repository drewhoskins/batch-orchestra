try:
    import argparse
    import asyncio
    import logging
    import multiprocessing
    import os
    import sys

    # Add ../ to the path for PIP, so we can use absolute imports in the samples.
    sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

    from temporalio.client import Client
    from temporalio.worker import Worker

    # Import our registry of page processors which are registered with @page_processor.
    # Without importing this, they will not be registered.
    import samples.lib.inflate_product_prices_page_processor  # noqa: F401
    from batch_orchestra.batch_orchestrator import BatchOrchestrator, process_page
    from batch_orchestra.batch_worker import BatchWorkerClient
except ModuleNotFoundError:
    import traceback
    print(f"""
Failed to import modules.
If you're using poetry, run `poetry run python samples/run_workers.py`.
To set up poetry, or alternatively to set up a virtual environment, first see Python Quick Start in python/README.md.
Original error:
{traceback.format_exc()}
    """)
    sys.exit(1)

logging.basicConfig(level=logging.INFO)

interrupt_event = asyncio.Event()

TEMPORAL_HOST = "localhost:7233"
TASK_QUEUE = "my-task-queue"


async def worker_async():
    """
    Start a worker to handle BatchOrchestrator workflows, including any registered page_processor
    activities.
    """
    try:
        # Set up the connection to temporal-server.
        temporal_client = await Client.connect(TEMPORAL_HOST)
        temporal_client = BatchWorkerClient.register(temporal_client)
    except RuntimeError as e:
        logging.error(f"""
            Could not connect to temporal-server at {TEMPORAL_HOST}. Check the README.md Python Quick Start if you need guidance.
            Original error: {e}
        """)
        sys.exit(1)

    # Run a worker for the activities and workflow
    async with Worker(
        temporal_client,
        task_queue=TASK_QUEUE,
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


def main(num_processes: int):
    processes = []

    for _ in range(num_processes):
        p = multiprocessing.Process(target=worker)
        p.start()
        processes.append(p)

    return processes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run a configurable number of Temporal workers to handle BatchOrchestrator workflows.",
        usage="poetry run python samples/run_workers.py",
    )
    parser.add_argument(
        "--num_processes",
        type=int,
        default=5,
        help="Number of worker processes to start.",
    )
    args = parser.parse_args()

    processes = main(num_processes=args.num_processes)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating processes")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join()
