import sys

try:
    import argparse
    from batch_orchestra.batch_orchestrator_client import BatchOrchestratorClient, BatchOrchestratorHandle
    import asyncio

    from batch_orchestrator_client import BatchOrchestratorClient, BatchOrchestratorHandle
    from temporalio.client import Client
except ModuleNotFoundError as e:
    print(f"""
This script requires poetry.  `poetry run python samples/perform_sql_batch_migration.py`.
But if you haven't, first see Python Quick Start in python/README.md for instructions on installing and setting up poetry.
Original error: {e}
        """)
    sys.exit(1)


async def main(max_parallelism, job_name):
    # Set up the connection to temporal-server.
    host = "localhost:7233"
    try:
        temporal_client = await Client.connect(host)
    except RuntimeError as e:
        print(f"""
Could not connect to temporal-server at {host}.  Check the README.md Python Quick Start if you need guidance.
Original error: {e}
           """)
        sys.exit(1)

    workflow_id = f"inflate_product_prices-{job_name}"
    handle: BatchOrchestratorHandle = BatchOrchestratorClient(temporal_client).get_handle(workflow_id)
    await handle.set_max_parallelism(max_parallelism)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Set the parallelism of a batch job created with perform_sql_batch_migration.py."
    )
    parser.add_argument(
        "--max_parallelism",
        type=int,
        help="The maximum number of pages to process at once.  0 means pause the job.",
    )
    parser.add_argument(
        "--job_name",
        type=str,
        help="Workflow will be called inflate_product_prices-{job_name}",
    )

    parser.usage = "poetry run python samples/pause_and_resume_processing.py --job_name <name> --max_parallelism <int>"
    args = parser.parse_args()
    asyncio.run(main(args.max_parallelism, args.job_name))
