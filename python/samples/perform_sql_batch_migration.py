from asyncio import sleep
import sys
from typing import Any, Optional

from batch_orchestrator_client import BatchOrchestratorClient, BatchOrchestratorHandle

try:
    import asyncio
    from tempfile import NamedTemporaryFile
    import uuid
    import os
    import argparse

    from temporalio.client import Client, WorkflowHandle
    import temporalio.service

    from batch_orchestrator_io import BatchOrchestratorProgress
    from batch_orchestrator import BatchOrchestratorInput

    from samples.lib.inflate_product_prices_page_processor import InflateProductPrices, ConfigArgs
    from samples.lib.product_db import ProductDB
except ModuleNotFoundError as e:
    print(f"""
This script requires poetry.  `poetry run python samples/perform_sql_batch_migration.py`.
But if you haven't, first see Python Quick Start in python/README.md for instructions on installing and setting up poetry.
Original error: {e}
        """)
    sys.exit(1)

                                                                              
#
# This sample shows a typical parallel batch migration of an entire sqlite table on an example table of products
# and their prices.  This file is the caller; see inflate_product_prices_page_processor.py for the implementation.
# To run this sample:
#  1. Start a temporal server locally, e.g. with 
#     docker-compose up
#   or 
#     temporal server start-dev --db-filename batch_orchestra_samples.db
#  2. Start your workers with 
#     poetry run python samples/run_workers.py
#  3. Run this script with 
#     poetry run python samples/perform_sql_batch_migration.py
#
async def main(num_items, pages_per_run, name: Optional[str]):
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

    print("Make sure to run the sample workers with `poetry run python samples/run_workers.py` if you haven't.")

    # Create a temporary database which we'll clean up at the end.
    db_file = NamedTemporaryFile(suffix="_my_product.db", delete=False)
    print(f"Creating a temporary database in {db_file.name}")
    db_connection = ProductDB.get_db_connection(db_file.name)

    ProductDB.populate_table(db_connection, num_records=num_items)
    try:
        print(f"Starting migration on --num_items={num_items} items.  Check your worker's output to see what's happening in detail.")
        args = ConfigArgs(db_file=db_file.name)
        page_size = 200
        # Execute the migration
        handle: BatchOrchestratorHandle[Any, BatchOrchestratorProgress] = await BatchOrchestratorClient(temporal_client).start(
            BatchOrchestratorInput(
                max_parallelism=5,
                page_processor=BatchOrchestratorInput.PageProcessorContext(
                    name=InflateProductPrices.__name__, 
                    page_size=page_size,
                    args=args.to_json()),
                pages_per_run=pages_per_run
            ),
            id=f"inflate_product_prices-{name or str(uuid.uuid4())}", 
            task_queue="my-task-queue"
            )
        
        # Suppose we want to track intermediate progress.  We could add a batch_tracker to run a tracker on the worker, 
        # but for this sample, we'll query the BatchOrchestrator so we can show something in this console window.
        time_slept = 0
        while True:
            await sleep(5)
            time_slept += 5
            try:
                progress = await handle.get_progress()
            except temporalio.service.RPCError:
                print(f"Waiting for workflow {handle.workflow_handle.id} to start...")
            else:
                if progress.is_finished:
                    break
                print(f"Current progress after {time_slept} seconds: {progress}")
        result = await handle.result()

        print(f"Migration finished after less than {time_slept} seconds with {result.num_completed_pages} pages processed.\n"+
              f"Max parallelism achieved: {result.max_parallelism_achieved}.")

        # Verify that we adjusted the prices on all rows.
        db_connection = ProductDB.get_db_connection(db_file.name)
        num_bad_apples = 0

        for (i, product) in ProductDB.for_each_product(db_connection):
            if not product.did_inflate_migration:
                num_bad_apples += 1
                print(f"Record {i} not migrated: {product}")
        if num_bad_apples > 0:
            raise Exception("Found unmigrated products.  This shouldn't happen.")

    finally:
        os.remove(db_file.name)
        info = await handle.workflow_handle.describe()
        if info.status == temporalio.client.WorkflowExecutionStatus.RUNNING:
            print("\nCanceling workflow") 
            await handle.workflow_handle.cancel()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample for using BatchOrchestrator to run a batch migration on an entire sqlite table.")
    parser.add_argument("--num_items", type=int, default=2000, help="The number of items to populate the table with and process.")
    parser.add_argument(
        "--pages_per_run", 
        type=int, 
        help="The number of items to process in run of the BatchOrchestrator workflow.  By default, it's as many pages until Temporal "\
         "suggests to start a new run.")
    parser.add_argument(
        "--job_name",
        type=str,
        help="Workflow will be called inflate_product_prices-{job_name}", 
        default=None
    )
    parser.usage = "poetry run python samples/perform_sql_batch_migration.py --num_items <N, default 2000> "\
        "--pages_per_run <N, default unspecified> --job_name <name, default UUID>"
    args = parser.parse_args()
    asyncio.run(main(args.num_items, args.pages_per_run, args.job_name))
