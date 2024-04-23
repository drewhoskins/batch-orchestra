from asyncio import sleep
import sys

try:
    import asyncio
    from dataclasses import dataclass
    from tempfile import NamedTemporaryFile
    from typing import Generic
    import uuid
    import os
    import argparse

    from temporalio.client import Client, WorkflowHandle
    from temporalio.types import MethodAsyncSingleParam
    import temporalio.service

    from batch_orchestrator_io import BatchOrchestratorProgress
    from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput

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
async def main(num_items):
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
        handle: WorkflowHandle = await temporal_client.start_workflow(
            BatchOrchestrator.run,  # type: ignore (unclear why this is necessary, but mypy complains without it.)
            BatchOrchestratorInput(
                max_parallelism=5,
                page_processor=BatchOrchestratorInput.PageProcessorContext(
                    name=InflateProductPrices.__name__, 
                    page_size=page_size,
                    args=args.to_json())
                ),
            id=f"inflate_product_prices-{str(uuid.uuid4())}", 
            task_queue="my-task-queue"
            )
        
        # Suppose we want to track intermediate progress.  We could add a batch_tracker to run a tracker on the worker, 
        # but for this sample, we'll query the BatchOrchestrator so we can show something in this console window.
        time_slept = 0
        while True:
            await sleep(5)
            time_slept += 5
            try:
                progress = await handle.query(BatchOrchestrator.current_progress)
            except temporalio.service.RPCError as e:
                print(f"Waiting for workflow {handle.id} to start...")
            else:
                if progress.is_finished:
                    break
                print(f"Current progress after {time_slept} seconds: {progress}")
        result = await handle.result()

        print(f"Migration finished after less than {time_slept} seconds with {result.num_completed_pages} pages processed.\n"+
              f"Max parallelism achieved: {result.max_parallelism_achieved}.")

        # Verify that we adjusted the prices on all rows.
        db_connection = ProductDB.get_db_connection(db_file.name)
        cursor = ""
        num_bad_apples = 0

        for (i, product) in ProductDB.for_each_product(db_connection):
            if not product.did_inflate_migration:
                num_bad_apples += 1
                print(f"Record {i} not migrated: {product}")
        if num_bad_apples > 0:
            raise Exception("Found unmigrated products.  This shouldn't happen.")

    finally:
        os.remove(db_file.name)
        info = await handle.describe()
        if info.status == temporalio.client.WorkflowExecutionStatus.RUNNING:
            print("\nCanceling workflow") 
            await handle.cancel()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample for using BatchOrchestrator to run a batch migration on an entire sqlite table.")
    parser.add_argument("--num_items", type=int, default=2000, help="The number of items to populate the table with and process.")
    parser.usage = "poetry run python samples/perform_sql_batch_migration.py --num_items <N, default 2000>"
    args = parser.parse_args()
    asyncio.run(main(args.num_items))