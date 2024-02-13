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

    from temporalio.client import Client
    from temporalio.types import MethodAsyncSingleParam

    from batch_orchestrator_io import BatchOrchestratorProgress
    from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput

    from inflate_product_prices_page_processor import inflate_product_prices, ConfigArgs, make_temporal_client
    from product_db import ProductDB
    from batch_orchestrator_io import batch_orchestrator_data_converter
except ModuleNotFoundError as e:
    print("This script requires poetry.  `poetry run python samples/perform_sql_batch_migration.py`.")
    print(f"Original error: {e}")
    sys.exit(1)

                                                                              
#
# This sample shows a typical parallel batch migration of an entire sqlite table on an example table of products
# and their prices.  This file is the caller; see inflate_product_prices_page_processor.py for the implementation.
# To run this sample:
#  1. Start a temporal server locally with docker-compose up
#  2. Start your workers with poetry run python samples/run_workers.py
#  3. Run this script with poetry run python samples/perform_sql_batch_migration.py
#
async def main(num_items):
    client = await Client.connect("localhost:7233", data_converter=batch_orchestrator_data_converter)
    print("Make sure to run the sample workers with `poetry run python samples/run_workers.py` if you haven't.")
    # Create a temporary database which we'll clean up at thte end.
    db_file = NamedTemporaryFile(suffix="_my_product.db", delete=False)
    print(f"Creating a temporary database in {db_file.name}")
    # Create client connected to server at the given address
    db_connection = ProductDB.get_db_connection(db_file.name)

    ProductDB.populate_table(db_connection, num_records=num_items)

    try:
        cursor = ""

        print(f"Starting migration on --num_items={num_items} items.  Check your worker's output to see what's happening in detail.")
        args = ConfigArgs(db_file=db_file.name)
        page_size = 200
        # Execute the migration
        handle = await client.start_workflow(
            BatchOrchestrator.run,  # type: ignore (unclear why this is necessary, but mypy complains without it.)
            BatchOrchestratorInput(
                temporal_client_factory_name=make_temporal_client.__name__,
                batch_id="inflate_product_prices", 
                max_parallelism=5,
                page_processor=BatchOrchestratorInput.PageProcessorContext(
                    name=inflate_product_prices.__name__, 
                    page_size=page_size,
                    args=args.to_json())
                ),
            id=f"inflate_product_prices-{str(uuid.uuid4())}", 
            task_queue="my-task-queue"
            )
        
        # Suppose we want to track intermediate progress.  We can query the BatchOrchestrator for its current state.
        time_slept = 0
        while True:
            await sleep(5)
            time_slept += 5
            interim_result = await handle.query(BatchOrchestrator.current_progress)
            assert interim_result.num_completed_pages <= 1 + (num_items / page_size)
            print(f"Current progress after {time_slept} seconds: {interim_result}")
            if interim_result.num_completed_pages == 1 + (num_items / page_size):
                break

        result = await handle.result()

        print(f"Migration finished after {time_slept} seconds with {result.num_completed_pages} pages processed.\n"+
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample for using BatchOrchestrator to run a batch migration on an entire sqlite table.")
    parser.add_argument("--num_items", type=int, default=2000, help="The number of items to populate the table with and process.")
    parser.usage = "poetry run python samples/perform_sql_batch_migration.py --num_items <N, default 1000>"
    args = parser.parse_args()
    asyncio.run(main(args.num_items))