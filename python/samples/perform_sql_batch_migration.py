try:
    import asyncio
    from dataclasses import dataclass
    from tempfile import NamedTemporaryFile
    from typing import Generic
    import uuid
    from temporalio.client import Client
    import sys, os
    from batch_orchestrator import BatchOrchestrator, BatchOrchestratorInput

    from inflate_product_prices_page_processor import inflate_product_prices, ConfigArgs, ProductDBCursor
    from product_db import ProductDB
    import argparse
    from batch_orchestrator_data import batch_orchestrator_data_converter
except ModuleNotFoundError:
    print("This script requires poetry.  `poetry run python samples/perform_sql_batch_migration.py`.")
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

    # Create a temporary database which we'll clean up at thte end.
    db_file = NamedTemporaryFile(suffix="_my_product.db", delete=False)
    print(f"Creating a temporary database in {db_file.name}")
    # Create client connected to server at the given address
    db_connection = ProductDB.get_db_connection(db_file.name)

    ProductDB.populate_table(db_connection, num_records=num_items)

    try:
        cursor = ""

        print(f"Starting migration on {num_items} items.  Check your worker's output to see what's happening.")
        args = ConfigArgs(db_file=db_file.name)
        # Execute the migration
        result = await client.execute_workflow(
            BatchOrchestrator.run, 
            BatchOrchestratorInput(
                batch_id="inflate_product_prices", 
                page_processor_name=inflate_product_prices.__name__, 
                max_parallelism=5,
                page_size=100,
                page_processor_args=args.to_json()), 
            id=f"inflate_product_prices-{str(uuid.uuid4())}", 
            task_queue="my-task-queue")

        print(f"Migration finished with {result.num_pages_processed} pages processed.  Max parallelism achieved: {result.max_parallelism_achieved}.")

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
    parser.add_argument("--num_items", type=int, default=1000, help="The number of items to populate the table with and process.")
    parser.usage = "poetry run python samples/perform_sql_batch_migration.py --num_items <N, default 1000>"
    args = parser.parse_args()
    asyncio.run(main(args.num_items))