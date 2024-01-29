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


#
# This sample shows a typical parallel batch migration of an entire sqlite table on an example table of products
# and their prices.  This file is the caller; see inflate_product_prices_page_processor.py for the implementation.
# To run this sample:
#  1. Start a temporal server locally with docker-compose up
#  2. Start your worker with poetry run python sample/run_worker.py
#  3. Run this script with poetry run python sample/perform_sql_batch_migration.py
#
async def main():
    client = await Client.connect("localhost:7233")

    # Create a temporary database which we'll clean up at thte end.
    db_file = NamedTemporaryFile(suffix="_my_product.db", delete=False)
    # Create client connected to server at the given address
    db_connection = ProductDB.get_db_connection(db_file.name)
    num_records = 1000
    ProductDB.populate_table(db_connection, num_records=num_records)

    try:
        print(f"Creating a temporary database in {db_file.name}")
        db_connection = ProductDB.get_db_connection(db_file.name)
        cursor = ""
        # Make sure we initialized the page pre-migration
        for (i, product) in ProductDB.for_each_product(db_connection):
            print(f"Record {i} before migration: {product}")

        args = ConfigArgs(db_file=db_file.name)
        # Execute the migration
        result = await client.execute_workflow(
            BatchOrchestrator.run, 
            BatchOrchestratorInput(
                batch_name="inflate_product_prices_jan_2024", 
                page_processor=inflate_product_prices.__name__, 
                max_parallelism=5,
                page_size=100,
                page_processor_args=args.to_json()), 
            id=f"inflate_product_prices_jan_2024-{str(uuid.uuid4())}", 
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
            raise Exception("Found unmigrated products")

    finally:
        os.remove(db_file.name)

if __name__ == "__main__":
    asyncio.run(main())