from asyncio import sleep
import sys

try:
    import asyncio
    from dataclasses import dataclass
    from tempfile import NamedTemporaryFile
    from typing import Generic
    import uuid
    import time
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
This script requires poetry.  `poetry run python samples/pause_and_resume_processing.py`.
But if you haven't, first see Python Quick Start in python/README.md for instructions on installing and setting up poetry.
Original error: {e}
        """)
    sys.exit(1)

"""
This sample shows how to pause and resume a batch job, or more generally how to change the level of parallelism.  
"""
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
        
        # start it
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

        time_slept = 0

        # pause it when things have kicked off so we can prove that we were initially doing something
        while True:
            await sleep(1)
            time_slept += 1
            try:
                progress = await handle.query(BatchOrchestrator.current_progress)
            except temporalio.service.RPCError as e:
                print(f"Waiting for workflow {handle.id} to start...")
            else:
                if progress.is_finished:
                    raise RuntimeError(f"Workflow {handle.id} has already finished -- didn't have time to pause!  Consider increasing --num_items.")
                if progress.num_processing_pages + progress.num_completed_pages > 0:
                    print(f"Progress before pausing (after {time_slept} seconds): {progress}")
                    break

        print("Pausing processing by calling set_max_parallelism(0)")
        await handle.signal(BatchOrchestrator.set_max_parallelism, 0)

        # Wait until progress stops progressing
        while True:
            await sleep(1)
            time_slept += 1
            progress = await handle.query(BatchOrchestrator.current_progress)
            if progress.is_finished:
                raise RuntimeError(f"Workflow {handle.id} has already finished -- didn't have time to resume!  Consider increasing --num_items.")

            if progress.num_processing_pages == 0:
                print(f"Success!  Progress has stopped after {time_slept} seconds: {progress}")
                break

        # resume it
        print("Resuming processing by calling restore_max_parallelism")
        await handle.signal(BatchOrchestrator.restore_max_parallelism)

        # await the finish.
        while True:
            await sleep(2)
            time_slept += 2
            progress = await handle.query(BatchOrchestrator.current_progress)
            print(f"Current progress after {time_slept} seconds: {progress}")
            if progress.is_finished:
                break
        result = await handle.result()
        print(f"Migration finished after less than {time_slept} seconds with {result.num_completed_pages} pages processed.\n"+
              f"Max parallelism achieved: {result.max_parallelism_achieved}.")

    finally:
        os.remove(db_file.name)
        info = await handle.describe()
        if info.status == temporalio.client.WorkflowExecutionStatus.RUNNING:
            print("\nCanceling workflow") 
            await handle.cancel()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample for using BatchOrchestrator to pause and then resume a batch migration.")
    parser.add_argument("--num_items", type=int, default=10000, help="The number of items to populate the table with and process.")
    parser.usage = "poetry run python samples/pause_and_resume_processing.py --num_items <N, default 10000>"
    args = parser.parse_args()
    asyncio.run(main(args.num_items))