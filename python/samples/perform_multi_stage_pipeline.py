import sys
from asyncio import sleep
from typing import Optional

try:
    import asyncio
    import os
    import sys
    import uuid
    from tempfile import NamedTemporaryFile

    # Add ../ to the path for PIP, so we can use absolute imports in the samples.
    sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

    import argparse
    import os

    import temporalio.service
    from temporalio.client import Client

    from batch_orchestra.batch_orchestrator import BatchOrchestratorInput
    from batch_orchestra.batch_orchestrator_client import BatchOrchestratorClient, BatchOrchestratorHandle
    from samples.lib.inflate_product_prices_page_processor import ConfigArgs
    from samples.lib.product_db import ProductDB
    from samples.lib.product_pipeline_stages import (
        AuditProductPricesStage,
        FetchProductPage,
        InflateProductPricesStage,
    )
except ModuleNotFoundError:
    import traceback
    print(f"""
Failed to import modules.
If you're using poetry, run `poetry run python samples/perform_multi_stage_pipeline.py`.
To set up poetry, or alternatively to set up a virtual environment, first see Python Quick Start in python/README.md.
Original error:
{traceback.format_exc()}
        """)
    sys.exit(1)


#
# This sample runs the same product-price migration as perform_sql_batch_migration.py, but splits it into a
# three-stage pipeline: fetch -> inflate -> audit.  See samples/lib/product_pipeline_stages.py for the stages.
#
# Why you might want stages:
#   * Each stage gets its own parallelism budget, so you can read widely while writing gently.
#   * Pages pipeline: page 3 can be fetching while page 1 is being audited.
#   * Each stage retries independently, so an audit failure doesn't re-run the write.
#
# To run this sample:
#  1. Start a temporal server locally, e.g. with
#     docker-compose up
#   or
#     temporal server start-dev --db-filename batch_orchestra_samples.db
#  2. Start your workers with
#     poetry run python samples/run_workers.py
#  3. Run this script with
#     poetry run python samples/perform_multi_stage_pipeline.py
#
async def main(num_items: int, name: Optional[str]):
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
        print(f"Starting a three-stage migration on --num_items={num_items} items.")
        args = ConfigArgs(db_file=db_file.name)
        handle: BatchOrchestratorHandle = await BatchOrchestratorClient(temporal_client).start(
            BatchOrchestratorInput(
                # Applies to each stage separately, unless the stage overrides it.
                max_parallelism=5,
                page_processor=BatchOrchestratorInput.PageProcessorContext(
                    name=FetchProductPage.__name__, page_size=200, args=args.to_json()
                ),
                subsequent_stages=[
                    # The write stage is the one our database cares about, so run fewer of them at a time, and don't
                    # let the fetch stage run more than 10 pages ahead of it.
                    BatchOrchestratorInput.StageContext(
                        name=InflateProductPricesStage.__name__,
                        args=args.to_json(),
                        max_parallelism=2,
                        max_queued_pages=10,
                    ),
                    BatchOrchestratorInput.StageContext(
                        name=AuditProductPricesStage.__name__, args=args.to_json()
                    ),
                ],
            ),
            id=f"product_pipeline-{name or str(uuid.uuid4())}",
            task_queue="my-task-queue",
        )

        print(
            f"See your batch job at http://localhost:8233/namespaces/default/workflows/{handle.workflow_handle.id}/{handle.workflow_handle.first_execution_run_id}/history."
        )

        # Watch the pages flow through the stages.
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
                print(f"After {time_slept} seconds, {progress.num_completed_pages} pages have finished the pipeline.")
                for stage in progress.stages:
                    print(
                        f"    stage {stage.stage_num} {stage.stage_name}: {stage.num_pending_pages} waiting, "
                        + f"{stage.num_processing_pages} running, {stage.num_completed_pages} done "
                        + f"(max parallelism {stage.max_parallelism_achieved}/{stage.max_parallelism})"
                    )
        result = await handle.result()

        print(
            f"\nPipeline finished after less than {time_slept} seconds; {result.num_completed_pages} pages made it "
            + f"through all {len(result.stages)} stages.\n"
            + f"Peak pages in flight anywhere in the pipeline: {result.max_parallelism_achieved}."
        )
        for stage in result.stages:
            print(f"    stage {stage.stage_num} {stage.stage_name}: {stage.num_completed_pages} pages")

        # The audit stage already verified each page as it went, so this is just a belt-and-suspenders check.
        db_connection = ProductDB.get_db_connection(db_file.name)
        num_bad_apples = sum(
            1 for _, product in ProductDB.for_each_product(db_connection) if not product.did_inflate_migration
        )
        if num_bad_apples > 0:
            raise Exception(f"Found {num_bad_apples} unmigrated products.  This shouldn't happen.")

    finally:
        os.remove(db_file.name)
        info = await handle.workflow_handle.describe()
        if info.status == temporalio.client.WorkflowExecutionStatus.RUNNING:
            print("\nCanceling workflow")
            await handle.workflow_handle.cancel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sample for using BatchOrchestrator to run a multi-stage pipeline over a sqlite table."
    )
    parser.add_argument(
        "--num_items", type=int, default=2000, help="The number of items to populate the table with and process."
    )
    parser.add_argument(
        "--job_name", type=str, help="Workflow will be called product_pipeline-{job_name}", default=None
    )
    parser.usage = (
        "poetry run python samples/perform_multi_stage_pipeline.py --num_items <N, default 2000> "
        "--job_name <name, default UUID>"
    )
    args = parser.parse_args()
    asyncio.run(main(args.num_items, args.job_name))
