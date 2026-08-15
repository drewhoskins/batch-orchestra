import asyncio
import os
import uuid
from datetime import datetime
from tempfile import NamedTemporaryFile

import streamlit as st
from batch_orchestrator import BatchOrchestratorInput
from batch_orchestrator_client import BatchOrchestratorClient, BatchOrchestratorHandle
from batch_orchestrator_io import BatchOrchestratorProgress
from samples.lib.inflate_product_prices_page_processor import (
    ConfigArgs,
    InflateProductPrices,
)
from samples.lib.product_db import ProductDB
from temporalio.client import Client, WorkflowExecutionStatus

TEMPORAL_HOST = "localhost:7233"

DEFAULT_NUM_ITEMS = 2000
DEFAULT_NUM_PAGES = 200
PAGES_PER_RUN = None


async def configure_temporal_client() -> Client | None:
    try:
        temporal_client = await Client.connect(TEMPORAL_HOST)
        return temporal_client
    except RuntimeError as e:
        st.error(f"""
            Could not connect to temporal-server at {TEMPORAL_HOST}.  Check the README.md Python Quick Start if you need guidance.
            Original error: {e}
           """)


# https://discuss.streamlit.io/t/how-do-you-hide-buttons-after-clicking-them/86671/2
def hide_buttons():
    st.markdown(
        """
        <style>
        button[data-testid="stBaseButton-secondary"] {
            display: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def create_example_database(num_items: int = DEFAULT_NUM_ITEMS):
    db_file = NamedTemporaryFile(suffix="_my_product.db", delete=False)
    print(f"Creating a temporary database in {db_file.name}")
    db_connection = ProductDB.get_db_connection(db_file.name)

    ProductDB.populate_table(db_connection, num_records=num_items)
    return db_file


def visualise_progress(
    status_widget,
    progress_bar,
    progress_info: BatchOrchestratorProgress,
    page_estimate: int,
):
    status_widget.update(
        label=f"Processing {progress_info.num_processing_pages} pages, completed {progress_info.num_completed_pages} pages"
    )

    estimated_completion_percent = progress_info.num_completed_pages / page_estimate
    progress_bar.progress(estimated_completion_percent)


def visualise_finished():
    st.balloons()


async def run_batch_checker(handle: BatchOrchestratorHandle, num_items, page_size):
    status = st.status("Checking batch progress....")
    progress = st.progress(0)

    # Sadly we won't know this in a real world scenario....
    page_estimate = int(num_items / page_size) + 1

    while True:
        await asyncio.sleep(1)
        progress_details = await handle.get_progress()
        visualise_progress(
            status_widget=status,
            progress_bar=progress,
            progress_info=progress_details,
            page_estimate=page_estimate,
        )
        if progress_details.is_finished:
            progress.empty()
            duration = (
                datetime.now()
                - datetime.fromtimestamp(progress_details._start_timestamp)
            ).total_seconds()
            status.update(
                label=f"Batch processing complete: {progress_details.num_completed_pages} pages processed in {duration} seconds",
                state="complete",
            )
            visualise_finished()
            break


async def app():
    st.title("DB Migration Progress Viewer")
    client = await configure_temporal_client()

    if client:
        try:
            col1, col2 = st.columns(2)
            with col1:
                num_items = st.number_input(
                    "Number of database items to simulate",
                    min_value=1,
                    max_value=1000000,
                    value=DEFAULT_NUM_ITEMS,
                    step=500,
                )
            with col2:
                page_size = st.number_input(
                    "Number of pages for batch processing",
                    min_value=1,
                    max_value=1000000,
                    value=DEFAULT_NUM_PAGES,
                    step=50,
                )
            _, middle, _ = st.columns(3)
            with middle:
                button = st.button("Start batch orchestra! :violin:")

            db_file = create_example_database(num_items)

            args = ConfigArgs(db_file=db_file.name)
            handle = await BatchOrchestratorClient(client).start(
                BatchOrchestratorInput(
                    max_parallelism=5,
                    page_processor=BatchOrchestratorInput.PageProcessorContext(
                        name=InflateProductPrices.__name__,
                        page_size=page_size,
                        args=args.to_json(),
                    ),
                    pages_per_run=PAGES_PER_RUN,
                ),
                id=f"inflate_product_prices-{str(uuid.uuid4())}",
                task_queue="my-task-queue",
            )

            if button:
                hide_buttons()
                middle.link_button(
                    "View in Temporal",
                    icon="🔗",
                    url=f"http://localhost:8233/namespaces/default/workflows/{handle.workflow_handle.id}/{handle.workflow_handle.first_execution_run_id}/history",
                )
                await run_batch_checker(handle, num_items, page_size)

        finally:
            os.remove(db_file.name)
            info = await handle.workflow_handle.describe()
            if info.status == WorkflowExecutionStatus.RUNNING:
                print("\nCanceling workflow")
                await handle.workflow_handle.cancel()


if __name__ == "__main__":
    asyncio.run(app())
