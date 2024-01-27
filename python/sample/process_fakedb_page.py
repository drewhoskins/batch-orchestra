from asyncio import sleep
from batch_orchestrator_page import BatchOrchestratorPage, MyCursor
from batch_page_processor_context import BatchPageProcessorContext
from batch_page_processor_registry import page_processor


@page_processor
async def process_fakedb_page(context: BatchPageProcessorContext):
    page = context.get_page()
    if page.cursor.i == 0:
        await context.enqueue_next_page(
            BatchOrchestratorPage(MyCursor(page.cursor.i + page.page_size), page.page_size)
        )
        print(f"Signaled the workflow {page}")
    print(f"Processing page {page}")
    # Sleep for a bit to simulate work
    await sleep(1)