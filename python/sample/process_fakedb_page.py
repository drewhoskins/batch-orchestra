from asyncio import sleep
from python.batch_orchestrator_page import BatchOrchestratorPage, MyCursor
from python.batch_page_processor_context import BatchPageProcessorContext
from python.batch_page_processor_registry import page_processor


@page_processor
async def process_fakedb_page(context: BatchPageProcessorContext):
    page = context.getPage()
    if page.cursor.i == 0:
        await context.enqueueNextPage(
            BatchOrchestratorPage(MyCursor(page.cursor.i + page.pageSize), page.pageSize)
        )
        print(f"Signaled the workflow {page}")
    print(f"Processing page {page}")
    # Sleep for a bit to simulate work
    await sleep(1)