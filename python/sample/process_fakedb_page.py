from __future__ import annotations
from asyncio import sleep
from dataclasses import asdict, dataclass
import json
from batch_processor import BatchProcessorContext, BatchPage, page_processor

@dataclass
class FakeDBCursor:
    i: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @staticmethod
    def from_json(json_str) -> FakeDBCursor:
        return FakeDBCursor(**json.loads(json_str))


@page_processor
async def process_fakedb_page(context: BatchProcessorContext):
    page = context.get_page()
    cursor = FakeDBCursor.from_json(page.cursor)
    if cursor.i == 0:
        await context.enqueue_next_page(
            BatchPage(FakeDBCursor(cursor.i + page.page_size).to_json(), page.page_size)
        )
        print(f"Signaled the workflow {page}")
    print(f"Processing page {page}")
    # Sleep for a bit to simulate work
    await sleep(1)