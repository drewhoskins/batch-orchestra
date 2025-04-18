from __future__ import annotations

import json
import sys
from asyncio import sleep
from dataclasses import asdict, dataclass
from typing import Optional

from batch_orchestra.batch_processor import BatchPage, BatchProcessorContext, PageProcessor, page_processor

from .product_db import ProductDB


@dataclass
class ConfigArgs:
    db_file: str

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str) -> ConfigArgs:
        return ConfigArgs(**json.loads(json_str))


@dataclass
class ProductDBCursor:
    key: Optional[str]

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str) -> ProductDBCursor:
        return ProductDBCursor(**json.loads(json_str))


# Raises OperationalError if it can't acquire a lock, and then it will be retried.
# I decided to leave this kinda flaky to show off the resilience of the BatchOrchestrator.
@page_processor
class InflateProductPrices(PageProcessor):
    async def run(self, context: BatchProcessorContext):
        page = context.page
        if page.cursor_str == "":
            cursor = ProductDBCursor(key=None)
        else:
            cursor = ProductDBCursor.from_json(page.cursor_str)

        args = ConfigArgs.from_json(context.args_str)
        db_connection = ProductDB.get_db_connection(args.db_file)

        products = ProductDB.fetch_page(db_connection, cursor.key, page.size)

        if len(products) == page.size:
            # We got a full set of results, so there are likely more pages to process
            await context.enqueue_next_page(BatchPage(ProductDBCursor(products[-1].key).to_json(), page.size))

        num_processed = 0
        for product in products:
            # Note that this write is idempotent, so if we have to retry something that already succeeded,
            # we won't multiply by 1.04^2
            await ProductDB.inflate_price(db_connection, product, 1.04)
            await sleep(0.010)  # Simulate a network hop
            num_processed += 1
            # Allows the worker to context-switch, showing off parallelism when testing on systems with fewer cores.

        next_page_message = f"Next page cursor = {products[-1].key}" if products else "And that's all, folks!"
        context.logger.info(f"Finished processing {num_processed} rows of page {page}. {next_page_message}")
        sys.stdout.flush()

    @property
    def retry_mode(self) -> PageProcessor.RetryMode:
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE
