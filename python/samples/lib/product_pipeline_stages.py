from __future__ import annotations

import json
import sys
from asyncio import sleep
from dataclasses import asdict, dataclass
from typing import List

from batch_orchestra.batch_processor import BatchPage, BatchProcessorContext, PageProcessor, page_processor

from .inflate_product_prices_page_processor import ConfigArgs, ProductDBCursor
from .product_db import ProductDB

#
# A three-stage pipeline over the same products table as inflate_product_prices_page_processor.py, to show how a
# page of work can flow through several activities:
#
#   FetchProductPage  ->  InflateProductPricesStage  ->  AuditProductPricesStage
#   (paginates, reads)    (writes, gently)               (verifies)
#
# Each stage:
#   * is an ordinary @page_processor,
#   * returns a small json payload that the next stage reads with context.previous_stage_result,
#   * gets its own parallelism budget (see perform_multi_stage_pipeline.py), because the read stage can hammer the
#     database harder than the write stage should.
#
# Only the first stage calls enqueue_next_page(); the later stages just process the page they're handed.
#


@dataclass
class ProductKeys:
    keys: List[str]

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str: str) -> ProductKeys:
        return ProductKeys(**json.loads(json_str))


@dataclass
class InflationReceipt:
    keys: List[str]
    num_inflated: int

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str: str) -> InflationReceipt:
        return InflationReceipt(**json.loads(json_str))


# Stage 0: paginate through the table and decide which products this page covers.
@page_processor
class FetchProductPage(PageProcessor):
    async def run(self, context: BatchProcessorContext) -> str:
        page = context.page
        cursor = ProductDBCursor(key=None) if page.cursor_str == "" else ProductDBCursor.from_json(page.cursor_str)
        args = ConfigArgs.from_json(context.args_str)
        db_connection = ProductDB.get_db_connection(args.db_file)

        products = ProductDB.fetch_page(db_connection, cursor.key, page.size)

        if len(products) == page.size:
            # We got a full set of results, so there are likely more pages to process.
            await context.enqueue_next_page(BatchPage(ProductDBCursor(products[-1].key).to_json(), page.size))

        context.logger.info(f"Fetched {len(products)} products for page {page}.")
        # Only the keys travel to the next stage: results are stored in the workflow's history, so keep them small.
        return ProductKeys(keys=[product.key for product in products]).to_json()

    @property
    def retry_mode(self) -> PageProcessor.RetryMode:
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


# Stage 1: do the actual (idempotent) write for the products the first stage found.
@page_processor
class InflateProductPricesStage(PageProcessor):
    async def run(self, context: BatchProcessorContext) -> str:
        keys = ProductKeys.from_json(context.previous_stage_result).keys
        args = ConfigArgs.from_json(context.args_str)
        db_connection = ProductDB.get_db_connection(args.db_file)

        num_inflated = 0
        for product in ProductDB.fetch_by_keys(db_connection, keys):
            # Note that this write is idempotent, so if we have to retry something that already succeeded,
            # we won't multiply by 1.04^2
            await ProductDB.inflate_price(db_connection, product, 1.04)
            await sleep(0.010)  # Simulate a network hop
            num_inflated += 1

        context.logger.info(f"Inflated {num_inflated} of {len(keys)} products on page {context.page}.")
        sys.stdout.flush()
        return InflationReceipt(keys=keys, num_inflated=num_inflated).to_json()

    @property
    def retry_mode(self) -> PageProcessor.RetryMode:
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE


# Stage 2: read the page back and confirm the migration took, per page, as the batch runs.
@page_processor
class AuditProductPricesStage(PageProcessor):
    async def run(self, context: BatchProcessorContext) -> str:
        receipt = InflationReceipt.from_json(context.previous_stage_result)
        args = ConfigArgs.from_json(context.args_str)
        db_connection = ProductDB.get_db_connection(args.db_file)

        unmigrated = [
            product.key
            for product in ProductDB.fetch_by_keys(db_connection, receipt.keys)
            if not product.did_inflate_migration
        ]

        if unmigrated:
            # A page processor that raises gets retried, and then moves to extended retries, just like any other.
            raise Exception(f"Audit found {len(unmigrated)} unmigrated products on page {context.page}.")

        context.logger.info(f"Audited {len(receipt.keys)} products on page {context.page}: all migrated.")
        return json.dumps({"num_audited": len(receipt.keys)})

    @property
    def retry_mode(self) -> PageProcessor.RetryMode:
        return PageProcessor.RetryMode.EXECUTE_AT_LEAST_ONCE
