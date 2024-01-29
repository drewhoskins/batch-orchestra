from __future__ import annotations
from asyncio import sleep
from dataclasses import asdict, dataclass
import json
from typing import Optional
from batch_processor import BatchProcessorContext, BatchPage, page_processor

import sqlite3

from product_db import ProductDB

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


@page_processor
async def inflate_product_prices(context: BatchProcessorContext):
    page = context.get_page()
    if page.cursor_str == "":
        cursor = ProductDBCursor(key=None)
    else:
        cursor = ProductDBCursor.from_json(page.cursor_str)

    args = ConfigArgs.from_json(context.get_args())
    db_connection = ProductDB.get_db_connection(args.db_file)
    
    products = ProductDB.fetch_page(db_connection, cursor.key, page.page_size)

    if len(products) == page.page_size:
        # We got a full set of results, so there are likely more pages to process
        await context.enqueue_next_page(
            BatchPage(ProductDBCursor(products[-1].key).to_json(), page.page_size)
        )
    print(f"Processing page {page}")
    for product in products:
        # Note that this write is idempotent, so if we have to retry something that already succeeded,
        # we won't multiply by 1.04^2
        ProductDB.inflate_price(db_connection, product, 1.04)
    print(f"Finished processing {page.page_size} rows of page {page}")
    
