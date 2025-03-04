import sqlite3
from typing import Optional
import uuid
from wonderwords import RandomWord
import random


class Product:
    def __init__(self, key: str, name: str, price: float, did_inflate_migration: int):
        self.key = key
        self.name = name
        self.price = price
        self.did_inflate_migration: bool = did_inflate_migration == 1

    def __str__(self):
        return f"Product(key={self.key}, name={self.name}, price={self.price}, did_inflate_migration={self.did_inflate_migration})"


class ProductDB:
    @staticmethod
    def create_product():
        return (
            str(uuid.uuid4()),
            f"{RandomWord().word(include_parts_of_speech=['adjectives'])} {RandomWord().word(include_parts_of_speech=['nouns'])}",
            random.uniform(1, 100),
            0,
        )

    @staticmethod
    def get_db_connection(filename):
        return sqlite3.connect(filename)

    @staticmethod
    def populate_table(conn: sqlite3.Connection, *, num_records: int):
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS my_products")
        cursor.execute("""
    CREATE TABLE my_products (
        key TEXT PRIMARY KEY, 
        name TEXT, 
        price REAL,
        did_inflate_migration INTEGER)
        """)

        # Fill up the table with some pricing data
        products = [ProductDB.create_product() for _ in range(num_records)]
        cursor.executemany("INSERT INTO my_products VALUES (?,?,?,?)", products)
        conn.commit()

    @staticmethod
    def fetch_page(conn: sqlite3.Connection, offset: Optional[str], page_size: int):
        cursor = conn.cursor()
        where_clause = f"WHERE key > '{offset}'" if offset else ""
        cursor.execute(f"SELECT * FROM my_products {where_clause} ORDER BY key LIMIT {page_size}")
        result_rows = cursor.fetchall()
        return [Product(*row) for row in result_rows]

    @staticmethod
    async def inflate_price(conn: sqlite3.Connection, product: Product, factor: float):
        cursor = conn.cursor()
        updated_price = product.price * factor
        # Idempotently update the price
        query = f"UPDATE my_products SET price = {updated_price}, did_inflate_migration = 1 WHERE key = '{product.key}' AND did_inflate_migration = 0"
        cursor.execute(query)
        conn.commit()

    @staticmethod
    def for_each_product(db_connection):
        cursor = ""
        i = 0
        while True:
            products = ProductDB.fetch_page(db_connection, cursor, 100)
            if len(products) == 0:
                break
            for product in products:
                yield i, product
                i += 1
            cursor = products[-1].key
