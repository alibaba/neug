#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import sys

from neug.async_connection import AsyncConnection
from neug.database import Database

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def create_graph(conn: AsyncConnection, data_dir: str):
    person_csv = os.path.join(data_dir, "person.csv")
    person_knows_person_csv = os.path.join(data_dir, "person_knows_person.csv")

    # First create the graph schema, should be sequential.
    await conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    await conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")

    # Then load data.
    await conn.execute(f'COPY person from "{person_csv}"')
    await conn.execute(
        f'COPY knows from "{person_knows_person_csv}" (from="person", to="person")'
    )


async def execute_query(conn: AsyncConnection):
    res = await conn.execute(
        "MATCH (p: person)-[e:knows*1..4]->(q: person) return count(q);"
    )
    for record in res:
        logger.info(f"Query result: {record}")


async def main(data_dir: str, db_dir: str):
    logger.info(f"Loading data from {data_dir} into database {db_dir}")

    db = Database(db_dir, "w")
    conn = db.async_connect()

    logger.info("Creating graph schema and loading data...")
    await create_graph(conn, data_dir)

    logger.info("Executing query...")
    await execute_query(conn)


if __name__ == "__main__":
    # expect 2 args, csv_data_dir and db_dir
    if len(sys.argv) != 3:
        logger.error("Usage: python simple_async_example.py <csv_data_dir> <db_dir>")
        sys.exit(1)
    data_dir = sys.argv[1]
    db_dir = sys.argv[2]

    import asyncio

    asyncio.run(main(data_dir, db_dir))
