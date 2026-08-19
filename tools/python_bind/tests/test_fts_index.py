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

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from neug.database import Database
from neug.session import Session

EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
pytestmark = pytest.mark.skipif(
    not EXTENSION_TESTS_ENABLED,
    reason="Set NEUG_RUN_EXTENSION_TESTS=1 to run FTS extension tests.",
)


def load_fts(connection, skip_if_unavailable=False):
    try:
        connection.execute("LOAD fts;")
    except RuntimeError as error:
        if skip_if_unavailable:
            pytest.skip(f"FTS extension not available: {error}")
        raise


def create_item_table(connection):
    connection.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, text STRING);")


def search(connection, query, limit=10):
    return list(
        connection.execute(
            "MATCH (n:Item) "
            f"RETURN n.id, bm25(n.text, '{query}') AS score "
            f"ORDER BY score ASC LIMIT {limit};"
        )
    )


@pytest.fixture()
def fts_database(tmp_path):
    db = Database(db_path=str(tmp_path / "fts_db"), mode="w")
    connection = db.connect()
    load_fts(connection, skip_if_unavailable=True)
    create_item_table(connection)
    connection.execute(
        "CREATE (:Item {id: 1, text: 'search text alpha'}), "
        "(:Item {id: 2, text: 'search text beta'}), "
        "(:Item {id: 3, text: 'gamma'});"
    )
    connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
    try:
        yield connection
    finally:
        connection.close()
        db.close()


def test_fts_topk_search(fts_database):
    rows = search(fts_database, "search text", limit=2)
    assert [row[0] for row in rows] == [1, 2]
    assert all(row[1] <= 0.0 for row in rows)


def test_fts_tracks_inserts_updates_and_deletes(fts_database):
    fts_database.execute("CREATE (:Item {id: 4, text: 'durable fox'});")
    assert [row[0] for row in search(fts_database, "durable")] == [4]

    fts_database.execute("MATCH (n:Item) WHERE n.id = 4 SET n.text = 'current fox';")
    assert search(fts_database, "durable") == []
    assert [row[0] for row in search(fts_database, "current")] == [4]

    fts_database.execute("MATCH (n:Item) WHERE n.id = 4 DELETE n;")
    assert search(fts_database, "current") == []


def test_fts_index_survives_database_reopen(tmp_path):
    database_path = str(tmp_path / "persistent_fts_db")

    db = Database(db_path=database_path, mode="w")
    connection = db.connect()
    load_fts(connection, skip_if_unavailable=True)
    create_item_table(connection)
    connection.execute("CREATE (:Item {id: 1, text: 'durable fox'});")
    connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
    connection.close()
    db.close()

    reopened_db = Database(db_path=database_path, mode="w")
    reopened_connection = reopened_db.connect()
    try:
        load_fts(reopened_connection)
        assert [row[0] for row in search(reopened_connection, "durable")] == [1]

        reopened_connection.execute("CREATE (:Item {id: 2, text: 'durable hare'});")
        assert {row[0] for row in search(reopened_connection, "durable")} == {
            1,
            2,
        }
    finally:
        reopened_connection.close()
        reopened_db.close()


def test_explicit_checkpoint_discards_later_uncheckpointed_fts_data(tmp_path):
    database_path = str(tmp_path / "explicit_checkpoint_fts_db")
    db = Database(db_path=database_path, mode="w", checkpoint_on_close=False)
    connection = db.connect()
    load_fts(connection, skip_if_unavailable=True)
    create_item_table(connection)
    connection.execute("CREATE (:Item {id: 1, text: 'durable token'});")
    connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
    connection.execute("CHECKPOINT;")

    connection.execute("CREATE (:Item {id: 2, text: 'volatile token'});")
    assert [row[0] for row in search(connection, "volatile")] == [2]
    connection.close()
    db.close()

    reopened_db = Database(db_path=database_path, mode="w", checkpoint_on_close=False)
    reopened_connection = reopened_db.connect()
    try:
        load_fts(reopened_connection)
        rows = list(
            reopened_connection.execute("MATCH (n:Item) RETURN n.id ORDER BY n.id;")
        )
        assert rows == [[1]]
        assert [row[0] for row in search(reopened_connection, "durable")] == [1]
        assert search(reopened_connection, "volatile") == []
    finally:
        reopened_connection.close()
        reopened_db.close()


def test_fts_dynamic_query_parameter_is_bound_per_execution(fts_database):
    statement = (
        "MATCH (n:Item) RETURN n.id, bm25(n.text, $query) AS score "
        "ORDER BY score ASC;"
    )
    assert {
        row[0]
        for row in fts_database.execute(statement, parameters={"query": "search"})
    } == {1, 2}
    assert [
        row[0] for row in fts_database.execute(statement, parameters={"query": "gamma"})
    ] == [3]


def test_fts_dynamic_query_parameter_rejects_invalid_values(fts_database):
    statement = (
        "MATCH (n:Item) RETURN n.id, bm25(n.text, $query) AS score "
        "ORDER BY score ASC;"
    )
    with pytest.raises(Exception):
        list(fts_database.execute(statement, parameters={}))
    with pytest.raises(Exception):
        list(fts_database.execute(statement, parameters={"query": None}))


def test_fts_read_write_transactions_are_isolated(tmp_path):
    db = Database(db_path=str(tmp_path / "fts_isolation_db"), mode="w")
    connection = db.connect()
    load_fts(connection, skip_if_unavailable=True)
    create_item_table(connection)
    connection.execute("CREATE (:Item {id: 0, text: 'shared token'});")
    connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
    connection.close()

    endpoint = db.serve(port=0, host="localhost", blocking=False, thread_num=2)
    writer = Session.open(endpoint)
    reader = Session.open(endpoint)

    inserted_count = 100
    create_query = "CREATE " + ", ".join(
        f"(:Item {{id: {item_id}, text: 'shared token'}})"
        for item_id in range(1, inserted_count + 1)
    )
    create_query += ";"
    barrier = Barrier(2)

    def write_transaction():
        barrier.wait()
        writer.execute(create_query, access_mode="update")

    def read_transaction():
        barrier.wait()
        return len(search(reader, "shared", limit=inserted_count + 1))

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            write_future = executor.submit(write_transaction)
            read_future = executor.submit(read_transaction)
            write_future.result()
            concurrent_count = read_future.result()

        assert concurrent_count in (1, inserted_count + 1)
        assert len(search(reader, "shared", limit=inserted_count + 1)) == (
            inserted_count + 1
        )
    finally:
        writer.close()
        reader.close()
        db.stop_serving()
        db.close()


@pytest.mark.parametrize("query", ["", "   ", 'unterminated"', "quick.brown"])
def test_invalid_fts_query_raises_runtime_error(fts_database, query):
    with pytest.raises(RuntimeError, match="FTS query failed"):
        search(fts_database, query)
