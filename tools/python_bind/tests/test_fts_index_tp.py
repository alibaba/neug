#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import socket
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from neug.database import Database
from neug.session import Session

pytestmark = pytest.mark.skipif(
    os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower()
    not in ("1", "true", "yes", "on"),
    reason="Set NEUG_RUN_EXTENSION_TESTS=1 to run FTS extension tests.",
)


@pytest.fixture()
def unused_tcp_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def search(session, query, limit=1000):
    return list(
        session.execute(
            "MATCH (n:Item) "
            f"RETURN n.id, bm25(n.text, '{query}') AS score "
            f"ORDER BY score ASC LIMIT {limit};"
        )
    )


@pytest.fixture()
def fts_service(tmp_path, unused_tcp_port, monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    db = Database(db_path=str(tmp_path / "fts_tp_db"), mode="w")
    connection = db.connect()
    try:
        connection.execute("LOAD fts;")
    except RuntimeError as error:
        connection.close()
        db.close()
        pytest.skip(f"FTS extension not available: {error}")
    connection.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, text STRING);")
    connection.execute("CREATE (:Item {id: 0, text: 'shared token'});")
    connection.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
    connection.close()
    endpoint = db.serve(
        port=unused_tcp_port,
        host="127.0.0.1",
        blocking=False,
        thread_num=2,
    )
    try:
        yield db, endpoint
    finally:
        db.stop_serving()
        db.close()


@pytest.fixture()
def fts_multi_column_service(tmp_path, unused_tcp_port, monkeypatch):
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    db = Database(db_path=str(tmp_path / "fts_multi_column_tp_db"), mode="w")
    connection = db.connect()
    try:
        connection.execute("LOAD fts;")
    except RuntimeError as error:
        connection.close()
        db.close()
        pytest.skip(f"FTS extension not available: {error}")
    connection.execute(
        "CREATE NODE TABLE Article("
        "id INT64 PRIMARY KEY, title STRING, description STRING, notes STRING);"
    )
    connection.execute(
        "CREATE (:Article {id: 1, title: 'target target', "
        "description: 'other', notes: 'mismatch'}), "
        "(:Article {id: 2, title: 'other', "
        "description: 'target target', notes: 'mismatch'}), "
        "(:Article {id: 3, title: 'other', "
        "description: 'other', notes: 'target'});"
    )
    connection.execute(
        "CREATE INDEX article_text_fts ON Article USING FTS (title, description);"
    )
    connection.execute(
        "CREATE NODE TABLE SingleArticle("
        "id INT64 PRIMARY KEY, title STRING, description STRING);"
    )
    connection.execute(
        "CREATE (:SingleArticle {id: 1, title: 'target', " "description: 'target'});"
    )
    connection.execute(
        "CREATE INDEX single_article_title_fts ON SingleArticle USING FTS (title);"
    )
    connection.close()
    endpoint = db.serve(
        port=unused_tcp_port,
        host="127.0.0.1",
        blocking=False,
        thread_num=2,
    )
    try:
        yield db, endpoint
    finally:
        db.stop_serving()
        db.close()


def test_fts_tp_multi_column_bm25_constant_and_dynamic_arguments(
    fts_multi_column_service,
):
    _, endpoint = fts_multi_column_service
    session = Session.open(endpoint)
    try:
        single_rows = session.execute(
            "MATCH (a:Article) "
            "RETURN a.id, bm25(a.title, $target) AS score "
            "ORDER BY score ASC;",
            parameters={"target": "target"},
        )
        assert [row[0] for row in single_rows] == [1]

        cases = [
            ("[10.0, 1.0]", "'target'", {}),
            ("$weights", "'target'", {"weights": [10.0, 1.0]}),
            ("[10.0, 1.0]", "$target", {"target": "target"}),
            (
                "$weights",
                "$target",
                {"weights": [10.0, 1.0], "target": "target"},
            ),
        ]
        for weights, target, parameters in cases:
            rows = session.execute(
                "MATCH (a:Article) RETURN a.id, "
                f"bm25([a.title, a.description], {weights}, {target}) AS score "
                "ORDER BY score ASC;",
                parameters=parameters,
            )
            assert [row[0] for row in rows] == [1, 2]

        with pytest.raises(Exception):
            session.execute(
                "MATCH (a:Article) " "RETURN bm25(a.notes, 'target') AS score;"
            )

        with pytest.raises(Exception):
            session.execute(
                "MATCH (a:SingleArticle) "
                "RETURN bm25([a.title, a.description], [1.0, 1.0], "
                "'target') AS score;"
            )
    finally:
        session.close()


def test_fts_tp_tracks_insert_update_delete_and_failed_transaction(fts_service):
    _, endpoint = fts_service
    session = Session.open(endpoint)
    try:
        session.execute(
            "CREATE (:Item {id: 1, text: 'inserted token'});", access_mode="update"
        )
        assert [row[0] for row in search(session, "inserted")] == [1]
        session.execute(
            "MATCH (n:Item) WHERE n.id = 1 SET n.text = 'updated token';",
            access_mode="update",
        )
        assert search(session, "inserted") == []
        assert [row[0] for row in search(session, "updated")] == [1]

        with pytest.raises(Exception):
            session.execute(
                "CREATE (:Item {id: 2, text: 'rolledback token'}), "
                "(:Item {id: 0, text: 'duplicate key'});",
                access_mode="update",
            )
        assert search(session, "rolledback") == []

        with pytest.raises(Exception):
            session.execute(
                "MATCH (n:Item) WHERE n.id = 1 "
                "SET n.text = 'aborted update' "
                "CREATE (:Item {id: 0, text: 'duplicate key'});",
                access_mode="update",
            )
        assert search(session, "aborted") == []
        assert [row[0] for row in search(session, "updated")] == [1]

        with pytest.raises(Exception):
            session.execute(
                "MATCH (n:Item) WHERE n.id = 1 DELETE n "
                "CREATE (:Item {id: 0, text: 'duplicate key'});",
                access_mode="update",
            )
        assert [row[0] for row in search(session, "updated")] == [1]

        session.execute("MATCH (n:Item) WHERE n.id = 1 DELETE n;", access_mode="update")
        assert search(session, "updated") == []
    finally:
        session.close()


def test_fts_tp_concurrent_readers_see_atomic_index_visibility(fts_service):
    _, endpoint = fts_service
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
        assert (
            len(search(reader, "shared", limit=inserted_count + 1))
            == inserted_count + 1
        )
    finally:
        writer.close()
        reader.close()
