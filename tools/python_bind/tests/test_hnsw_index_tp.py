#!/usr/bin/env python3
# Copyright 2020 Alibaba Group Holding Limited.
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

import os

import pytest
from conftest import wait_for_server_ready

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
    reason="Set NEUG_RUN_EXTENSION_TESTS=1 to run vector-search tests.",
)


def _search(session, target):
    result = session.execute(
        "PROFILE MATCH (n:Item) "
        "RETURN n.id, n.name, vector_distance_l2(n.embedding, $target) AS score "
        "ORDER BY score ASC LIMIT 1;",
        access_mode="read",
        parameters={"target": target},
    )
    rows = list(result)
    operators = [
        operator["operator_name"]
        for operator in result.get_profile_metrics()["operators"]
    ]
    assert "IndexScanOpr" in operators
    return rows[0] if rows else None


def _check_search(failures, operation, row, expected):
    actual = row[:2] if row is not None else []
    if actual != expected:
        failures.append(f"{operation}: expected {expected}, got {actual}")


def test_hnsw_index_tp_mutations(tmp_path, unused_tcp_port):
    """Build in AP mode, then maintain and query the HNSW index in TP mode."""
    csv_path = tmp_path / "items.csv"
    csv_path.write_text(
        "id|name|embedding\n"
        "1|one|[1.0, 0.0, 0.0, 0.0]\n"
        "2|two|[0.0, 1.0, 0.0, 0.0]\n"
        "3|three|[0.0, 0.0, 1.0, 0.0]\n"
    )

    db = Database(str(tmp_path / "database"), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    try:
        conn.execute("LOAD vector_search;")
        conn.execute(
            "CREATE NODE TABLE Item("
            "id INT64 PRIMARY KEY, name STRING, embedding FLOAT[4]);"
        )
        conn.execute("CREATE REL TABLE Similar(FROM Item TO Item, weight DOUBLE);")
        conn.execute(
            f'COPY Item FROM (LOAD FROM "{csv_path}" '
            "RETURN id, name, CAST(embedding, 'FLOAT[4]') AS embedding);"
        )
        conn.execute(
            "CREATE INDEX item_embedding_hnsw ON Item USING HNSW (embedding) "
            "WITH (metric = 'l2', m = 16, ef_construction = 200);"
        )
    finally:
        conn.close()

    endpoint = db.serve(unused_tcp_port, "localhost", False)
    session = None
    try:
        wait_for_server_ready(endpoint)
        session = Session.open(endpoint, timeout="10s")

        failures = []

        _check_search(
            failures,
            "initial search",
            _search(session, [1.0, 0.0, 0.0, 0.0]),
            [1, "one"],
        )

        session.execute(
            "MATCH (src:Item {id: 2}), (dst:Item {id: 3}) "
            "CREATE (src)-[:Similar {weight: 0.5}]->(dst);",
            access_mode="insert",
        )
        result = session.execute(
            "MATCH (:Item {id: 2})-[e:Similar]->(:Item {id: 3}) " "RETURN e.weight;",
            access_mode="read",
        )
        assert list(result) == [[0.5]]

        create_indexed_item = (
            "CREATE (:Item {id: 4, name: 'four', " "embedding: [0.9, 0.0, 0.0, 0.0]});"
        )
        with pytest.raises(Exception, match="Insert-only mode"):
            session.execute(create_indexed_item, access_mode="insert")
        session.execute(create_indexed_item)
        _check_search(
            failures,
            "search after insert",
            _search(session, [0.9, 0.0, 0.0, 0.0]),
            [4, "four"],
        )

        session.execute(
            "MATCH (n:Item {id: 1}) DELETE n;",
            access_mode="update",
        )
        _check_search(
            failures,
            "search after delete",
            _search(session, [1.0, 0.0, 0.0, 0.0]),
            [4, "four"],
        )

        session.execute(
            "MATCH (n:Item {id: 2}) "
            "SET n.name = 'two-updated', "
            "n.embedding = [1.5, 0.0, 0.0, 0.0];",
            access_mode="update",
        )
        _check_search(
            failures,
            "search after update",
            _search(session, [1.5, 0.0, 0.0, 0.0]),
            [2, "two-updated"],
        )

        target = [2.0, 0.0, 0.0, 0.0]
        with pytest.raises(Exception):
            session.execute(
                "CREATE (:Item {id: 5, name: 'aborted', "
                "embedding: [2.0, 0.0, 0.0, 0.0]}), "
                "(:Item {id: 3, name: 'duplicate', "
                "embedding: [0.0, 0.0, 0.0, 0.0]});"
            )
        _check_search(
            failures,
            "search after aborted insert",
            _search(session, target),
            [2, "two-updated"],
        )

        with pytest.raises(Exception):
            session.execute(
                "MATCH (n:Item {id: 4}) DELETE n "
                "CREATE (:Item {id: 3, name: 'duplicate', "
                "embedding: [0.0, 0.0, 0.0, 0.0]});",
                access_mode="update",
            )
        _check_search(
            failures,
            "search after aborted delete",
            _search(session, [0.9, 0.0, 0.0, 0.0]),
            [4, "four"],
        )

        with pytest.raises(Exception):
            session.execute(
                "MATCH (n:Item {id: 2}) "
                "SET n.name = 'aborted-upsert', "
                "n.embedding = [2.5, 0.0, 0.0, 0.0] "
                "CREATE (:Item {id: 3, name: 'duplicate', "
                "embedding: [0.0, 0.0, 0.0, 0.0]});",
                access_mode="update",
            )
        _check_search(
            failures,
            "search after aborted upsert",
            _search(session, [1.5, 0.0, 0.0, 0.0]),
            [2, "two-updated"],
        )

        assert not failures, "\n".join(failures)
    finally:
        if session is not None:
            session.close()
        db.stop_serving()
        db.close()
