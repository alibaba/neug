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
import subprocess
import sys
import textwrap

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


def _run_cold_process(script, *args):
    env = os.environ.copy()
    tests_dir = os.path.dirname(__file__)
    env["PYTHONPATH"] = os.pathsep.join(
        path for path in (tests_dir, env.get("PYTHONPATH")) if path
    )
    subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script), *map(str, args)],
        check=True,
        env=env,
    )


def test_hnsw_create_index_wal_cold_restart(tmp_path, unused_tcp_port):
    """Replay CREATE INDEX before the extension is registered in the process."""
    db_path = tmp_path / "cold-restart-db"
    prepare = r"""
        import sys
        from neug.database import Database
        from neug.session import Session
        from conftest import wait_for_server_ready

        db = Database(sys.argv[1], mode="w", checkpoint_on_close=False)
        conn = db.connect()
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, embedding FLOAT[4]);")
        conn.execute("CREATE (:Item {id: 1, embedding: [1.0, 0.0, 0.0, 0.0]});")
        conn.close()
        endpoint = db.serve(int(sys.argv[2]), "localhost", False)
        wait_for_server_ready(endpoint)
        session = Session.open(endpoint, timeout="10s")
        session.execute("LOAD vector_search;")
        session.execute(
            "CREATE INDEX item_embedding_hnsw ON Item USING HNSW (embedding) "
            "WITH (metric = 'l2');"
        )
        # This mutation is replayed after CREATE INDEX while the recovered
        # index is still pending in the cold process.
        session.execute(
            "CREATE (:Item {id: 2, embedding: [0.0, 1.0, 0.0, 0.0]});"
        )
        session.close()
        db.stop_serving()
        db.close()
    """
    recover = r"""
        import sys
        from neug.database import Database
        from neug.session import Session
        from conftest import wait_for_server_ready

        # Database construction replays WAL before this fresh process has
        # loaded vector_search.
        db = Database(sys.argv[1], mode="w", checkpoint_on_close=False)
        conn = db.connect()
        conn.execute("LOAD vector_search;")
        conn.close()
        endpoint = db.serve(int(sys.argv[2]), "localhost", False)
        wait_for_server_ready(endpoint)
        session = Session.open(endpoint, timeout="10s")
        rows = list(session.execute("CALL SHOW_INDEXES() RETURN name;"))
        assert rows == [["item_embedding_hnsw"]], rows
        result = session.execute(
            "PROFILE MATCH (n:Item) "
            "RETURN n.id, vector_distance_l2(n.embedding, [0.0, 1.0, 0.0, 0.0]) "
            "AS score ORDER BY score LIMIT 1;",
            access_mode="read",
        )
        assert list(result)[0][0] == 2
        operators = [op["operator_name"] for op in result.get_profile_metrics()["operators"]]
        assert "IndexScanOpr" in operators, operators
        session.close()
        db.stop_serving()
        db.close()
    """

    _run_cold_process(prepare, db_path, unused_tcp_port)
    _run_cold_process(recover, db_path, unused_tcp_port)


def _search(session, target, expect_index=True):
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
    if expect_index:
        assert "IndexScanOpr" in operators
    else:
        assert "IndexScanOpr" not in operators
    return rows[0] if rows else None


def _check_search(failures, operation, row, expected):
    actual = row[:2] if row is not None else []
    if actual != expected:
        failures.append(f"{operation}: expected {expected}, got {actual}")


def _check_no_index_fallback(session):
    assert list(
        session.execute(
            "MATCH (n:Item) RETURN n.id, n.embedding ORDER BY n.id;",
            access_mode="read",
        )
    ) == [
        [1, [1.0, 0.0, 0.0, 0.0]],
        [2, [0.0, 1.0, 0.0, 0.0]],
    ]
    assert (
        list(
            session.execute(
                "MATCH (n:Item) "
                "RETURN n.id, vector_distance_l2("
                "n.embedding, [1.0, 0.0, 0.0, 0.0]) AS score "
                "ORDER BY score ASC LIMIT 1;",
                access_mode="read",
            )
        )[0][0]
        == 1
    )


def test_hnsw_index_tp_mutations(tmp_path, unused_tcp_port):
    """Create, maintain, query, and drop an HNSW index in TP mode."""
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
        conn.execute(
            "CREATE NODE TABLE Item("
            "id INT64 PRIMARY KEY, name STRING, embedding FLOAT[4]);"
        )
        conn.execute("CREATE REL TABLE Similar(FROM Item TO Item, weight DOUBLE);")
        conn.execute(
            f'COPY Item FROM (LOAD FROM "{csv_path}" '
            "RETURN id, name, CAST(embedding, 'FLOAT[4]') AS embedding);"
        )
    finally:
        conn.close()

    endpoint = db.serve(unused_tcp_port, "localhost", False)
    session = None
    try:
        wait_for_server_ready(endpoint)
        session = Session.open(endpoint, timeout="10s")

        session.execute("LOAD vector_search;")
        session.execute(
            "CREATE INDEX item_embedding_hnsw ON Item USING HNSW (embedding) "
            "WITH (metric = 'l2', m = 16, ef_construction = 200);"
        )

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

        session.execute("DROP INDEX item_embedding_hnsw;")
        assert list(session.execute("CALL SHOW_INDEXES() RETURN name;")) == []
    finally:
        if session is not None:
            session.close()
        db.stop_serving()
        db.close()


def test_hnsw_index_tp_ddl_recovery_after_reopen(tmp_path, unused_tcp_port):
    """Recover TP create/drop index commits after closing without a checkpoint."""
    db_path = tmp_path / "database"
    db = Database(str(db_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE Item("
            "id INT64 PRIMARY KEY, name STRING, embedding FLOAT[4]);"
        )
        conn.execute(
            "CREATE (:Item {id: 1, name: 'one', "
            "embedding: [1.0, 0.0, 0.0, 0.0]}), "
            "(:Item {id: 2, name: 'two', "
            "embedding: [0.0, 1.0, 0.0, 0.0]});"
        )
    finally:
        conn.close()

    session = None
    try:
        endpoint = db.serve(unused_tcp_port, "localhost", False)
        wait_for_server_ready(endpoint)
        session = Session.open(endpoint, timeout="10s")
        session.execute("LOAD vector_search;")
        session.execute(
            "CREATE INDEX item_embedding_hnsw ON Item USING HNSW (embedding) "
            "WITH (metric = 'l2');"
        )
    finally:
        if session is not None:
            session.close()
        db.stop_serving()
        db.close()

    db = Database(str(db_path), mode="w", checkpoint_on_close=False)
    session = None
    try:
        endpoint = db.serve(unused_tcp_port, "localhost", False)
        wait_for_server_ready(endpoint)
        session = Session.open(endpoint, timeout="10s")
        session.execute("LOAD vector_search;")
        assert list(session.execute("CALL SHOW_INDEXES() RETURN name;")) == [
            ["item_embedding_hnsw"]
        ]
        assert _search(session, [1.0, 0.0, 0.0, 0.0])[:2] == [1, "one"]
        session.execute("DROP INDEX item_embedding_hnsw;")
        assert list(session.execute("CALL SHOW_INDEXES() RETURN name;")) == []
        _check_no_index_fallback(session)
    finally:
        if session is not None:
            session.close()
        db.stop_serving()
        db.close()

    db = Database(str(db_path), mode="w", checkpoint_on_close=False)
    session = None
    try:
        endpoint = db.serve(unused_tcp_port, "localhost", False)
        wait_for_server_ready(endpoint)
        session = Session.open(endpoint, timeout="10s")
        session.execute("LOAD vector_search;")
        assert list(session.execute("CALL SHOW_INDEXES() RETURN name;")) == []
        _check_no_index_fallback(session)
    finally:
        if session is not None:
            session.close()
        db.stop_serving()
        db.close()
