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

import json
import os
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from conftest import cypher_path
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


def test_local_buffer_tp_concurrent_writes_and_wal_replay(tmp_path, unused_tcp_port):
    """An existing HNSW checkpoint plus WAL-only tail survives cold activation."""
    from test_hnsw_index import _local_buffer_checkpoint
    from test_hnsw_index import _profile_operator_names

    path = tmp_path / "local-buffer-tp"
    db = Database(str(path), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    try:
        conn.execute("LOAD vector_search;")
        conn.execute(
            "CREATE NODE TABLE Item(id INT64 PRIMARY KEY, name STRING, embedding FLOAT[4]);"
        )
        conn.execute(
            "CREATE (:Item {id: 0, name: 'base', embedding: [0.0,0.0,0.0,0.0]});"
        )
        # Build more than 1000 HNSW nodes to exceed ZVec's default brute-force
        # threshold. Distant vectors leave the snapshot's nearest two unchanged.
        conn.begin_transaction()
        for i in range(1000):
            conn.execute(
                f"CREATE (:Item {{id: {-i - 2}, name: 'base', "
                f"embedding: [{float(100 + i)},0.0,0.0,0.0]}});"
            )
        conn.commit()
        assert list(conn.execute("MATCH (n:Item) RETURN count(n);")) == [[1001]]
        conn.execute("CREATE INDEX item_hnsw ON Item USING HNSW (embedding);")
        # Persist a nonempty Flat tail, then recover additional WAL-only IDs.
        conn.execute(
            "CREATE (:Item {id: -1, name: 'checkpoint-tail', embedding: [30.0,0.0,0.0,0.0]});"
        )
        conn.execute("CHECKPOINT;")
    finally:
        conn.close()
    before = _local_buffer_checkpoint(path, "item_hnsw")
    assert before[0] == 1001
    endpoint = db.serve(unused_tcp_port, "localhost", False)
    wait_for_server_ready(endpoint)

    phase = Barrier(3, timeout=20)

    def search_rows(reader, value):
        result = reader.execute(
            "PROFILE MATCH (n:Item) RETURN n.id, vector_distance_l2(n.embedding, "
            f"[{float(value)},0.0,0.0,0.0]) AS score ORDER BY score LIMIT 2;",
            access_mode="read",
        )
        rows = list(result)
        assert "IndexScanOpr" in _profile_operator_names(result)
        return rows

    def read_repeatedly():
        reader = Session.open(endpoint, timeout="30s")
        try:
            reader.begin_transaction(read_only=True)
            assert search_rows(reader, 1) == [[0, 1.0], [-1, 841.0]]
            # Both readers pin their snapshots before the first write.
            phase.wait()
            for i in range(1, 15):
                phase.wait()  # The writer has committed this exact vector.
                assert search_rows(reader, i) == [
                    [0, float(i * i)],
                    [-1, float((30 - i) ** 2)],
                ]
                phase.wait()
            reader.rollback()
            # Releasing the old snapshot makes the committed tail visible.
            assert search_rows(reader, 14)[0] == [14, 0.0]
        except BaseException:
            phase.abort()
            raise
        finally:
            reader.close()

    session = Session.open(endpoint, timeout="30s")
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            readers = [pool.submit(read_repeatedly) for _ in range(2)]
            try:
                phase.wait()
                for i in range(1, 15):
                    session.execute(
                        f"CREATE (:Item {{id: {i}, name: 'tail', "
                        f"embedding: [{float(i)},0.0,0.0,0.0]}});"
                    )
                    # A fresh read sees the exact match; pinned readers must not.
                    assert search_rows(session, i)[0] == [i, 0.0]
                    phase.wait()
                    phase.wait()
            except BaseException:
                phase.abort()
                raise
            for reader in readers:
                reader.result()
        session.execute(
            "MERGE (n:Item {id: 15}) SET n.name='merged', n.embedding=[15.0,0.0,0.0,0.0];"
        )
        session.execute("MATCH (n:Item {id: 1}) SET n.embedding=[20.0,0.0,0.0,0.0];")
        session.execute("MATCH (n:Item {id: 2}) DELETE n;")
        assert _search(session, [20.0, 0.0, 0.0, 0.0])[:2] == [1, "tail"]
        assert _search(session, [15.0, 0.0, 0.0, 0.0])[:2] == [15, "merged"]
    finally:
        session.close()
        db.stop_serving()
        db.close()
    # No checkpoint during TP writes: the graph file and F are unchanged.
    assert _local_buffer_checkpoint(path, "item_hnsw") == before
    _run_cold_process(
        r"""
        import sys
        from pathlib import Path
        from neug import Database
        from test_hnsw_index import _local_buffer_checkpoint
        path = Path(sys.argv[1])
        db = Database(str(path), mode="w", checkpoint_on_close=False)
        conn = db.connect()
        conn.execute("LOAD vector_search;")
        for value, expected in ((20.0, 1), (15.0, 15), (0.0, 0), (30.0, -1)):
            result = conn.execute(
                "PROFILE MATCH (n:Item) RETURN n.id, "
                f"vector_distance_l2(n.embedding, [{value},0.0,0.0,0.0]) AS score "
                "ORDER BY score LIMIT 1;"
            )
            assert list(result) == [[expected, 0.0]]
            assert "IndexScanOpr" in [op["operator_name"] for op in result.get_profile_metrics()["operators"]]
        assert list(conn.execute("MATCH (n:Item {id: 2}) RETURN n.id;")) == []
        conn.execute("CHECKPOINT;")
        assert _local_buffer_checkpoint(path, "item_hnsw")[0] == 1001
        conn.close()
        db.close()
    """,
        path,
    )


@pytest.mark.parametrize("preload_extension", [False, True])
def test_index_optimize_tp_snapshot_and_wal(
    tmp_path, unused_tcp_port, preload_extension
):
    path = tmp_path / "optimize-tp"
    db = Database(str(path), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    try:
        conn.execute("LOAD vector_search;")
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[4]);")
        conn.execute("CREATE INDEX item_hnsw ON Item USING HNSW (v);")
        conn.execute("CHECKPOINT;")
        for i in range(2):
            conn.execute(f"CREATE (:Item {{id: {i}, v: [{float(i)},0.0,0.0,0.0]}});")
    finally:
        conn.close()
    endpoint = db.serve(unused_tcp_port, "localhost", False)
    wait_for_server_ready(endpoint)
    reader = Session.open(endpoint)
    writer = Session.open(endpoint)
    query = (
        "MATCH (n:Item) RETURN n.id, vector_distance_l2(n.v, [2.0,0.0,0.0,0.0]) "
        "AS score ORDER BY score LIMIT 5;"
    )
    try:
        reader.begin_transaction(read_only=True)
        assert list(reader.execute(query, access_mode="read")) == [[1, 1.0], [0, 4.0]]
        writer.execute("CREATE (:Item {id: 2, v: [2.0,0.0,0.0,0.0]});")
        with pytest.raises(Exception):
            writer.execute("CALL optimize_indexes();", access_mode="read")
        rows = list(
            writer.execute(
                "CALL optimize_indexes(type := $type);", parameters={"type": "hnsw"}
            )
        )
        assert rows[0][:4] == ["hnsw", "item_hnsw", 3, 0]
        assert list(writer.execute("CALL optimize_indexes();"))[0][2:4] == [0, 0]
        assert list(reader.execute(query, access_mode="read")) == [[1, 1.0], [0, 4.0]]
        reader.rollback()
        assert list(reader.execute(query, access_mode="read"))[0] == [2, 0.0]
    finally:
        reader.close()
        writer.close()
        db.stop_serving()
        db.close()
    _run_cold_process(
        r"""
        import sys
        from pathlib import Path
        from neug import Database
        path = Path(sys.argv[1])
        preload = sys.argv[2] == "True"
        if preload:
            bootstrap = Database(str(path.parent / "bootstrap"), mode="w", checkpoint_on_close=False)
            c = bootstrap.connect()
            c.execute("LOAD vector_search;")
            c.close()
            bootstrap.close()
        db = Database(str(path), mode="w", checkpoint_on_close=False)
        conn = db.connect()
        conn.execute("LOAD vector_search;")
        assert list(conn.execute("MATCH (n:Item) RETURN count(n);")) == [[3]]
        result = conn.execute(
            "PROFILE MATCH (n:Item) RETURN n.id, vector_distance_l2(n.v, [2.0,0.0,0.0,0.0]) "
            "AS score ORDER BY score LIMIT 5;"
        )
        assert list(result) == [[2, 0.0], [1, 1.0], [0, 4.0]]
        assert "IndexScanOpr" in [op["operator_name"] for op in result.get_profile_metrics()["operators"]]
        rows = list(conn.execute("CALL optimize_indexes();"))
        # Available indexes replay maintenance; unavailable extensions retain
        # their existing pending activation behavior and can be optimized later.
        assert rows[0][2:4] == ([0, 0] if preload else [3, 0]), rows
        conn.close()
        db.close()
        """,
        path,
        preload_extension,
    )


@pytest.mark.parametrize(
    "metric,legacy", [("cosine", False), ("l2", False), ("l2", True)]
)
def test_hnsw_pending_final_vectors_cold_restart(
    tmp_path, unused_tcp_port, metric, legacy
):
    """Deferred activation restores final slots and the checkpoint-time boundary."""
    path = tmp_path / "pending-final-vectors"
    _run_cold_process(
        r"""
        import sys
        from neug import Database
        from neug.session import Session
        from conftest import wait_for_server_ready

        db = Database(sys.argv[1], mode="w", checkpoint_on_close=False)
        conn = db.connect()
        conn.execute("LOAD vector_search;")
        conn.execute(
            "CREATE NODE TABLE Item(id INT64 PRIMARY KEY, name STRING, embedding FLOAT[4]);"
        )
        for id, vector in [(0, [1.,0.,0.,0.]), (1, [0.,1.,0.,0.]),
                           (2, [0.,0.,1.,0.]), (9, [0.5,0.5,0.5,0.5])]:
            conn.execute(f"CREATE (:Item {{id: {id}, name: 'base', embedding: {vector}}});")
        conn.execute(
            "CREATE INDEX item_hnsw ON Item USING HNSW (embedding) "
            f"WITH (metric='{sys.argv[3]}', cosine_normalize=false);"
        )
        conn.execute(
            "CREATE INDEX item_ip ON Item USING HNSW (embedding) "
            "WITH (metric='ip', cosine_normalize=false);"
        )
        conn.execute("CHECKPOINT;")
        conn.close()
        endpoint = db.serve(int(sys.argv[2]), "localhost", False)
        wait_for_server_ready(endpoint)
        session = Session.open(endpoint, timeout="30s")
        for vector in ([0.6,0.,0.8,0.], [0.,0.,0.,1.]):
            session.execute(f"MATCH (n:Item {{id: 0}}) SET n.embedding = {vector};")
        # A later unrelated update must not hide the vector mutation.
        session.execute("MATCH (n:Item {id: 0}) SET n.name = 'final';")
        session.execute("MATCH (n:Item {id: 1}) SET n.embedding = [0.6,0.8,0.0,0.0];")
        session.execute("MATCH (n:Item {id: 1}) DELETE n;")
        session.execute("CREATE (:Item {id: 3, name: 'new', embedding: [1.0,0.0,0.0,0.0]});")
        session.execute("MATCH (n:Item {id: 3}) SET n.embedding = [0.0,1.0,0.0,0.0];")
        session.execute("CREATE (:Item {id: 4, name: 'deleted', embedding: [1.0,0.0,0.0,0.0]});")
        session.execute("MATCH (n:Item {id: 4}) DELETE n;")
        session.close()
        db.stop_serving()
        db.close()
        """,
        path,
        unused_tcp_port,
        metric,
    )
    if legacy:
        checkpoint = path / "checkpoint"
        current = (checkpoint / "CURRENT").read_text().strip()
        manifest_path = checkpoint / "manifests" / (current + ".manifest")
        manifest = json.loads(manifest_path.read_text())
        descriptor = manifest["modules"]["index_item_hnsw"]
        del descriptor["extra"]["flat_index_offset"]
        del descriptor["objects"]["flat_index_buffer"]
        manifest_path.write_text(json.dumps(manifest))
    _run_cold_process(
        r"""
        import sys
        from neug import Database
        from test_hnsw_index import _profile_operator_names

        db = Database(sys.argv[1], mode="w", checkpoint_on_close=False)
        conn = db.connect()
        assert list(conn.execute(
            "CALL show_indexes(type := 'HNSW', name := 'item_hnsw') "
            "RETURN name, state, buffered_count;"
        )) == [["item_hnsw", "pending", None]]
        conn.execute("LOAD vector_search;")
        metric = sys.argv[2]
        def check():
            assert list(conn.execute("MATCH (n:Item) RETURN n.id ORDER BY n.id;")) == [
                [0], [2], [3], [9]
            ]
            for id, target in [(0, [0.,0.,0.,1.]), (3, [0.,1.,0.,0.])]:
                result = conn.execute(
                    "PROFILE MATCH (n:Item) RETURN n.id, "
                    f"vector_distance_{metric}(n.embedding, {target}) AS score "
                    "ORDER BY score LIMIT 4;"
                )
                rows = list(result)
                assert "IndexScanOpr" in _profile_operator_names(result)
                assert rows[0][0] == id, rows
                assert sorted(row[0] for row in rows) == [0, 2, 3, 9], rows
        check()
        # Both persisted pending indexes share the same recovered column log.
        result = conn.execute(
            "PROFILE MATCH (n:Item) RETURN n.id, "
            "vector_distance_ip(n.embedding,[0.0,1.0,0.0,0.0]) AS score "
            "ORDER BY score DESC LIMIT 10;"
        )
        assert "IndexScanOpr" in _profile_operator_names(result)
        assert sorted(row[0] for row in result) == [0, 2, 3, 9]
        stats = list(conn.execute("CALL optimize_indexes('hnsw', 'item_hnsw');"))
        if metric == "l2":
            assert stats[0][2] == 2, stats
        check()
        conn.execute("CHECKPOINT;")
        conn.close()
        db.close()
        """,
        path,
        metric,
    )
    # The repaired state and boundary must survive another cold checkpoint open.
    _run_cold_process(
        r"""
        import sys
        from pathlib import Path
        from neug import Database
        from test_hnsw_index import _local_buffer_checkpoint, _profile_operator_names

        path = Path(sys.argv[1])
        assert _local_buffer_checkpoint(path, "item_hnsw")[0] > 0
        db = Database(str(path), mode="w", checkpoint_on_close=False)
        conn = db.connect()
        conn.execute("LOAD vector_search;")
        result = conn.execute(
            "PROFILE MATCH (n:Item) RETURN n.id, "
            f"vector_distance_{sys.argv[2]}(n.embedding, [0.0,1.0,0.0,0.0]) AS score "
            "ORDER BY score LIMIT 4;"
        )
        rows = list(result)
        assert "IndexScanOpr" in _profile_operator_names(result)
        assert rows[0][0] == 3, rows
        assert sorted(row[0] for row in rows) == [0, 2, 3, 9], rows
        assert list(conn.execute("CALL optimize_indexes('hnsw', 'item_hnsw');"))[0][2] == 0
        conn.close()
        db.close()
        """,
        path,
        metric,
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
            f"COPY Item FROM (LOAD FROM {cypher_path(csv_path)} "
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


def test_bitmap_sparse_candidates_keep_reader_snapshot(tmp_path, unused_tcp_port):
    """Sparse scalar candidates remain stable across delete/update/optimize."""
    db = Database(
        str(tmp_path / "bitmap-snapshot"), mode="w", checkpoint_on_close=False
    )
    conn = db.connect()
    try:
        conn.execute("LOAD vector_search;")
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[4]);")
        conn.begin_transaction()
        for i in range(1100):
            conn.execute(f"CREATE (:Item {{id:{i}, v:[{float(i)},0.0,0.0,0.0]}});")
        conn.commit()
        conn.execute("CREATE INDEX item_hnsw ON Item USING HNSW(v);")
        conn.begin_transaction()
        for i in range(1100, 2200):
            conn.execute(f"CREATE (:Item {{id:{i}, v:[{float(i)},0.0,0.0,0.0]}});")
        conn.commit()
    finally:
        conn.close()
    endpoint = db.serve(unused_tcp_port, "localhost", False)
    wait_for_server_ready(endpoint)
    reader = Session.open(endpoint)
    writer = Session.open(endpoint)
    query = (
        "MATCH(n:Item) WHERE n.id=1 OR n.id=1101 RETURN n.id, "
        "vector_distance_l2(n.v, [0.0,0.0,0.0,0.0]) AS score "
        "ORDER BY score LIMIT 5;"
    )
    old = [[1, 1.0], [1101, 1212201.0]]
    try:
        reader.begin_transaction(read_only=True)
        assert list(reader.execute(query, access_mode="read")) == old
        writer.begin_transaction()
        writer.execute("MATCH(n:Item {id:1}) DELETE n;")
        writer.execute("MATCH(n:Item {id:1101}) SET n.v=[0.0,0.0,0.0,0.0];")
        writer.execute("CALL optimize_indexes('hnsw','item_hnsw');")
        assert list(writer.execute(query)) == [[1101, 0.0]]
        writer.rollback()
        assert list(writer.execute(query, access_mode="read")) == old
        assert list(reader.execute(query, access_mode="read")) == old
        writer.execute("MATCH(n:Item {id:1}) DELETE n;")
        writer.execute("MATCH(n:Item {id:1101}) SET n.v=[0.0,0.0,0.0,0.0];")
        writer.execute("CALL optimize_indexes('hnsw','item_hnsw');")
        assert list(reader.execute(query, access_mode="read")) == old
        reader.rollback()
        assert list(reader.execute(query, access_mode="read")) == [[1101, 0.0]]
    finally:
        reader.close()
        writer.close()
        db.stop_serving()
        db.close()
