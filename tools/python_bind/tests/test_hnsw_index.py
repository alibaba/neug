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

import hashlib
import json
import math
import os
import random
import subprocess
import sys
import textwrap

import pytest
from conftest import cypher_path

from neug import Database

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

DIMENSION = 16
NUM_VECTORS = 2000


@pytest.mark.parametrize("concurrency", [None, 0, 1, 4, 2147483647])
def test_index_concurrency_option_survives_reopen(tmp_path, concurrency):
    path = tmp_path / "concurrency-db"
    db = Database(str(path), mode="w", max_thread_num=1, checkpoint_on_close=False)
    conn = db.connect()
    option = "" if concurrency is None else f" WITH (concurrency={concurrency})"
    csv = tmp_path / "vectors.csv"
    csv.write_text("id|v\n" + "".join(f"{i}|[{float(i)},0.0]\n" for i in range(128)))
    try:
        conn.execute("LOAD vector_search;")
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[2]);")
        conn.execute(
            f"COPY Item FROM (LOAD FROM {cypher_path(csv)} (delim='|') "
            "RETURN id, CAST(v, 'FLOAT[2]') AS v);"
        )
        conn.execute(f"CREATE INDEX item_hnsw ON Item USING HNSW (v){option};")
        # COPY into an existing index exercises the same configured Optimize path.
        csv.write_text(
            "id|v\n" + "".join(f"{i}|[{float(i)},0.0]\n" for i in range(128, 256))
        )
        conn.execute(
            f"COPY Item FROM (LOAD FROM {cypher_path(csv)} (delim='|') "
            "RETURN id, CAST(v, 'FLOAT[2]') AS v);"
        )
        conn.execute("CHECKPOINT;")
    finally:
        _close_database(db, conn)

    checkpoint = path / "checkpoint"
    current = (checkpoint / "CURRENT").read_text().strip()
    manifest = json.loads(
        (checkpoint / "manifests" / f"{current}.manifest").read_text()
    )
    meta = json.loads(manifest["modules"]["index_item_hnsw"]["extra"]["index_meta"])
    if concurrency is None:
        assert "concurrency" not in meta["options"]
    else:
        assert meta["options"]["concurrency"] == str(concurrency)

    db = Database(str(path), mode="w", max_thread_num=2, checkpoint_on_close=False)
    conn = db.connect()
    try:
        conn.execute("LOAD vector_search;")
        for i in range(256, 320):
            conn.execute(f"CREATE (:Item {{id:{i}, v:[{float(i)},0.0]}});")
        rows = list(conn.execute("CALL optimize_indexes();"))
        assert rows[0][2:4] == [64, 0]
        assert list(conn.execute("CALL show_indexes() RETURN buffered_count;")) == [[0]]
        assert list(conn.execute("MATCH (n:Item) RETURN n.id ORDER BY n.id;")) == [
            [i] for i in range(320)
        ]
        for i in (0, 127, 128, 255, 319):
            result = conn.execute(
                "PROFILE MATCH (n:Item) RETURN n.id, "
                "vector_distance_l2(n.v,$v) AS score ORDER BY score LIMIT 1;",
                parameters={"v": [float(i), 0.0]},
            )
            assert list(result) == [[i, 0.0]]
            assert "IndexScanOpr" in _profile_operator_names(result)
    finally:
        _close_database(db, conn)


@pytest.mark.parametrize(
    "value", ["'-1'", "1.5", "'2workers'", "'invalid'", "2147483648"]
)
def test_index_concurrency_rejects_invalid_values(tmp_path, value):
    db, conn = _open_database(tmp_path / "invalid-concurrency")
    try:
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[2]);")
        with pytest.raises(
            RuntimeError,
            match=(
                r"concurrency.*must be a non-negative integer; "
                r"0 uses the maximum number of CPU cores on the hardware platform\. "
                r"Large values are capped by the hardware CPU core count and the "
                r"number of task chunks\."
            ),
        ):
            conn.execute(
                "CREATE INDEX item_hnsw ON Item USING HNSW (v) "
                f"WITH (concurrency={value});"
            )
    finally:
        _close_database(db, conn)


def _local_buffer_checkpoint(path, name):
    checkpoint = path / "checkpoint"
    current = (checkpoint / "CURRENT").read_text().strip()
    manifest = json.loads(
        (checkpoint / "manifests" / (current + ".manifest")).read_text()
    )
    descriptor = manifest["modules"]["index_" + name]
    assert "hybrid_format_version" not in descriptor["extra"]
    flat_object = descriptor["objects"]["flat_index_buffer"]
    assert (checkpoint / "objects" / flat_object).is_file()
    object_id = descriptor.get("objects", {}).get("index_buffer")
    digest = (
        hashlib.sha256((checkpoint / "objects" / object_id).read_bytes()).hexdigest()
        if object_id
        else None
    )
    return int(descriptor["extra"]["flat_index_offset"]), digest


@pytest.mark.parametrize("metric", ["l2", "ip", "cosine"])
@pytest.mark.parametrize("initial_count", [0, 6])
def test_local_buffer_mixed_search_and_checkpoint(tmp_path, metric, initial_count):
    """Writes stay out of HNSW while Flat remains exact, including after reopen."""
    path = tmp_path / "local-buffer"
    db, conn = _open_database(path, checkpoint_on_close=False)
    rng = random.Random(481)
    vectors = [[rng.uniform(-1, 1) for _ in range(4)] for _ in range(12)]

    def create(index):
        conn.execute(
            f"CREATE (:Buffered {{id: {index}, group_id: {index % 2}, "
            f"embedding: {_array_literal(vectors[index])}}});"
        )

    def check():
        for predicate in ("", "WHERE n.group_id=0 ", "WHERE n.group_id=99 "):
            distance = f"vector_distance_{metric}(n.embedding, $query)"
            direction = "DESC" if metric == "ip" else "ASC"
            body = f"MATCH (n:Buffered) {predicate}RETURN n.id, "
            suffix = f" AS score ORDER BY score {direction} SKIP 1 LIMIT 3;"
            params = {"query": vectors[10]}
            indexed = conn.execute(
                "PROFILE " + body + distance + suffix, parameters=params
            )
            actual = list(indexed)
            assert "IndexScanOpr" in _profile_operator_names(indexed)
            # Arithmetic around the distance prevents the ANN rewrite.
            expected = list(
                conn.execute(body + distance + " + 0.0" + suffix, parameters=params)
            )
            assert [row[0] for row in actual] == [row[0] for row in expected]
            assert [row[1] for row in actual] == pytest.approx(
                [row[1] for row in expected], abs=2e-5
            )

    try:
        conn.execute(
            "CREATE NODE TABLE Buffered(id INT64 PRIMARY KEY, group_id INT64, embedding FLOAT[4]);"
        )
        for i in range(initial_count):
            create(i)
        conn.execute(
            "CREATE INDEX buffered_hnsw ON Buffered USING HNSW (embedding) "
            f"WITH (metric='{metric}', cosine_normalize=true, m=8, ef_construction=40);"
        )
        conn.execute("CHECKPOINT;")
        initial_offset, _ = _local_buffer_checkpoint(path, "buffered_hnsw")
        assert initial_offset == initial_count
        for i in range(initial_count, 8):
            create(i)
        conn.execute(
            "MERGE (n:Buffered {id: 8}) SET n.group_id=0, "
            f"n.embedding={_array_literal(vectors[8])};"
        )
        for i in (9, 10):
            conn.execute(
                f"MATCH (n:Buffered {{id: 0}}) SET n.embedding={_array_literal(vectors[i])};"
            )
        conn.execute("MATCH (n:Buffered) WHERE n.id=1 OR n.id=7 DELETE n;")
        conn.begin_transaction()
        create(11)
        conn.rollback()
        check()
        conn.execute("CHECKPOINT;")
        # Flushing HNSW can update timestamps without changing its indexed
        # prefix. Verify the boundary and persisted graph presence, not bytes.
        offset, graph_digest = _local_buffer_checkpoint(path, "buffered_hnsw")
        assert offset == initial_count
        assert (graph_digest is not None) == (initial_count > 0)
    finally:
        _close_database(db, conn)
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        check()
        create(11)
        check()
    finally:
        _close_database(db, conn)


def test_local_buffer_copy_builds_existing_tail(tmp_path):
    db, conn = _open_database(tmp_path / "copy-db", checkpoint_on_close=False)
    try:
        conn.execute(
            "CREATE NODE TABLE Buffered(id INT64 PRIMARY KEY, embedding FLOAT[4]);"
        )
        conn.execute("CREATE INDEX buffered_hnsw ON Buffered USING HNSW (embedding);")
        conn.execute("CREATE (:Buffered {id: 0, embedding: [0.0, 0.0, 0.0, 0.0]});")
        csv = tmp_path / "tail.csv"
        csv.write_text("id|embedding\n1|[1.0,0.0,0.0,0.0]\n2|[2.0,0.0,0.0,0.0]\n")
        conn.execute(
            f"COPY Buffered FROM (LOAD FROM {cypher_path(csv)} (delim='|') "
            "RETURN id, CAST(embedding, 'FLOAT[4]') AS embedding);"
        )
        conn.execute("CHECKPOINT;")
        assert _local_buffer_checkpoint(tmp_path / "copy-db", "buffered_hnsw")[0] == 3
        rows = list(
            conn.execute(
                "MATCH (n:Buffered) RETURN n.id, "
                "vector_distance_l2(n.embedding, [0.0,0.0,0.0,0.0]) "
                "AS score ORDER BY score LIMIT 3;"
            )
        )
        assert rows == [[0, 0.0], [1, 1.0], [2, 4.0]]
    finally:
        _close_database(db, conn)


@pytest.mark.parametrize("delete_local_buffer", [False, True], ids=["append", "delete"])
def test_local_buffer_copy_retry_after_transaction_failure(
    tmp_path, capfd, delete_local_buffer
):
    path = tmp_path / "copy-retry"
    db, conn = _open_database(path, checkpoint_on_close=False)

    def copy_from(csv):
        conn.execute(
            f"COPY Buffered FROM (LOAD FROM {cypher_path(csv)} (delim='|') "
            "RETURN id, CAST(embedding, 'FLOAT[4]') AS embedding);"
        )

    def check(expected_ids):
        assert list(conn.execute("MATCH (n:Buffered) RETURN n.id ORDER BY n.id;")) == [
            [i] for i in expected_ids
        ]
        result = conn.execute(
            "PROFILE MATCH (n:Buffered) RETURN n.id, "
            "vector_distance_l2(n.embedding, [0.0,0.0,0.0,0.0]) AS score "
            "ORDER BY score LIMIT 10;"
        )
        assert list(result) == [[i, float(i * i)] for i in expected_ids]
        assert "IndexScanOpr" in _profile_operator_names(result)
        for i in expected_ids:
            result = conn.execute(
                "PROFILE MATCH (n:Buffered) RETURN n.id, "
                f"vector_distance_l2(n.embedding, [{float(i)},0.0,0.0,0.0]) "
                "AS score ORDER BY score LIMIT 1;"
            )
            assert list(result) == [[i, 0.0]]
            assert "IndexScanOpr" in _profile_operator_names(result)

    try:
        conn.execute(
            "CREATE NODE TABLE Buffered(id INT64 PRIMARY KEY, embedding FLOAT[4]);"
        )
        conn.execute("CREATE INDEX buffered_hnsw ON Buffered USING HNSW (embedding);")
        for i in range(2):
            conn.execute(
                f"CREATE (:Buffered {{id: {i}, embedding: [{float(i)},0.0,0.0,0.0]}});"
            )
        conn.execute("CHECKPOINT;")
        assert _local_buffer_checkpoint(path, "buffered_hnsw")[0] == 0
        first = tmp_path / "first.csv"
        first.write_text("id|embedding\n10|[10.0,0.0,0.0,0.0]\n")
        second = tmp_path / "second.csv"
        second.write_text("id|embedding\n20|[20.0,0.0,0.0,0.0]\n")

        conn.begin_transaction()
        if delete_local_buffer:
            conn.execute("MATCH (n:Buffered {id: 0}) DELETE n;")
            # Both scalar and index queries must respect the private deletion.
            check([1])
        copy_from(first)
        check([1, 10] if delete_local_buffer else [0, 1, 10])
        # COPY has appended the LocalBuffer to the shared HNSW. Fail commit
        # afterwards so the transaction rolls back its rows and F, but not
        # those physical HNSW additions. A malformed CSV would fail too early.
        manifests = path / "checkpoint" / "manifests"
        saved = path / "checkpoint" / "saved_manifests"
        manifests.rename(saved)
        try:
            manifests.write_text("block checkpoint directory creation")
            with pytest.raises(RuntimeError, match="failed to create"):
                conn.commit()
        finally:
            manifests.unlink()
            saved.rename(manifests)
        with pytest.raises(RuntimeError, match="rollback-only"):
            conn.commit()
        conn.rollback()
        check([0, 1])
        assert _local_buffer_checkpoint(path, "buffered_hnsw")[0] == 0

        capfd.readouterr()
        conn.begin_transaction()
        copy_from(second)
        conn.commit()
        captured = capfd.readouterr()
        existing_ids = [1] if delete_local_buffer else [0, 1]
        for i in existing_ids:
            assert (
                f"Skip existing HNSW index_id during bulk append: {i}" in captured.err
            )
        if delete_local_buffer:
            # The aborted COPY skipped this ID. After rollback it must be
            # appended for the first time, rather than treated as a duplicate.
            assert (
                "Skip existing HNSW index_id during bulk append: 0" not in captured.err
            )
        check([0, 1, 20])
        assert _local_buffer_checkpoint(path, "buffered_hnsw")[0] > 0
    finally:
        _close_database(db, conn)

    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        check([0, 1, 20])
    finally:
        _close_database(db, conn)


@pytest.mark.parametrize(
    "arguments,expected_names",
    [
        ("", ["a_hnsw", "b_hnsw"]),
        ("'hnsw'", ["a_hnsw", "b_hnsw"]),
        ("'hnsw', 'b_hnsw'", ["b_hnsw"]),
        ("'hnsw', name := 'b_hnsw'", ["b_hnsw"]),
        ("type := 'HNSW'", ["a_hnsw", "b_hnsw"]),
        ("name := 'b_hnsw'", ["b_hnsw"]),
        ("type := 'hnsw', name := 'b_hnsw'", ["b_hnsw"]),
        ("name := 'b_hnsw', type := 'hnsw'", ["b_hnsw"]),
        ("name := $name, type := $type", ["b_hnsw"]),
        ("$type, $name", ["b_hnsw"]),
    ],
)
def test_index_optimize_selection_and_checkpoint(tmp_path, arguments, expected_names):
    path = tmp_path / "optimize"
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[4]);")
        for name in ("a_hnsw", "b_hnsw"):
            conn.execute(f"CREATE INDEX {name} ON Item USING HNSW (v);")
        for i in range(3):
            conn.execute(f"CREATE (:Item {{id: {i}, v: [{float(i)},0.0,0.0,0.0]}});")
        conn.execute("MATCH (n:Item {id: 1}) DELETE n;")
        conn.execute("MATCH (n:Item {id: 2}) SET n.v=[4.0,0.0,0.0,0.0];")
        parameters = {"name": "b_hnsw", "type": "HNSW"} if "$" in arguments else None
        for _ in range(2):
            conn.begin_transaction(read_only=True)
            stats = list(
                conn.execute(
                    f"CALL show_indexes({arguments}) RETURN type, name, buffered_count;",
                    parameters=parameters,
                )
            )
            conn.commit()
            assert stats == [["hnsw", name, 4] for name in expected_names]
        rows = list(
            conn.execute(f"CALL optimize_indexes({arguments});", parameters=parameters)
        )
        assert [row[:4] for row in rows] == [
            ["hnsw", name, 2, 2] for name in expected_names
        ]
        assert all(len(row) == 5 and row[4] >= 0 for row in rows)
        stats = list(
            conn.execute(
                f"CALL show_indexes({arguments}) RETURN type, name, buffered_count;",
                parameters=parameters,
            )
        )
        assert stats == [["hnsw", name, 0] for name in expected_names]

        repeated = list(
            conn.execute(f"CALL optimize_indexes({arguments});", parameters=parameters)
        )
        assert [row[:4] for row in repeated] == [
            ["hnsw", name, 0, 0] for name in expected_names
        ]
        conn.execute("CHECKPOINT;")
        for name in ("a_hnsw", "b_hnsw"):
            assert _local_buffer_checkpoint(path, name)[0] == (
                4 if name in expected_names else 0
            )
    finally:
        _close_database(db, conn)
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        result = conn.execute(
            "PROFILE MATCH (n:Item) RETURN n.id, vector_distance_l2(n.v, [4.0,0.0,0.0,0.0]) "
            "AS score ORDER BY score LIMIT 5;"
        )
        assert list(result) == [[2, 0.0], [0, 16.0]]
        assert "IndexScanOpr" in _profile_operator_names(result)
    finally:
        _close_database(db, conn)


def test_index_optimize_validation_and_deleted_tail(tmp_path):
    path = tmp_path / "optimize-validation"
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        assert list(conn.execute("CALL optimize_indexes();")) == []
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[4]);")
        conn.execute("CREATE INDEX item_hnsw ON Item USING HNSW (v);")
        conn.execute("CREATE (:Item {id: 0, v: [0.0,0.0,0.0,0.0]});")
        for arguments in (
            "name := 'missing'",
            "type := 'fts', name := 'item_hnsw'",
            "typo := 'hnsw'",
            "type := 'hnsw', type := 'hnsw'",
            "'hnsw', type := 'hnsw'",
            "type := 12",
            "type := null",
            "name := ''",
            "'hnsw', 'item_hnsw', 'extra'",
        ):
            with pytest.raises(RuntimeError):
                conn.execute(f"CALL optimize_indexes({arguments});")
            if arguments in ("name := 'missing'", "type := 'fts', name := 'item_hnsw'"):
                assert list(conn.execute(f"CALL show_indexes({arguments});")) == []
            else:
                with pytest.raises(RuntimeError):
                    conn.execute(f"CALL show_indexes({arguments});")
        assert list(conn.execute("CALL optimize_indexes(type := 'missing');")) == []
        conn.begin_transaction(read_only=True)
        with pytest.raises(RuntimeError):
            conn.execute("CALL optimize_indexes();")
        conn.rollback()
        conn.execute("MATCH (n:Item) DELETE n;")
        rows = list(conn.execute("CALL optimize_indexes();"))
        assert rows[0][:4] == ["hnsw", "item_hnsw", 0, 1]
        conn.execute("CHECKPOINT;")
        assert _local_buffer_checkpoint(path, "item_hnsw")[0] == 1
    finally:
        _close_database(db, conn)


def test_index_optimize_atomic_rollback_and_retry(tmp_path, capfd):
    path = tmp_path / "optimize-retry"
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[4]);")
        for name in ("a_hnsw", "b_hnsw"):
            conn.execute(f"CREATE INDEX {name} ON Item USING HNSW (v);")
        conn.execute("CREATE (:Item {id: 0, v: [1.0,0.0,0.0,0.0]});")
        conn.begin_transaction()
        rows = list(conn.execute("CALL optimize_indexes();"))
        assert [row[:4] for row in rows] == [
            ["hnsw", name, 1, 0] for name in ("a_hnsw", "b_hnsw")
        ]
        with pytest.raises(RuntimeError):
            conn.execute("CALL optimize_indexes(name := 'missing');")
        with pytest.raises(RuntimeError, match="rollback-only"):
            conn.commit()
        conn.rollback()
        conn.execute("CHECKPOINT;")
        for name in ("a_hnsw", "b_hnsw"):
            assert _local_buffer_checkpoint(path, name)[0] == 0
        capfd.readouterr()
        rows = list(conn.execute("CALL optimize_indexes();"))
        assert [row[:4] for row in rows] == [
            ["hnsw", name, 0, 1] for name in ("a_hnsw", "b_hnsw")
        ]
        assert capfd.readouterr().err.count("Skip existing HNSW index_id") == 2
        assert list(conn.execute("MATCH (n:Item) RETURN n.id;")) == [[0]]
        conn.execute("CHECKPOINT;")
        for name in ("a_hnsw", "b_hnsw"):
            assert _local_buffer_checkpoint(path, name)[0] == 1
    finally:
        _close_database(db, conn)


@pytest.mark.parametrize(
    "offset",
    [None, "", "-1", "abc", "1x", "4294967296", "999999999999999999999999", "4"],
)
def test_local_buffer_offset_metadata(tmp_path, offset):
    path = tmp_path / "offset-metadata"
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        conn.execute(
            "CREATE NODE TABLE Item(id INT64 PRIMARY KEY, embedding FLOAT[4]);"
        )
        for i in range(3):
            conn.execute(
                f"CREATE (:Item {{id: {i}, embedding: [{float(i)},0.0,0.0,0.0]}});"
            )
        conn.execute("CREATE INDEX item_hnsw ON Item USING HNSW (embedding);")
        conn.execute("CHECKPOINT;")
    finally:
        _close_database(db, conn)
    checkpoint = path / "checkpoint"
    current = (checkpoint / "CURRENT").read_text().strip()
    manifest_path = checkpoint / "manifests" / (current + ".manifest")
    manifest = json.loads(manifest_path.read_text())
    descriptor = manifest["modules"]["index_item_hnsw"]
    if offset is None:
        # Legacy checkpoint: neither an offset nor a persisted Flat registry.
        del descriptor["extra"]["flat_index_offset"]
        del descriptor["objects"]["flat_index_buffer"]
    else:
        descriptor["extra"]["flat_index_offset"] = offset
    manifest_path.write_text(json.dumps(manifest))
    if offset is not None:
        reopened = None
        try:
            with pytest.raises(RuntimeError):
                reopened = Database(str(path), mode="w", checkpoint_on_close=False)
                connection = reopened.connect()
                try:
                    connection.execute("LOAD vector_search;")
                finally:
                    connection.close()
        finally:
            if reopened is not None:
                reopened.close()
        return
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        conn.execute("CREATE (:Item {id: 3, embedding: [3.0,0.0,0.0,0.0]});")
        result = conn.execute(
            "PROFILE MATCH (n:Item) RETURN n.id, "
            "vector_distance_l2(n.embedding, [3.0,0.0,0.0,0.0]) AS score "
            "ORDER BY score LIMIT 1;"
        )
        assert list(result) == [[3, 0.0]]
        assert "IndexScanOpr" in _profile_operator_names(result)
        conn.execute("CHECKPOINT;")
        assert _local_buffer_checkpoint(path, "item_hnsw")[0] == 3
    finally:
        _close_database(db, conn)


def _array_literal(values):
    literals = []
    for value in values:
        literal = format(float(value), ".9g")
        if "." not in literal and "e" not in literal.lower():
            literal += ".0"
        literals.append(literal)
    return "[" + ",".join(literals) + "]"


def _constant_vector(value):
    return [float(value)] * DIMENSION


def _cosine_vector(index):
    angle = 2.0 * math.pi * index / NUM_VECTORS
    return [math.cos(angle), math.sin(angle)] + [0.0] * (DIMENSION - 2)


def _open_database(path, checkpoint_on_close=True):
    db = Database(db_path=str(path), mode="w", checkpoint_on_close=checkpoint_on_close)
    conn = db.connect()
    conn.execute("LOAD vector_search;")
    return db, conn


def _close_database(db, conn):
    conn.close()
    db.close()


def _duplicate_statistics_line(log_output, index_name):
    marker = f"HNSW duplicate statistics for index '{index_name}'"
    matching_lines = [line for line in log_output.splitlines() if marker in line]
    assert len(matching_lines) == 1, log_output
    return matching_lines[0]


def _l2_search(conn, query_value, topk=10, predicate=""):
    where = f"WHERE {predicate} " if predicate else ""
    return list(
        conn.execute(
            f"MATCH (n:Item) {where}"
            "RETURN n.id, "
            f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(query_value))}) "
            "AS score ORDER BY score ASC "
            f"LIMIT {topk};"
        )
    )


def _profile_operator_names(result):
    return [
        operator["operator_name"]
        for operator in result.get_profile_metrics()["operators"]
    ]


def _create_advanced_data(conn):
    conn.execute(
        "CREATE NODE TABLE Item("
        "id INT64 PRIMARY KEY, group_id INT64, name STRING, "
        f"l2_vec FLOAT[{DIMENSION}], "
        f"cosine_vec FLOAT[{DIMENSION}], "
        f"ip_vec FLOAT[{DIMENSION}]);"
    )
    conn.execute("CREATE REL TABLE NEXT(FROM Item TO Item);")
    for start in range(0, NUM_VECTORS, 100):
        nodes = []
        for index in range(start, min(start + 100, NUM_VECTORS)):
            nodes.append(
                "(:Item {"
                f"id: {index}, group_id: {index % 2}, name: 'item_{index}', "
                f"l2_vec: {_array_literal(_constant_vector(index))}, "
                f"cosine_vec: {_array_literal(_cosine_vector(index))}, "
                f"ip_vec: {_array_literal(_constant_vector(index))}"
                "})"
            )
        conn.execute("CREATE " + ",".join(nodes) + ";")

    for start in range(0, NUM_VECTORS - 1, 100):
        matches = []
        edges = []
        for index in range(start, min(start + 100, NUM_VECTORS - 1)):
            suffix = index - start
            matches.append(
                f"(source{suffix}:Item {{id: {index}}}),"
                f"(target{suffix}:Item {{id: {index + 1}}})"
            )
            edges.append(f"(source{suffix})-[:NEXT]->(target{suffix})")
        conn.execute("MATCH " + ",".join(matches) + " CREATE " + ",".join(edges) + ";")

    for name, prop, metric in (
        ("item_l2_hnsw", "l2_vec", "l2"),
        ("item_cosine_hnsw", "cosine_vec", "cosine"),
        ("item_ip_hnsw", "ip_vec", "ip"),
    ):
        conn.execute(
            f"CREATE INDEX {name} ON Item USING HNSW ({prop}) "
            f"WITH (metric = '{metric}', m = 16, ef_construction = 200);"
        )

    conn.execute("CREATE NODE TABLE Source(id STRING PRIMARY KEY);")
    conn.execute("CREATE NODE TABLE Chunk(id STRING PRIMARY KEY, embedding FLOAT[4]);")
    conn.execute("CREATE REL TABLE HAS_CHUNK(FROM Source TO Chunk);")
    conn.execute("CREATE (:Source {id: 'source'});")
    conn.execute(
        "CREATE (:Chunk {id: 'chunk-a', embedding: [1.0, 0.0, 0.0, 0.0]}), "
        "(:Chunk {id: 'chunk-b', embedding: [0.0, 1.0, 0.0, 0.0]});"
    )
    conn.execute(
        "MATCH (source:Source), (chunk:Chunk) " "CREATE (source)-[:HAS_CHUNK]->(chunk);"
    )
    conn.execute(
        "CREATE INDEX chunk_hnsw ON Chunk USING HNSW (embedding) "
        "WITH (metric = 'cosine', m = 2, ef_construction = 10);"
    )


@pytest.fixture(scope="module")
def advanced_database(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("hnsw-advanced") / "database"
    db, conn = _open_database(db_path)
    try:
        _create_advanced_data(conn)
    finally:
        _close_database(db, conn)
    return db_path


@pytest.fixture(scope="module")
def advanced_connection(advanced_database):
    db, conn = _open_database(advanced_database, checkpoint_on_close=False)
    try:
        yield conn
    finally:
        _close_database(db, conn)


def test_l2_index_scan_and_index_filtering(advanced_connection):
    rows = _l2_search(advanced_connection, 3.1)
    assert [row[0] for row in rows[:3]] == [3, 4, 2]
    assert [row[1] for row in rows[:3]] == pytest.approx([0.16, 12.96, 19.36], abs=1e-5)
    assert _l2_search(advanced_connection, 0.0)[0][0] == 0
    centered = _l2_search(advanced_connection, 500.0)[:3]
    assert centered[0][0] == 500
    assert {row[0] for row in centered[1:]} == {499, 501}
    assert _l2_search(advanced_connection, 999.0)[0][0] == 999
    filtered = _l2_search(advanced_connection, 3.1, 2, "n.group_id = 0")
    assert [row[0] for row in filtered] == [4, 2]


def test_l2_query_uses_hnsw_index_scan(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) RETURN n.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT 3;"
    )
    list(result)
    assert "IndexScanOpr" in _profile_operator_names(result)


def test_property_access_after_hnsw_index_scan(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) RETURN n.id, n.name, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT 3;"
    )
    rows = list(result)
    assert [(row[0], row[1]) for row in rows] == [
        (500, "item_500"),
        (501, "item_501"),
        (499, "item_499"),
    ]
    assert "IndexScanOpr" in _profile_operator_names(result)


def test_primary_key_equality_does_not_use_hnsw_index_scan(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) WHERE n.id = 500 RETURN n.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT 3;"
    )
    rows = list(result)
    assert rows[0] == pytest.approx([500, 0.16], abs=3e-5)
    assert "IndexScanOpr" not in _profile_operator_names(result)


def test_hnsw_index_scan_with_dynamic_target(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) RETURN n.id, "
        "vector_distance_l2(n.l2_vec, $target) AS score "
        "ORDER BY score ASC LIMIT 3;",
        parameters={"target": _constant_vector(500.1)},
    )
    rows = list(result)
    assert [row[0] for row in rows] == [500, 501, 499]
    assert "IndexScanOpr" in _profile_operator_names(result)


def test_hnsw_index_scan_with_dynamic_limit(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) RETURN n.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT $k;",
        parameters={"k": 3},
    )

    assert [row[0] for row in result] == [500, 501, 499]
    operator_names = _profile_operator_names(result)
    assert "IndexScanOpr" in operator_names
    assert "ProjectOrderByOprBeta" in operator_names


def test_hnsw_index_scan_with_dynamic_skip_and_limit(advanced_connection):
    query = (
        "PROFILE MATCH (n:Item) RETURN n.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC SKIP $offset LIMIT $k;"
    )
    result = advanced_connection.execute(query, parameters={"offset": 1, "k": 2})

    rows = list(result)
    assert [row[0] for row in rows] == [501, 499]
    assert "IndexScanOpr" in _profile_operator_names(result)
    assert "ProjectOrderByOprBeta" in _profile_operator_names(result)

    result = advanced_connection.execute(query, parameters={"offset": 0, "k": 1})
    assert [row[0] for row in result] == [500]
    assert "IndexScanOpr" in _profile_operator_names(result)
    assert "ProjectOrderByOprBeta" in _profile_operator_names(result)

    result = advanced_connection.execute(query, parameters={"offset": 10, "k": 0})
    assert list(result) == []
    assert "IndexScanOpr" in _profile_operator_names(result)
    assert "ProjectOrderByOprBeta" in _profile_operator_names(result)


def test_hnsw_dynamic_skip_without_limit_uses_brute_force(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) RETURN n.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC SKIP $offset;",
        parameters={"offset": 1},
    )

    rows = list(result)
    assert len(rows) == NUM_VECTORS - 1
    assert [row[0] for row in rows[:2]] == [501, 499]
    operator_names = _profile_operator_names(result)
    assert "IndexScanOpr" not in operator_names
    assert "OrderByOpr" in operator_names


@pytest.mark.parametrize(
    ("suffix", "parameters"),
    [
        ("LIMIT 4294967296", None),
        ("LIMIT $value", {"value": 2**32}),
        ("SKIP 4294967296 LIMIT 1", None),
        ("SKIP $value LIMIT 1", {"value": 2**32}),
    ],
)
def test_hnsw_rejects_range_values_above_uint32_max(
    advanced_connection, suffix, parameters
):
    with pytest.raises(Exception, match="exceeds maximum allowed value: 4294967295"):
        advanced_connection.execute(
            "MATCH (n:Item) RETURN n.id, "
            f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
            f"AS score ORDER BY score ASC {suffix}",
            parameters=parameters,
        )


def test_scalar_function_with_dynamic_target(advanced_connection):
    node_vector = _constant_vector(500.0)
    target_vector = _constant_vector(500.1)
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) WHERE n.id = 500 RETURN n.id, "
        "vector_distance_l2(n.l2_vec, $target) AS score;",
        parameters={"target": target_vector},
    )
    rows = list(result)
    expected_score = sum(
        (left - right) ** 2 for left, right in zip(node_vector, target_vector)
    )
    assert len(rows) == 1
    assert rows[0] == pytest.approx([500, expected_score], abs=3e-5)
    assert "IndexScanOpr" not in _profile_operator_names(result)


def test_cosine_index_scan(advanced_connection):
    target_id = 181
    rows = list(
        advanced_connection.execute(
            "MATCH (n:Item) RETURN n.id, "
            f"vector_distance_cosine(n.cosine_vec, {_array_literal(_cosine_vector(target_id))}) "
            "AS score ORDER BY score ASC LIMIT 3;"
        )
    )
    assert rows[0][0] == target_id
    assert rows[0][1] == pytest.approx(0.0, abs=1e-6)
    assert {rows[1][0], rows[2][0]} == {target_id - 1, target_id + 1}


def test_inner_product_index_scan(advanced_connection):
    rows = list(
        advanced_connection.execute(
            "MATCH (n:Item) RETURN n.id, "
            f"vector_distance_ip(n.ip_vec, {_array_literal([1.0] * DIMENSION)}) "
            "AS score ORDER BY score DESC LIMIT 3;"
        )
    )
    expected_ids = [NUM_VECTORS - 1, NUM_VECTORS - 2, NUM_VECTORS - 3]
    assert [row[0] for row in rows] == expected_ids
    assert [row[1] for row in rows] == pytest.approx(
        [index * DIMENSION for index in expected_ids]
    )


def test_hnsw_limit_above_1024(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) RETURN n.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.0))}) "
        "AS score ORDER BY score ASC LIMIT 1025;"
    )
    rows = list(result)
    assert len(rows) == 1025
    assert "IndexScanOpr" in _profile_operator_names(result)


def test_graph_filtering_during_index_scan(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (source:Item)-[:NEXT]->(n:Item) RETURN n.id, n.name, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT 3;"
    )
    rows = list(result)
    assert rows[0][:2] == [500, "item_500"]
    assert {(row[0], row[1]) for row in rows[1:3]} == {
        (499, "item_499"),
        (501, "item_501"),
    }
    operators = _profile_operator_names(result)
    assert "IndexScanOpr" in operators
    assert "JoinOpr" not in operators

    node_result = advanced_connection.execute(
        "PROFILE MATCH (source:Item)-[:NEXT]->(n:Item) RETURN n, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT 3;"
    )
    assert len(list(node_result)) == 3
    node_operators = _profile_operator_names(node_result)
    assert "IndexScanOpr" in node_operators
    assert "JoinOpr" not in node_operators

    empty_result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) WHERE n.group_id = 99 RETURN n.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT 3;"
    )
    assert list(empty_result) == []
    assert "IndexScanOpr" in _profile_operator_names(empty_result)


def test_correlated_pattern_outputs_use_hnsw_join(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item)-[:NEXT]->(e:Item) RETURN n.id, e.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT 3;"
    )
    rows = list(result)
    assert [(row[0], row[1]) for row in rows] == [
        (500, 501),
        (501, 502),
        (499, 500),
    ]
    operators = _profile_operator_names(result)
    assert "IndexScanOpr" in operators
    assert "JoinOpr" in operators


def test_correlated_hnsw_join_applies_final_limit_after_fanout(
    advanced_connection,
):
    result = advanced_connection.execute(
        "PROFILE MATCH (n:Item)-[:NEXT]-(e:Item) RETURN n.id, e.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT 3;"
    )
    rows = list(result)
    assert len(rows) == 3
    assert [row[0] for row in rows[:2]] == [500, 500]
    assert {row[1] for row in rows[:2]} == {499, 501}
    assert rows[2][0] == 501
    operators = _profile_operator_names(result)
    assert "IndexScanOpr" in operators
    assert "JoinOpr" in operators


def test_issue_935_graph_constrained_topk_uses_hnsw_join(advanced_connection):
    result = advanced_connection.execute(
        "PROFILE MATCH (source:Source)-[:HAS_CHUNK]->(chunk:Chunk) "
        "RETURN source.id, chunk.id, "
        "vector_distance_cosine(chunk.embedding, $embedding) AS distance "
        "ORDER BY distance ASC LIMIT 1;",
        parameters={"embedding": [1.0, 0.0, 0.0, 0.0]},
    )
    rows = list(result)
    assert [row[:2] for row in rows] == [["source", "chunk-a"]]
    assert rows[0][2] == pytest.approx(0.0)
    operators = _profile_operator_names(result)
    assert "IndexScanOpr" in operators
    assert "JoinOpr" in operators


def test_correlated_hnsw_join_passes_scan_predicate_to_index(
    advanced_connection,
):
    result = advanced_connection.execute(
        "PROFILE MATCH (source:Source)-[:HAS_CHUNK]->(chunk:Chunk) "
        "WHERE chunk.id = 'chunk-b' "
        "RETURN source.id, chunk.id, "
        "vector_distance_cosine(chunk.embedding, $embedding) AS distance "
        "ORDER BY distance ASC LIMIT 1;",
        parameters={"embedding": [1.0, 0.0, 0.0, 0.0]},
    )
    rows = list(result)
    assert [row[:2] for row in rows] == [["source", "chunk-b"]]
    assert rows[0][2] == pytest.approx(1.0)
    operators = _profile_operator_names(result)
    assert "IndexScanOpr" in operators
    assert "JoinOpr" in operators


def test_update_and_delete_maintain_index(advanced_connection):
    advanced_connection.execute(
        "MATCH (n:Item {id: 3}) "
        f"SET n.l2_vec = {_array_literal(_constant_vector(700.25))};"
    )
    assert _l2_search(advanced_connection, 700.25, 1)[0] == pytest.approx(
        [3, 0.0], abs=1e-6
    )
    advanced_connection.execute("MATCH (n:Item {id: 333}) DELETE n;")
    rows = _l2_search(advanced_connection, 333.1)
    assert 333 not in [row[0] for row in rows]
    assert rows[0][0] == 334


def test_bulk_build_reports_duplicate_vectors(capfd):
    db, conn = _open_database(":memory:")
    try:
        conn.execute(
            "CREATE NODE TABLE Item(" "id INT64 PRIMARY KEY, embedding FLOAT[8]);"
        )
        for i in range(100):
            vector = [float(i % 17)] * 8
            conn.execute(
                "CREATE (:Item {id: $id, embedding: $embedding})",
                parameters={"id": i, "embedding": vector},
            )

        capfd.readouterr()
        conn.execute(
            "CREATE INDEX item_hnsw ON Item USING HNSW (embedding) "
            "WITH (metric='ip', m=16, ef_construction=200);"
        )
        statistics_line = _duplicate_statistics_line(
            capfd.readouterr().err, "item_hnsw"
        )
        assert statistics_line.startswith("W")
        assert "83 / 100 (83%) duplicate vectors" in statistics_line
    finally:
        _close_database(db, conn)


def test_bulk_build_reports_low_duplicate_ratio_as_info(capfd):
    db, conn = _open_database(":memory:")
    try:
        conn.execute(
            "CREATE NODE TABLE Item(" "id INT64 PRIMARY KEY, embedding FLOAT[8]);"
        )
        for i in range(10):
            conn.execute(
                "CREATE (:Item {id: $id, embedding: $embedding})",
                parameters={"id": i, "embedding": [float(i)] * 8},
            )

        capfd.readouterr()
        conn.execute(
            "CREATE INDEX item_hnsw ON Item USING HNSW (embedding) "
            "WITH (metric='ip', m=16, ef_construction=200);"
        )
        statistics_line = _duplicate_statistics_line(
            capfd.readouterr().err, "item_hnsw"
        )
        assert statistics_line.startswith("I")
        assert "0 / 10 (0%) duplicate vectors" in statistics_line
    finally:
        _close_database(db, conn)


def test_bulk_build_ignores_index_id_consumed_by_rolled_back_transaction(capfd):
    db, conn = _open_database(":memory:")
    try:
        conn.execute(
            "CREATE NODE TABLE Item(" "id INT64 PRIMARY KEY, embedding FLOAT[4]);"
        )
        conn.execute(
            "CREATE (:Item {id: 1, embedding: [1.0, 0.0, 0.0, 0.0]}), "
            "(:Item {id: 2, embedding: [0.0, 1.0, 0.0, 0.0]});"
        )

        conn.begin_transaction()
        conn.execute("CREATE (:Item {id: 3, embedding: [0.0, 0.0, 1.0, 0.0]});")
        conn.rollback()
        assert list(conn.execute("MATCH (n:Item) RETURN n.id ORDER BY n.id;")) == [
            [1],
            [2],
        ]

        capfd.readouterr()
        conn.execute(
            "CREATE INDEX item_hnsw ON Item USING HNSW (embedding) "
            "WITH (metric='l2', m=16, ef_construction=200);"
        )
        statistics_line = _duplicate_statistics_line(
            capfd.readouterr().err, "item_hnsw"
        )
        assert "0 / 2 (0%) duplicate vectors" in statistics_line
    finally:
        _close_database(db, conn)


def test_documented_schema_index_and_query_examples(tmp_path):
    """Exercise the DDL, DML, SHOW_INDEXES, brute-force and ANN doc examples."""
    db, conn = _open_database(tmp_path / "docs")
    try:
        conn.execute(
            "CREATE NODE TABLE vector_node("
            "id INT64, vec FLOAT[4], PRIMARY KEY (id));"
        )
        conn.execute(
            "CREATE NODE TABLE vector_node_with_default("
            "id INT64, vec FLOAT[4] DEFAULT [0.1, 0.1, 0.2, 0.2], "
            "PRIMARY KEY (id));"
        )
        conn.execute("CREATE (:vector_node_with_default {id: 1});")
        default_vector = list(
            conn.execute("MATCH (n:vector_node_with_default {id: 1}) RETURN n.vec;")
        )
        assert default_vector[0][0] == pytest.approx([0.1, 0.1, 0.2, 0.2], abs=1e-6)
        conn.execute("ALTER TABLE vector_node ADD IF NOT EXISTS vec2 FLOAT[4];")
        conn.execute("CREATE REL TABLE links(FROM vector_node TO vector_node);")
        conn.execute(
            "CREATE (:vector_node {id: 1, vec: [0.1, 0.2, 0.3, 0.4]}),"
            "(:vector_node {id: 2, vec: [0.2, 0.1, 0.1, 0.1]}),"
            "(:vector_node {id: 4, vec: [0.8, 0.8, 0.8, 0.8]});"
        )
        conn.execute(
            "MATCH (a:vector_node {id: 1}), (b:vector_node) "
            "WHERE b.id <> 1 CREATE (a)-[:links]->(b);"
        )

        brute_force = list(
            conn.execute(
                "MATCH (n:vector_node) RETURN n.id, "
                "vector_distance_l2(n.vec, [0.1, 0.2, 0.3, 0.4]) AS d "
                "ORDER BY d ASC;"
            )
        )
        assert brute_force[0] == pytest.approx([1, 0.0], abs=1e-6)

        conn.execute(
            "CREATE INDEX vec_hnsw_index IF NOT EXISTS ON vector_node "
            "USING HNSW (vec) WITH "
            "(metric = 'l2', m = 16, ef_construction = 200);"
        )
        indexes = list(conn.execute("CALL SHOW_INDEXES() RETURN *;"))
        assert any(row[0] == "vec_hnsw_index" for row in indexes)

        # The documented post-index insert must be visible to HNSW immediately.
        conn.execute("CREATE (:vector_node " "{id: 3, vec: [0.2, 0.2, 0.1, 0.1]});")
        conn.execute(
            "MATCH (a:vector_node {id: 1}), (b:vector_node {id: 3}) "
            "CREATE (a)-[:links]->(b);"
        )

        nearest = list(
            conn.execute(
                "MATCH (n:vector_node) RETURN n.id, "
                "vector_distance_l2(n.vec, [0.1, 0.2, 0.3, 0.4]) AS score "
                "ORDER BY score ASC LIMIT 3;"
            )
        )
        assert [row[0] for row in nearest] == [1, 3, 2]

        hybrid = list(
            conn.execute(
                "MATCH (n:vector_node) WITH n "
                "ORDER BY vector_distance_l2("
                "n.vec, [0.1, 0.2, 0.3, 0.4]) LIMIT 3 "
                "MATCH (n)-[e:links]->(n2) RETURN n.id, n2.id;"
            )
        )
        assert {tuple(row) for row in hybrid} == {(1, 2), (1, 3), (1, 4)}

        filtered = list(
            conn.execute(
                "MATCH (n:vector_node) WHERE n.id <> 1 RETURN n.id, "
                "vector_distance_l2(n.vec, [0.1, 0.2, 0.3, 0.4]) AS score "
                "ORDER BY score ASC LIMIT 3;"
            )
        )
        assert [row[0] for row in filtered] == [3, 2, 4]

        graph_filtered = list(
            conn.execute(
                "MATCH (n1:vector_node {id: 1})-[:links]->(n2:vector_node) "
                "RETURN n2.id, "
                "vector_distance_l2(n2.vec, [0.1, 0.2, 0.3, 0.4]) AS score "
                "ORDER BY score ASC LIMIT 3;"
            )
        )
        assert [row[0] for row in graph_filtered] == [3, 2, 4]

        # Missing fixed-length vectors use the implicit all-zero default and are
        # maintained by HNSW in the same way as explicitly supplied vectors.
        conn.execute("CREATE (:vector_node {id: 5});")
        implicit_default = list(
            conn.execute(
                "MATCH (n:vector_node {id: 5}) RETURN n.vec, "
                "vector_distance_l2(n.vec, [0.0, 0.0, 0.0, 0.0]);"
            )
        )
        assert implicit_default[0][0] == pytest.approx([0.0, 0.0, 0.0, 0.0])
        assert implicit_default[0][1] == pytest.approx(0.0)
        zero_nearest = list(
            conn.execute(
                "MATCH (n:vector_node) RETURN n.id, "
                "vector_distance_l2(n.vec, [0.0, 0.0, 0.0, 0.0]) AS score "
                "ORDER BY score ASC LIMIT 1;"
            )
        )
        assert zero_nearest[0] == pytest.approx([5, 0.0])

        conn.execute(
            "MATCH (n:vector_node) WHERE n.id = 1 " "SET n.vec = [0.2, 0.2, 0.1, 0.1];"
        )
        with pytest.raises(RuntimeError, match="Setting NULL for property vec"):
            conn.execute("MATCH (n:vector_node {id: 1}) SET n.vec = NULL;")
        after_failed_null = list(
            conn.execute(
                "MATCH (n:vector_node) RETURN n.id, "
                "vector_distance_l2(n.vec, [0.2, 0.2, 0.1, 0.1]) AS score "
                "ORDER BY score ASC LIMIT 2;"
            )
        )
        assert {row[0] for row in after_failed_null} == {1, 3}
        conn.execute("MATCH (n:vector_node) WHERE n.id = 2 DELETE n;")
        conn.execute("DROP INDEX vec_hnsw_index IF EXISTS;")
        conn.execute("DROP TABLE vector_node;")
    finally:
        _close_database(db, conn)


@pytest.mark.parametrize("file_format", ["csv", "json", "parquet"])
def test_documented_vector_import_examples(tmp_path, file_format):
    """Exercise the documented CSV, JSON, and Parquet import examples."""
    db, conn = _open_database(tmp_path / f"import-{file_format}")
    try:
        conn.execute(
            "CREATE NODE TABLE vector_node("
            "id INT64, vec FLOAT[4], PRIMARY KEY (id));"
        )
        if file_format == "csv":
            path = tmp_path / "vec.csv"
            path.write_text("id|vec\n1|[0.1, 0.2, 0.3, 0.4]\n2|[0.2, 0.1, 0.1, 0.1]\n")
        elif file_format == "json":
            path = tmp_path / "vec.json"
            path.write_text(
                json.dumps(
                    [
                        {"id": 1, "vec": [0.1, 0.2, 0.3, 0.4]},
                        {"id": 2, "vec": [0.2, 0.1, 0.1, 0.1]},
                    ]
                )
            )
        else:
            pa = pytest.importorskip("pyarrow")
            pq = pytest.importorskip("pyarrow.parquet")
            path = tmp_path / "vec.parquet"
            pq.write_table(
                pa.table(
                    {
                        "id": pa.array([1, 2], type=pa.int64()),
                        "vec": pa.array(
                            [[0.1, 0.2, 0.3, 0.4], [0.2, 0.1, 0.1, 0.1]],
                            type=pa.list_(pa.float32(), 4),
                        ),
                    }
                ),
                path,
            )
            conn.execute("LOAD parquet;")
        conn.execute(
            f"COPY vector_node FROM (LOAD FROM {cypher_path(path)} "
            "RETURN id, CAST(vec, 'FLOAT[4]') AS vec);"
        )
        assert list(conn.execute("MATCH (n:vector_node) RETURN count(n);"))[0][0] == 2
    finally:
        _close_database(db, conn)


def test_cosine_normalize_1024d_regression_and_incremental_writes(tmp_path):
    """Regression for #931: normalization must use the VecColumn storage."""
    dimension = 1024
    initial = [3.0, 4.0] + [0.0] * (dimension - 2)
    inserted = [5.0, 12.0] + [0.0] * (dimension - 2)
    db, conn = _open_database(tmp_path / "normalize-1024d", checkpoint_on_close=False)
    try:
        conn.execute(
            "CREATE NODE TABLE NormalizedItem("
            f"id INT64 PRIMARY KEY, embedding FLOAT[{dimension}]);"
        )
        conn.execute(
            "CREATE (:NormalizedItem {"
            f"id: 1, embedding: {_array_literal(initial)}}});"
        )
        conn.execute(
            "CREATE INDEX normalized_item_hnsw ON NormalizedItem USING HNSW "
            "(embedding) WITH (metric = 'cosine');"
        )

        first = list(
            conn.execute("MATCH (n:NormalizedItem {id: 1}) RETURN n.embedding;")
        )[0][0]
        assert first[:2] == pytest.approx([0.6, 0.8], abs=1e-6)

        conn.execute(
            "CREATE (:NormalizedItem {"
            f"id: 2, embedding: {_array_literal(inserted)}}});"
        )
        second = list(
            conn.execute("MATCH (n:NormalizedItem {id: 2}) RETURN n.embedding;")
        )[0][0]
        assert second[:2] == pytest.approx([5.0 / 13.0, 12.0 / 13.0], abs=1e-6)

        zero = [0.0] * dimension
        conn.execute(
            "CREATE (:NormalizedItem {" f"id: 3, embedding: {_array_literal(zero)}}});"
        )
        zero_distance = list(
            conn.execute(
                "MATCH (n:NormalizedItem {id: 3}) RETURN "
                f"vector_distance_cosine(n.embedding, {_array_literal(initial)});"
            )
        )[0][0]
        assert zero_distance == pytest.approx(1.0)

        result = conn.execute(
            "PROFILE MATCH (n:NormalizedItem) RETURN n.id, "
            f"vector_distance_cosine(n.embedding, {_array_literal(initial)}) "
            "AS score ORDER BY score ASC LIMIT 1;"
        )
        assert list(result)[0][0] == 1
        assert "IndexScanOpr" in _profile_operator_names(result)

        with pytest.raises(Exception):
            conn.execute("MATCH (n:NormalizedItem {id: 1}) SET n.embedding = NULL;")
    finally:
        _close_database(db, conn)


def test_issue_931_cosine_ann_returns_distance_with_normalize(tmp_path):
    """Regression for #931: ANN projection must return cosine distances."""
    dimension = 1024
    rng = random.Random(42)
    vec_a = [rng.uniform(0.0, 0.9) for _ in range(dimension)]
    vec_b = [rng.uniform(0.0, 0.9) for _ in range(dimension)]
    literal_a = _array_literal(vec_a)
    literal_b = _array_literal(vec_b)
    db, conn = _open_database(tmp_path / "issue-931", checkpoint_on_close=False)
    try:
        conn.execute(
            "CREATE NODE TABLE Issue931Node("
            f"uuid STRING PRIMARY KEY, embedding FLOAT[{dimension}]);"
        )
        conn.execute(
            "CREATE INDEX issue_931_hnsw ON Issue931Node USING HNSW "
            "(embedding) WITH (metric = 'cosine', cosine_normalize = true);"
        )
        conn.execute(
            "MERGE (n:Issue931Node {uuid: 'a'}) "
            f"SET n.embedding = CAST({literal_a}, 'FLOAT[{dimension}]');"
        )
        conn.execute(
            "MERGE (n:Issue931Node {uuid: 'b'}) "
            f"SET n.embedding = CAST({literal_b}, 'FLOAT[{dimension}]');"
        )

        base = (
            "MATCH (n:Issue931Node) WITH n, "
            f"vector_distance_cosine(n.embedding, {literal_a}) AS dist "
        )
        rows_without_limit = list(
            conn.execute(base + "RETURN n.uuid, dist ORDER BY dist ASC;")
        )
        tight_result = conn.execute(
            "PROFILE " + base + "ORDER BY dist ASC LIMIT 10 RETURN n.uuid, dist;"
        )
        rows_tight = list(tight_result)
        rows_extra_with = list(
            conn.execute(
                base + "WITH n, dist ORDER BY dist ASC LIMIT 10 " "RETURN n.uuid, dist;"
            )
        )

        assert "IndexScanOpr" in _profile_operator_names(tight_result)
        assert [row[0] for row in rows_tight] == ["a", "b"]
        assert rows_tight[0][1] == pytest.approx(0.0, abs=1e-6)
        # FP32 cosine distances can round slightly outside the theoretical range.
        assert all(-1e-6 <= row[1] <= 2.0 + 1e-6 for row in rows_tight), rows_tight
        assert [row[0] for row in rows_tight] == [row[0] for row in rows_without_limit]
        assert [row[1] for row in rows_tight] == pytest.approx(
            [row[1] for row in rows_without_limit], abs=1e-6
        )
        assert [row[0] for row in rows_tight] == [row[0] for row in rows_extra_with]
        assert [row[1] for row in rows_tight] == pytest.approx(
            [row[1] for row in rows_extra_with], abs=1e-6
        )
    finally:
        _close_database(db, conn)


@pytest.mark.parametrize("metric", ["l2", "ip"])
def test_cosine_normalize_is_ignored_for_non_cosine_metrics(tmp_path, metric):
    db, conn = _open_database(
        tmp_path / f"cosine-normalize-ignored-{metric}", checkpoint_on_close=False
    )
    try:
        conn.execute(
            "CREATE NODE TABLE RawVectorItem("
            "id INT64 PRIMARY KEY, embedding FLOAT[4]);"
        )
        conn.execute(
            "CREATE (:RawVectorItem {" "id: 1, embedding: [3.0, 4.0, 0.0, 0.0]});"
        )
        conn.execute(
            "CREATE INDEX raw_vector_hnsw ON RawVectorItem USING HNSW "
            "(embedding) WITH ("
            f"metric = '{metric}', cosine_normalize = true);"
        )

        stored = list(
            conn.execute("MATCH (n:RawVectorItem {id: 1}) RETURN n.embedding;")
        )[0][0]
        assert stored == pytest.approx([3.0, 4.0, 0.0, 0.0])
    finally:
        _close_database(db, conn)


def test_cosine_normalize_false_preserves_valid_unit_vectors(tmp_path):
    path = tmp_path / "cosine-normalize-disabled"
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        conn.execute(
            "CREATE NODE TABLE UnitVectorItem("
            "id INT64 PRIMARY KEY, embedding FLOAT[4]);"
        )
        conn.execute(
            "CREATE (:UnitVectorItem {" "id: 1, embedding: [0.6, 0.8, 0.0, 0.0]});"
        )
        conn.execute(
            "CREATE INDEX unit_vector_hnsw ON UnitVectorItem USING HNSW "
            "(embedding) WITH ("
            "metric = 'cosine', cosine_normalize = false);"
        )

        stored = list(
            conn.execute("MATCH (n:UnitVectorItem {id: 1}) RETURN n.embedding;")
        )[0][0]
        assert stored == pytest.approx([0.6, 0.8, 0.0, 0.0])
        conn.execute("CHECKPOINT;")
        assert _local_buffer_checkpoint(path, "unit_vector_hnsw")[0] == 1
        conn.execute("CREATE (:UnitVectorItem {id: 3, embedding: [0.0,1.0,0.0,0.0]});")
        conn.execute("CHECKPOINT;")
        assert _local_buffer_checkpoint(path, "unit_vector_hnsw")[0] == 2
        _close_database(db, conn)
        db, conn = _open_database(path, checkpoint_on_close=False)
        result = conn.execute(
            "PROFILE MATCH (n:UnitVectorItem) RETURN n.id, "
            "vector_distance_cosine(n.embedding, [0.0,7.0,0.0,0.0]) AS score "
            "ORDER BY score LIMIT 2;"
        )
        rows = list(result)
        assert [row[0] for row in rows] == [3, 1]
        assert [row[1] for row in rows] == pytest.approx([0.0, 0.2], abs=1e-5)
        assert "IndexScanOpr" in _profile_operator_names(result)
    finally:
        _close_database(db, conn)


@pytest.mark.parametrize("checkpoint_mode", ["none", "manual", "bulk", "close"])
@pytest.mark.parametrize("fail_statement", [False, True])
def test_hnsw_abort_checkpoint_reuses_consistent_index_state(
    tmp_path, checkpoint_mode, fail_statement
):
    """Clean tables and indexes must reuse the same committed checkpoint state."""
    path = tmp_path / "abort-next-index-id"
    db, conn = _open_database(path, checkpoint_on_close=checkpoint_mode == "close")
    try:
        conn.execute("CREATE NODE TABLE Other(id INT64 PRIMARY KEY);")
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[4]);")
        conn.execute("CREATE (:Item {id: 0, v: [1.0,0.0,0.0,0.0]});")
        # Disable LocalBuffer so the aborted write actually reaches HNSW.
        conn.execute(
            "CREATE INDEX item_hnsw ON Item USING HNSW(v) "
            "WITH(metric='cosine', cosine_normalize=false);"
        )
        conn.execute("CHECKPOINT;")
        original_index = _local_buffer_checkpoint(path, "item_hnsw")
        conn.begin_transaction()
        conn.execute("CREATE (:Item {id: 1, v: [0.0,1.0,0.0,0.0]});")
        assert list(conn.execute("MATCH (n:Item) RETURN count(n);")) == [[2]]
        # A separate failing statement aborts the transaction after ID 1 was
        # successfully added to the shared graph. Rollback clears its state.
        if fail_statement:
            with pytest.raises(RuntimeError, match="already exists"):
                conn.execute("CREATE (:Item {id: 0, v: [1.0,0.0,0.0,0.0]});")
            with pytest.raises(RuntimeError, match="rollback-only"):
                conn.commit()
        conn.rollback()
        assert list(conn.execute("MATCH (n:Item) RETURN n.id;")) == [[0]]

        persisted_next_id = None
        if checkpoint_mode != "none":
            # Both clean modules reuse the committed state. The aborted
            # physical node must not enter the new checkpoint.
            if checkpoint_mode == "manual":
                conn.execute("CHECKPOINT;")
            elif checkpoint_mode == "bulk":
                csv = tmp_path / "other.csv"
                csv.write_text("id\n42\n")
                conn.execute(f"COPY Other FROM {cypher_path(csv)};")
            else:
                _close_database(db, conn)
                db, conn = _open_database(path, checkpoint_on_close=False)
            checkpoint = path / "checkpoint"
            current = (checkpoint / "CURRENT").read_text().strip()
            manifest = json.loads(
                (checkpoint / "manifests" / (current + ".manifest")).read_text()
            )
            assert _local_buffer_checkpoint(path, "item_hnsw") == original_index
            persisted_next_id = int(
                manifest["modules"]["vertex_Item_prop_0/offset_accessor"]["extra"][
                    "next_index_id"
                ]
            )
        try:
            conn.execute("CREATE (:Item {id: 2, v: [0.0,0.0,1.0,0.0]});")
        except RuntimeError as error:
            pytest.fail(
                "Append after abort failed: "
                f"checkpoint={checkpoint_mode}, "
                f"persisted next_index_id={persisted_next_id} (expected 1 for reused checkpoint). {error}"
            )
        if checkpoint_mode != "none":
            assert persisted_next_id == 1
        assert list(conn.execute("MATCH (n:Item) RETURN n.id ORDER BY n.id;")) == [
            [0],
            [2],
        ]
        result = conn.execute(
            "PROFILE MATCH (n:Item) RETURN n.id, "
            "vector_distance_cosine(n.v, [0.0,0.0,1.0,0.0]) AS score "
            "ORDER BY score LIMIT 1;"
        )
        assert list(result) == [[2, 0.0]]
        assert "IndexScanOpr" in _profile_operator_names(result)
    finally:
        _close_database(db, conn)


def test_cosine_normalize_rejects_property_used_by_raw_hnsw_index(tmp_path):
    """A normalized representation cannot replace data used by a raw index."""
    db, conn = _open_database(
        tmp_path / "normalize-index-conflict", checkpoint_on_close=False
    )
    try:
        conn.execute(
            "CREATE NODE TABLE ConflictingIndexItem("
            "id INT64 PRIMARY KEY, embedding FLOAT[4]);"
        )
        conn.execute(
            "CREATE (:ConflictingIndexItem {"
            "id: 1, embedding: [3.0, 4.0, 0.0, 0.0]});"
        )
        conn.execute(
            "CREATE INDEX raw_embedding_hnsw ON ConflictingIndexItem "
            "USING HNSW (embedding) WITH (metric = 'l2');"
        )

        with pytest.raises(
            RuntimeError,
            match="Cannot normalize a vector property used by an existing HNSW index",
        ):
            conn.execute(
                "CREATE INDEX normalized_embedding_hnsw ON ConflictingIndexItem "
                "USING HNSW (embedding) "
                "WITH (metric = 'cosine', cosine_normalize = true);"
            )

        stored = list(
            conn.execute("MATCH (n:ConflictingIndexItem {id: 1}) RETURN n.embedding;")
        )[0][0]
        assert stored == pytest.approx([3.0, 4.0, 0.0, 0.0])

        raw_result = conn.execute(
            "PROFILE MATCH (n:ConflictingIndexItem) RETURN n.id, "
            "vector_distance_l2(n.embedding, [3.0, 4.0, 0.0, 0.0]) "
            "AS score ORDER BY score ASC LIMIT 1;"
        )
        raw_rows = list(raw_result)
        assert raw_rows[0][0] == 1
        assert raw_rows[0][1] == pytest.approx(0.0)
        assert "IndexScanOpr" in _profile_operator_names(raw_result)
    finally:
        _close_database(db, conn)


def test_index_persistence_after_checkpoint_and_reopen(tmp_path):
    db_path = tmp_path / "database"
    db, conn = _open_database(db_path, checkpoint_on_close=False)
    _create_advanced_data(conn)
    conn.execute(
        "MATCH (n:Item {id: 3}) "
        f"SET n.l2_vec = {_array_literal(_constant_vector(700.25))};"
    )
    conn.execute("CHECKPOINT;")
    _close_database(db, conn)

    db, conn = _open_database(db_path, checkpoint_on_close=False)
    try:
        assert _l2_search(conn, 700.25, 1)[0] == pytest.approx([3, 0.0], abs=1e-6)
    finally:
        _close_database(db, conn)


def test_vector_update_slots_do_not_mask_property_column_growth(tmp_path):
    db, conn = _open_database(
        tmp_path / "vector-column-growth", checkpoint_on_close=False
    )
    vector = _array_literal([0.1] * 32)
    try:
        conn.execute(
            "CREATE NODE TABLE T("
            "id INT64 PRIMARY KEY, vector FLOAT[32], text VARCHAR(65535));"
        )
        conn.execute(
            "CREATE INDEX t_hnsw ON T USING HNSW (vector) " "WITH (metric = 'cosine');"
        )

        # Fill the initial VID capacity in batches, then update a vector so its
        # versioned index-ID slots grow beyond the row-addressed text column.
        for start in range(0, 4096, 128):
            nodes = [
                f"(:T {{id: {node_id}, vector: {vector}, text: 'created'}})"
                for node_id in range(start, start + 128)
            ]
            conn.execute("CREATE " + ",".join(nodes) + ";")
        conn.execute(
            f"MATCH (n:T {{id: 0}}) SET n.vector = {vector}, n.text = 'updated';"
        )

        # Growing past 4096 must resize the text column even though the vector
        # column already reports a larger size due to the update above.
        conn.execute(f"CREATE (:T {{id: 4096, vector: {vector}, text: 'boundary'}});")
        assert list(conn.execute("MATCH (n:T) RETURN count(n);")) == [[4097]]
        assert list(conn.execute("MATCH (n:T {id: 4096}) RETURN n.text;")) == [
            ["boundary"]
        ]
    finally:
        _close_database(db, conn)


def test_index_persistence_after_process_restart(tmp_path):
    db_path = tmp_path / "database"
    create_script = textwrap.dedent(
        f"""
        from neug import Database
        db = Database({str(db_path)!r}, mode="w")
        conn = db.connect()
        conn.execute("LOAD vector_search;")
        conn.execute("CREATE NODE TABLE Item(id STRING PRIMARY KEY, embedding FLOAT[4]);")
        conn.execute("CREATE (:Item {{id: 'a', embedding: [1.0, 0.0, 0.0, 0.0]}});")
        conn.execute("CREATE INDEX item_embedding_hnsw ON Item USING HNSW (embedding) WITH (metric = 'ip');")
        conn.close()
        db.close()
        """
    )
    subprocess.run([sys.executable, "-c", create_script], check=True)

    reopen_script = textwrap.dedent(
        f"""
        from neug import Database

        db = Database({str(db_path)!r}, mode="w", checkpoint_on_close=False)
        conn = db.connect()
        conn.execute("LOAD vector_search;")
        rows = list(
            conn.execute(
                "MATCH (n:Item) RETURN n.id, "
                "vector_distance_ip(n.embedding, [1.0, 0.0, 0.0, 0.0]) "
                "AS score ORDER BY score DESC LIMIT 1;"
            )
        )
        assert rows == [["a", 1.0]], rows
        conn.close()
        db.close()
        """
    )
    subprocess.run([sys.executable, "-c", reopen_script], check=True)


def test_auto_load_extension_restores_index_in_new_process(tmp_path):
    """Persist a manually loaded index, then query it in a fresh interpreter."""
    db_path = tmp_path / "auto-load-database"
    csv_path = tmp_path / "vectors.csv"
    csv_path.write_text(
        "id|embedding\n"
        "1|[1.0, 0.0, 0.0, 0.0]\n"
        "2|[0.0, 1.0, 0.0, 0.0]\n"
        "3|[0.0, 0.0, 1.0, 0.0]\n"
    )
    creator_pid = os.getpid()
    db = Database(str(db_path), mode="w", auto_load_extension=False)
    conn = db.connect()
    try:
        conn.execute("LOAD EXTENSION vector_search;")
        conn.execute(
            "CREATE NODE TABLE Item(id INT64 PRIMARY KEY, embedding FLOAT[4]);"
        )
        conn.execute(
            "CREATE INDEX item_embedding_hnsw ON Item USING HNSW (embedding) "
            "WITH (metric = 'l2');"
        )
        conn.execute(
            f"COPY Item FROM (LOAD FROM {cypher_path(csv_path)} "
            "RETURN id, CAST(embedding, 'FLOAT[4]') AS embedding);"
        )
        assert list(conn.execute("MATCH (n:Item) RETURN count(n);")) == [[3]]
    finally:
        _close_database(db, conn)

    # subprocess starts a new interpreter, so it cannot reuse this process's
    # loaded libraries or module registrations. The parent stays alive, making
    # the explicit PID comparison immune to PID reuse.
    script = textwrap.dedent(
        f"""
        import os
        from neug import Database

        assert os.getpid() != {creator_pid}, "Query must run in another process"
        db = Database({str(db_path)!r}, mode="r", auto_load_extension=True)
        conn = db.connect()
        try:
            # No LOAD here: opening the database must load the extension.
            result = conn.execute(
                "PROFILE MATCH (n:Item) RETURN n.id, "
                "vector_distance_l2(n.embedding, [1.0, 0.0, 0.0, 0.0]) "
                "AS distance ORDER BY distance ASC LIMIT 1;"
            )
            rows = list(result)
            assert rows == [[1, 0.0]], rows
            operators = result.get_profile_metrics()["operators"]
            assert "IndexScanOpr" in [op["operator_name"] for op in operators], operators
        finally:
            conn.close()
            db.close()
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("metric", ["l2", "ip", "cosine"])
@pytest.mark.parametrize("layout", ["flat", "hnsw", "mixed"])
def test_visible_bitmap_candidates_and_maintenance(tmp_path, metric, layout):
    """Dense/sparse candidates survive slot replacement, abort and reopen."""
    path = tmp_path / "bitmap-candidates"
    db, conn = _open_database(path, checkpoint_on_close=False)
    rng = random.Random(991)
    vectors = [[rng.uniform(-1, 1) for _ in range(4)] for _ in range(201)]
    direction = "DESC" if metric == "ip" else "ASC"

    def insert(i):
        conn.execute(
            f"CREATE (:Item {{id:{i}, bucket:{i % 100}, "
            f"v:{_array_literal(vectors[i])}}});"
        )

    def check():
        # On each 100-ID branch these exercise below, at, and above 10%.
        for predicate in (
            "",
            "WHERE n.bucket<9 ",
            "WHERE n.bucket<10 ",
            "WHERE n.bucket<11 ",
            "WHERE n.bucket<0 ",
        ):
            body = f"MATCH (n:Item) {predicate}RETURN n.id, "
            distance = f"vector_distance_{metric}(n.v, $q)"
            suffix = f" AS score ORDER BY score {direction} LIMIT 20;"
            params = {"q": vectors[200]}
            result = conn.execute(
                "PROFILE " + body + distance + suffix, parameters=params
            )
            actual = list(result)
            assert "IndexScanOpr" in _profile_operator_names(result)
            expected = list(
                conn.execute(body + distance + " + 0.0" + suffix, parameters=params)
            )
            assert [r[0] for r in actual] == [r[0] for r in expected]
            assert [r[1] for r in actual] == pytest.approx(
                [r[1] for r in expected], abs=2e-5
            )

    try:
        conn.execute(
            "CREATE NODE TABLE Item(id INT64 PRIMARY KEY, bucket INT64, v FLOAT[4]);"
        )
        if layout != "flat":
            for i in range(100):
                insert(i)
        conn.execute(
            "CREATE INDEX item_hnsw ON Item USING HNSW(v) "
            f"WITH(metric='{metric}',cosine_normalize=true,m=8,ef_construction=40);"
        )
        if layout == "flat":
            for i in range(100):
                insert(i)
        elif layout == "mixed":
            for i in range(100, 200):
                insert(i)
        check()
        conn.execute(f"MATCH(n:Item {{id:0}}) SET n.v={_array_literal(vectors[200])};")
        conn.execute("MATCH(n:Item) WHERE n.id=1 OR n.id=104 DELETE n;")
        check()
        conn.begin_transaction()
        conn.execute("MATCH(n:Item {id:2}) DELETE n;")
        conn.execute(f"MATCH(n:Item {{id:3}}) SET n.v={_array_literal(vectors[199])};")
        check()
        conn.execute("CALL optimize_indexes('hnsw', 'item_hnsw');")
        check()
        conn.rollback()
        check()
        # Physical HNSW additions survive abort; logical bitmaps must not.
        conn.execute("CALL optimize_indexes('hnsw', 'item_hnsw');")
        check()
        conn.execute("MATCH(n:Item) WHERE n.id>=5 DELETE n;")
        check()  # Sparse visibility alone also enables candidate brute force.
        conn.execute("CHECKPOINT;")
    finally:
        _close_database(db, conn)
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        check()
        insert(200)
        check()
    finally:
        _close_database(db, conn)


def test_incremental_bitmaps_shared_column_and_aborted_slots(tmp_path):
    """Each index consumes shared deletion records independently across snapshots."""
    path = tmp_path / "shared-column-bitmaps"
    db, conn = _open_database(path, checkpoint_on_close=False)

    def check():
        for metric, order in (("l2", "ASC"), ("ip", "DESC")):
            for predicate in ("", "WHERE n.id<3 "):
                prefix = f"MATCH(n:Item) {predicate}RETURN n.id, "
                distance = f"vector_distance_{metric}(n.v, [1.0,0.0,0.0,0.0])"
                suffix = f" AS score ORDER BY score {order} LIMIT 10;"
                result = conn.execute("PROFILE " + prefix + distance + suffix)
                actual = list(result)
                assert "IndexScanOpr" in _profile_operator_names(result)
                expected = list(conn.execute(prefix + distance + " + 0.0" + suffix))
                assert actual == expected

    try:
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[4]);")
        for metric in ("l2", "ip"):
            conn.execute(
                f"CREATE INDEX idx_{metric} ON Item USING HNSW(v) "
                f"WITH(metric='{metric}',m=8,ef_construction=40);"
            )
        for i in range(30):
            conn.execute(f"CREATE (:Item {{id:{i},v:[{float(i)},0.0,0.0,0.0]}});")
        conn.execute("CALL optimize_indexes('hnsw');")
        conn.begin_transaction()
        conn.execute("MATCH(n:Item {id:0}) SET n.v=[100.0,0.0,0.0,0.0];")
        conn.execute("CREATE (:Item {id:30,v:[101.0,0.0,0.0,0.0]});")
        conn.execute("MATCH(n:Item {id:1}) DELETE n;")
        check()
        conn.execute("CALL optimize_indexes('hnsw');")
        conn.rollback()
        check()
        conn.execute("MATCH(n:Item {id:0}) SET n.v=[102.0,0.0,0.0,0.0];")
        conn.execute("MATCH(n:Item {id:1}) DELETE n;")
        conn.execute("CREATE (:Item {id:31,v:[103.0,0.0,0.0,0.0]});")
        check()
        # Several invalidations in one statement must all reach both indexes.
        conn.execute("MATCH(n:Item) WHERE n.id>=20 SET n.v=[200.0,0.0,0.0,0.0];")
        conn.execute("MATCH(n:Item) WHERE n.id>=25 DELETE n;")
        check()
        conn.execute("CALL optimize_indexes('hnsw');")
        check()
        conn.execute("CHECKPOINT;")
    finally:
        _close_database(db, conn)
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        check()
    finally:
        _close_database(db, conn)


@pytest.mark.parametrize("mixed", [False, True])
def test_cosine_shared_normalized_query_with_zero_and_scaled_inputs(tmp_path, mixed):
    db, conn = _open_database(tmp_path / "shared-query", checkpoint_on_close=False)
    rng = random.Random(1151)
    vectors = [[rng.uniform(-1, 1) for _ in range(16)] for _ in range(1101)]
    try:
        conn.execute(
            "CREATE NODE TABLE SharedQuery(id INT64 PRIMARY KEY, embedding FLOAT[16]);"
        )
        for i, vector in enumerate(vectors):
            if mixed and i == 1001:
                conn.execute(
                    "CREATE INDEX shared_query ON SharedQuery USING HNSW "
                    "(embedding) WITH(metric='cosine');"
                )
            conn.execute(
                "CREATE (:SharedQuery {id:$id, embedding:CAST($v,'FLOAT[16]')});",
                parameters={"id": i, "v": vector},
            )
        if not mixed:
            conn.execute(
                "CREATE INDEX shared_query ON SharedQuery USING HNSW "
                "(embedding) WITH(metric='cosine');"
            )
        for source in [0, 1050, None, 0]:
            target = (
                [value * 17.0 for value in vectors[source]]
                if source is not None
                else [0.0] * 16
            )
            result = conn.execute(
                "PROFILE MATCH(n:SharedQuery) RETURN n.id, "
                "vector_distance_cosine(n.embedding,$v) AS score "
                "ORDER BY score LIMIT 10;",
                parameters={"v": target},
            )
            rows = list(result)
            assert "IndexScanOpr" in _profile_operator_names(result)
            assert len(rows) == 10
            if source is not None:
                assert rows[0][0] == source
            for identifier, score in rows:
                vector = vectors[identifier]
                expected = 1.0
                if source is not None:
                    expected -= sum(a * b for a, b in zip(vector, target)) / (
                        math.sqrt(sum(a * a for a in vector))
                        * math.sqrt(sum(b * b for b in target))
                    )
                assert score == pytest.approx(expected, abs=1e-5)
    finally:
        _close_database(db, conn)


def test_show_indexes_buffered_count_transaction_and_reopen(tmp_path):
    path = tmp_path / "optimize-stats"
    db, conn = _open_database(path, checkpoint_on_close=False)

    def pending():
        return list(
            conn.execute("CALL show_indexes('hnsw') RETURN type, name, buffered_count;")
        )

    try:
        assert pending() == []
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[4]);")
        conn.execute("CREATE INDEX item_hnsw ON Item USING HNSW(v);")
        conn.execute("CREATE (:Item {id:0,v:[1.0,0.0,0.0,0.0]});")
        conn.execute("CALL optimize_indexes('hnsw');")
        assert pending() == [["hnsw", "item_hnsw", 0]]
        row = list(conn.execute("CALL show_indexes('hnsw', 'item_hnsw');"))[0]
        assert row[:4] == ["item_hnsw", "hnsw", "Item", "v"]
        assert row[5:] == ["active", 0]
        for removed in ("index_optimize", "show_index_optimize_stats"):
            with pytest.raises(RuntimeError):
                conn.execute(f"CALL {removed}();")
        conn.begin_transaction()
        conn.execute("MATCH(n:Item {id:0}) SET n.v=[2.0,0.0,0.0,0.0];")
        conn.execute("CREATE (:Item {id:1,v:[3.0,0.0,0.0,0.0]});")
        assert pending() == [["hnsw", "item_hnsw", 2]]
        conn.execute("MATCH(n:Item {id:1}) DELETE n;")
        assert pending() == [["hnsw", "item_hnsw", 2]]
        conn.execute("CALL optimize_indexes('hnsw');")
        assert pending() == [["hnsw", "item_hnsw", 0]]
        conn.rollback()
        assert pending() == [["hnsw", "item_hnsw", 0]]
        conn.execute("CREATE (:Item {id:2,v:[4.0,0.0,0.0,0.0]});")
        assert pending() == [["hnsw", "item_hnsw", 3]]
        conn.execute("CHECKPOINT;")
    finally:
        _close_database(db, conn)
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        conn.begin_transaction(read_only=True)
        assert pending() == [["hnsw", "item_hnsw", 3]]
        conn.commit()
    finally:
        _close_database(db, conn)


def test_local_buffer_checkpoint_binary_timestamp(tmp_path):
    # 26 (0x1A) in the timestamp metadata must not be treated as Windows EOF.
    path = tmp_path / "binary-timestamp"
    db, conn = _open_database(path, checkpoint_on_close=False)
    try:
        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, v FLOAT[4]);")
        conn.execute("CREATE INDEX item_hnsw ON Item USING HNSW(v);")
        for i in range(26):
            conn.execute(
                f"CREATE (:Item {{id:{i}, v:{_array_literal([float(i), 0.0, 0.0, 0.0])}}});"
            )
        conn.execute("CHECKPOINT;")
        assert list(conn.execute("MATCH(n:Item) RETURN count(n);")) == [[26]]
        _close_database(db, conn)
        db, conn = _open_database(path, checkpoint_on_close=False)
        assert list(conn.execute("MATCH(n:Item) RETURN count(n);")) == [[26]]
        assert list(conn.execute("CALL show_indexes() RETURN buffered_count;")) == [
            [26]
        ]
    finally:
        _close_database(db, conn)
