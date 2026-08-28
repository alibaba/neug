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
import math
import os
import random
import subprocess
import sys
import textwrap

import pytest

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
    rows = list(
        advanced_connection.execute(
            "MATCH (source:Item)-[:NEXT]->(n:Item) RETURN n.id, "
            f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
            "AS score ORDER BY score ASC LIMIT 3;"
        )
    )
    assert rows[0][0] == 500
    assert {row[0] for row in rows[1:3]} == {499, 501}

    empty_result = advanced_connection.execute(
        "PROFILE MATCH (n:Item) WHERE n.group_id = 99 RETURN n.id, "
        f"vector_distance_l2(n.l2_vec, {_array_literal(_constant_vector(500.1))}) "
        "AS score ORDER BY score ASC LIMIT 3;"
    )
    assert list(empty_result) == []
    assert "IndexScanOpr" in _profile_operator_names(empty_result)


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
            f'COPY vector_node FROM (LOAD FROM "{path}" '
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
        assert all(0.0 <= row[1] <= 2.0 for row in rows_tight)
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
    db, conn = _open_database(
        tmp_path / "cosine-normalize-disabled", checkpoint_on_close=False
    )
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
