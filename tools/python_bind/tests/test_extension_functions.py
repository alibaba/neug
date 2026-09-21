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

"""Mini functional tests for NeuG extensions.

These tests are gated by the environment variable NEUG_RUN_EXTENSION_TESTS.
Each test creates a fresh temporary database, installs/loads the extension,
and exercises a small piece of functionality declared by that extension.

For pure install/load coverage see tests/test_extension_install_load.py.
"""

import os

import pytest

from neug.database import Database

_EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)


skip_if_extension_tests_disabled = pytest.mark.skipif(
    not _EXTENSION_TESTS_ENABLED,
    reason="Extension tests disabled by default; set NEUG_RUN_EXTENSION_TESTS=1.",
)


def _load_extension(conn, ext_name):
    conn.execute(f"INSTALL {ext_name}")
    conn.execute(f"LOAD EXTENSION {ext_name}")


def _assert_extension_loaded(conn, ext_name):
    result = conn.execute("CALL SHOW_LOADED_EXTENSIONS() RETURN *")
    names = [row[0].upper() for row in result if row[0]]
    assert ext_name.upper() in names


@skip_if_extension_tests_disabled
def test_extension_parquet(tmp_path):
    """Mini test for parquet extension: round-trip via COPY TO / LOAD FROM."""
    db_dir = str(tmp_path / "test_extension_parquet")
    db = Database(db_dir, "w")
    conn = db.connect()
    try:
        _load_extension(conn, "parquet")
        _assert_extension_loaded(conn, "parquet")

        conn.execute(
            "CREATE NODE TABLE person(id INT64 PRIMARY KEY, name STRING, age INT64);"
        )
        conn.execute("CREATE (:person {id: 1, name: 'Alice', age: 30});")
        conn.execute("CREATE (:person {id: 2, name: 'Bob', age: 25});")

        parquet_path = os.path.join(db_dir, "person.parquet")
        conn.execute(
            f'COPY (MATCH (p:person) RETURN p.id, p.name, p.age) TO "{parquet_path}";'
        )
        rows = list(conn.execute(f'LOAD FROM "{parquet_path}" RETURN *;'))
        assert len(rows) == 2
    finally:
        conn.close()
        db.close()


@skip_if_extension_tests_disabled
def test_extension_httpfs(tmp_path):
    """Mini test for httpfs extension: only verify INSTALL/LOAD, no real access."""
    db_dir = str(tmp_path / "test_extension_httpfs")
    db = Database(db_dir, "w")
    conn = db.connect()
    try:
        _load_extension(conn, "httpfs")
        # httpfs is a VFS-layer extension; LOAD succeeding is sufficient.
    finally:
        conn.close()
        db.close()


@skip_if_extension_tests_disabled
def test_extension_pattern_matching(tmp_path):
    """Mini test for pattern_matching extension: CALL PATTERN_MATCH."""
    db_dir = str(tmp_path / "test_extension_pattern_matching")
    db = Database(db_dir, "w")
    conn = db.connect()
    try:
        _load_extension(conn, "pattern_matching")
        _assert_extension_loaded(conn, "pattern_matching")

        conn.execute(
            "CREATE NODE TABLE Person("
            "id INT32 PRIMARY KEY, name STRING, age INT32, city STRING);"
        )
        conn.execute(
            "CREATE REL TABLE person_knows_person(FROM Person TO Person, weight DOUBLE);"
        )
        conn.execute("CREATE (:Person {id: 0, name: 'Alice', age: 20, city: 'NYC'});")
        conn.execute("CREATE (:Person {id: 1, name: 'Bob', age: 30, city: 'LA'});")
        conn.execute(
            "MATCH (a:Person {id: 0}), (b:Person {id: 1}) "
            "CREATE (a)-[:person_knows_person {weight: 1.0}]->(b);"
        )

        rows = list(
            conn.execute(
                "CALL PATTERN_MATCH('(a:Person)-[r:person_knows_person]->(b:Person)') "
                "RETURN *;"
            )
        )
        assert len(rows) == 1
    finally:
        conn.close()
        db.close()


@skip_if_extension_tests_disabled
def test_extension_vector_search(tmp_path):
    """Mini test for vector_search extension: HNSW index and vector distance."""
    db_dir = str(tmp_path / "test_extension_vector_search")
    db = Database(db_dir, "w")
    conn = db.connect()
    try:
        _load_extension(conn, "vector_search")
        _assert_extension_loaded(conn, "vector_search")

        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, vec FLOAT[4]);")
        conn.execute("CREATE (:Item {id: 1, vec: [1.0, 0.0, 0.0, 0.0]});")
        conn.execute("CREATE (:Item {id: 2, vec: [0.0, 1.0, 0.0, 0.0]});")
        conn.execute(
            "CREATE INDEX item_vec_hnsw ON Item USING HNSW (vec) "
            "WITH (metric = 'l2', m = 16, ef_construction = 200);"
        )
        rows = list(
            conn.execute(
                "MATCH (n:Item) RETURN n.id, vector_distance_l2(n.vec, [1.0, 0.0, 0.0, 0.0]) "
                "ORDER BY n.id;"
            )
        )
        assert len(rows) == 2
        assert rows[0][0] == 1
    finally:
        conn.close()
        db.close()


@skip_if_extension_tests_disabled
def test_extension_fts(tmp_path):
    """Mini test for fts extension: FTS index and bm25 search."""
    db_dir = str(tmp_path / "test_extension_fts")
    db = Database(db_dir, "w")
    conn = db.connect()
    try:
        _load_extension(conn, "fts")
        _assert_extension_loaded(conn, "fts")

        conn.execute("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, text STRING);")
        conn.execute("CREATE (:Item {id: 1, text: 'hello world'});")
        conn.execute("CREATE (:Item {id: 2, text: 'foo bar'});")
        conn.execute("CREATE INDEX item_text_fts ON Item USING FTS (text);")
        rows = list(
            conn.execute(
                "MATCH (n:Item) RETURN n.id, bm25(n.text, 'hello') AS score "
                "ORDER BY score ASC LIMIT 10;"
            )
        )
        assert len(rows) >= 1
    finally:
        conn.close()
        db.close()


@skip_if_extension_tests_disabled
def test_extension_gds(tmp_path):
    """Mini test for gds extension: project_graph and page_rank."""
    db_dir = str(tmp_path / "test_extension_gds")
    db = Database(db_dir, "w")
    conn = db.connect()
    try:
        _load_extension(conn, "gds")
        _assert_extension_loaded(conn, "gds")

        conn.execute("CREATE NODE TABLE Person(id INT64 PRIMARY KEY, name STRING);")
        conn.execute("CREATE REL TABLE knows(FROM Person TO Person, weight DOUBLE);")
        conn.execute("CREATE (:Person {id: 1, name: 'Alice'});")
        conn.execute("CREATE (:Person {id: 2, name: 'Bob'});")
        conn.execute(
            "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
            "CREATE (a)-[:knows {weight: 1.0}]->(b);"
        )
        conn.execute(
            "CALL project_graph('g', ['Person'], {'[Person, knows, Person]': ''});"
        )
        rows = list(
            conn.execute(
                "CALL page_rank('g', {max_iterations: 10}) "
                "YIELD node, rank RETURN node.id, rank;"
            )
        )
        assert len(rows) == 2
    finally:
        conn.close()
        db.close()
