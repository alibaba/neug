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
import time
import unittest

from neug.database import Database

logger = logging.getLogger(__name__)


class TestBachLoading(unittest.TestCase):
    """
    Test running alter property query on a graph that is already created and loaded
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_alter_properties(self):
        # create a tmp directory for the graph
        db_dir = "/tmp/test_batch_loading"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        # get env : FLEX_DATA_DIR
        flex_data_dir = os.environ.get("FLEX_DATA_DIR")
        if not flex_data_dir:
            raise Exception("FLEX_DATA_DIR is not set")
        person_csv = os.path.join(flex_data_dir, "person.csv")
        person_knows_person_csv = os.path.join(flex_data_dir, "person_knows_person.csv")

        db = Database(db_dir, "w")
        conn = db.connect()
        # First create the graph schema
        conn.execute(
            "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")

        # Then load data.
        conn.execute(f'COPY person from "{person_csv}"')
        conn.execute(
            f'COPY knows from "{person_knows_person_csv}" (from="person", to="person")'
        )

        # Test add edge property
        res = conn.execute("ALTER TABLE knows ADD since INT32")

        res = conn.execute("MATCH (n:person)-[e:knows]->(m:person) return e;")
        for record in res:
            print(record)

        # Test delete edge property
        res = conn.execute("ALTER TABLE knows DROP since")
        res = conn.execute("MATCH (:person)-[e:knows]->(:person) return e;")
        for record in res:
            print(record)

        # Test delete all edge property
        res = conn.execute("ALTER TABLE knows DROP weight")
        res = conn.execute("MATCH (:person)-[e:knows]->(:person) return e;")
        for record in res:
            print(record)

        # Add new table
        conn.execute("CREATE REL TABLE follows(FROM person TO person, weight DOUBLE);")
        conn.execute(
            f'COPY follows from "{person_knows_person_csv}" (from="person", to="person")'
        )

        # Test delete the only one property
        res = conn.execute("ALTER TABLE follows DROP weight")
        res = conn.execute("MATCH (:person)-[e:follows]->(:person) return e;")
        for record in res:
            print(record)

        res = conn.execute("ALTER TABLE follows ADD since INT32")
        res = conn.execute("MATCH (:person)-[e:follows]->(:person) return e;")
        for record in res:
            print(record)

    def _extension_tests_enabled(self):
        return os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )

    def _skip_if_extension_tests_disabled(self):
        if not self._extension_tests_enabled():
            self.skipTest(
                "Extension tests disabled by default; set NEUG_RUN_EXTENSION_TESTS=1."
            )

    def _load_extension(self, conn, ext_name):
        conn.execute(f"INSTALL {ext_name}")
        conn.execute(f"LOAD EXTENSION {ext_name}")

    def _assert_extension_loaded(self, conn, ext_name):
        result = conn.execute("CALL SHOW_LOADED_EXTENSIONS() RETURN *")
        names = [row[0].upper() for row in result if row[0]]
        self.assertIn(ext_name.upper(), names)

    def test_extension_parquet(self):
        """Mini test for parquet extension: round-trip via COPY TO / LOAD FROM."""
        self._skip_if_extension_tests_disabled()
        db_dir = "/tmp/test_extension_parquet"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        db = Database(db_dir, "w")
        conn = db.connect()
        try:
            self._load_extension(conn, "parquet")
            self._assert_extension_loaded(conn, "parquet")

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
            self.assertEqual(len(rows), 2)
        finally:
            conn.close()
            db.close()

    def test_extension_httpfs(self):
        """Mini test for httpfs extension: only verify INSTALL/LOAD, no real access."""
        self._skip_if_extension_tests_disabled()
        db_dir = "/tmp/test_extension_httpfs"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        db = Database(db_dir, "w")
        conn = db.connect()
        try:
            self._load_extension(conn, "httpfs")
            self._assert_extension_loaded(conn, "httpfs")
        finally:
            conn.close()
            db.close()

    def test_extension_pattern_matching(self):
        """Mini test for pattern_matching extension: CALL PATTERN_MATCH."""
        self._skip_if_extension_tests_disabled()
        db_dir = "/tmp/test_extension_pattern_matching"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        db = Database(db_dir, "w")
        conn = db.connect()
        try:
            self._load_extension(conn, "pattern_matching")
            self._assert_extension_loaded(conn, "pattern_matching")

            conn.execute(
                "CREATE NODE TABLE Person("
                "id INT32 PRIMARY KEY, name STRING, age INT32, city STRING);"
            )
            conn.execute(
                "CREATE REL TABLE person_knows_person(FROM Person TO Person, weight DOUBLE);"
            )
            conn.execute(
                "CREATE (:Person {id: 0, name: 'Alice', age: 20, city: 'NYC'});"
            )
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
            self.assertEqual(len(rows), 1)
        finally:
            conn.close()
            db.close()

    def test_extension_vector_search(self):
        """Mini test for vector_search extension: HNSW index and vector distance."""
        self._skip_if_extension_tests_disabled()
        db_dir = "/tmp/test_extension_vector_search"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        db = Database(db_dir, "w")
        conn = db.connect()
        try:
            self._load_extension(conn, "vector_search")
            self._assert_extension_loaded(conn, "vector_search")

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
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0][0], 1)
        finally:
            conn.close()
            db.close()

    def test_extension_fts(self):
        """Mini test for fts extension: FTS index and bm25 search."""
        self._skip_if_extension_tests_disabled()
        db_dir = "/tmp/test_extension_fts"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        db = Database(db_dir, "w")
        conn = db.connect()
        try:
            self._load_extension(conn, "fts")
            self._assert_extension_loaded(conn, "fts")

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
            self.assertTrue(len(rows) >= 1)
        finally:
            conn.close()
            db.close()

    def test_extension_gds(self):
        """Mini test for gds extension: project_graph and page_rank."""
        self._skip_if_extension_tests_disabled()
        db_dir = "/tmp/test_extension_gds"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        db = Database(db_dir, "w")
        conn = db.connect()
        try:
            self._load_extension(conn, "gds")
            self._assert_extension_loaded(conn, "gds")

            conn.execute("CREATE NODE TABLE Person(id INT64 PRIMARY KEY, name STRING);")
            conn.execute(
                "CREATE REL TABLE knows(FROM Person TO Person, weight DOUBLE);"
            )
            conn.execute("CREATE (:Person {id: 1, name: 'Alice'});")
            conn.execute("CREATE (:Person {id: 2, name: 'Bob'});")
            conn.execute(
                "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
                "CREATE (a)-[:knows {weight: 1.0}]->(b);"
            )
            conn.execute(
                "CALL project_graph('g', ['Person'], "
                "{'[Person, knows, Person]': ''});"
            )
            rows = list(
                conn.execute(
                    "CALL page_rank('g', {max_iterations: 10}) "
                    "YIELD node, rank RETURN node.id, rank;"
                )
            )
            self.assertEqual(len(rows), 2)
        finally:
            conn.close()
            db.close()
