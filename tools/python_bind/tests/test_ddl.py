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
import shutil
import sys
import unittest

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database

logger = logging.getLogger(__name__)


def test_batch_loading_modern_graph():
    # create a tmp directory for the graph
    db_dir = "/tmp/test_batch_loading"
    shutil.rmtree(db_dir, ignore_errors=True)

    db = Database(db_dir, "w")
    conn = db.connect()
    # First create the graph schema
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")

    # Test Adding Column
    conn.execute("ALTER TABLE person ADD birthday DATE;")

    # Test adding column if not exists
    conn.execute("ALTER TABLE person ADD IF NOT EXISTS birthday DATE;")

    # Close and reopen database
    conn.close()
    db.close()
    db = Database(db_dir, "w")
    conn = db.connect()

    # Add name STRING, expect exception since name already exists
    with pytest.raises(Exception):
        conn.execute("ALTER TABLE person ADD name STRING;")

    # Drop the only edge property
    conn.execute("ALTER TABLE knows DROP weight;")

    # Add a edge property
    conn.execute("ALTER TABLE knows ADD registion DATE;")

    # Drop an non-existing column
    with pytest.raises(Exception):
        conn.execute("ALTER TABLE person DROP non_existing_column;")

    # Drop a column if not exists
    conn.execute("ALTER TABLE person DROP birthday;")

    # Drop an existing column
    conn.execute("ALTER TABLE person DROP IF EXISTS birthday;")

    # Rename a column
    conn.execute("ALTER TABLE person RENAME name TO username;")

    # Batch delete vertices in table
    conn.execute("MATCH (v:person) DELETE v;")

    # Delete a edge type
    conn.execute("DROP TABLE knows;")

    # Delete a vertex type
    conn.execute("DROP TABLE person;")


def test_add_multiple_edge_properties():
    db_dir = "/tmp/test_add_multiple_edge_triplet"
    shutil.rmtree(db_dir, ignore_errors=True)

    db = Database(db_dir, "w")
    conn = db.connect()
    # First create the graph schema
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    conn.execute("CREATE REL TABLE likes(FROM person TO person, weight DOUBLE);")

    # insert some vertices and edges
    conn.execute("CREATE (:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {id: 2, name: 'Bob', age: 32});")
    conn.execute("CREATE (:person {id: 3, name: 'Charlie', age: 25});")
    conn.execute(
        "MATCH (a:person {id: 1}), (b:person {id: 2}) CREATE (a)-[:knows {weight: 0.5}]->(b);"
    )
    conn.execute(
        "MATCH (a:person {id: 1}), (c:person {id: 3}) CREATE (a)-[:likes {weight: 0.8}]->(c);"
    )

    res = conn.execute("MATCH (a:person)-[e]->(b:person) RETURN e.weight;")
    assert list(res) == [[0.5], [0.8]]

    conn.execute("ALTER TABLE knows ADD since DATE;")
    conn.execute("ALTER TABLE likes ADD since DATE;")
    conn.execute("ALTER TABLE knows ADD strength INT64;")
    conn.execute("ALTER TABLE likes ADD strength INT64;")

    res = conn.execute(
        "MATCH (a:person)-[e]->(b:person) RETURN e.weight, e.since, e.strength;"
    )
    import datetime

    expected = [
        [0.5, datetime.date(1970, 1, 1), 0],
        [0.8, datetime.date(1970, 1, 1), 0],
    ]
    assert list(res) == expected

    # Then open from the directory, expect the schema and data are correct
    conn.close()
    db.close()

    db2 = Database(db_dir, "r")
    conn2 = db2.connect()
    res2 = conn2.execute(
        "MATCH (a:person)-[e]->(b:person) RETURN e.weight, e.since, e.strength;"
    )
    expected = [
        [0.5, datetime.date(1970, 1, 1), 0],
        [0.8, datetime.date(1970, 1, 1), 0],
    ]
    assert list(res2) == expected
    conn2.close()
    db2.close()


def test_add_multiple_edge_properties2():
    db_dir = "/tmp/test_add_multiple_edge_triplet2"
    shutil.rmtree(db_dir, ignore_errors=True)

    db = Database(db_dir, "w")
    conn = db.connect()
    # First create the graph schema
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person);")
    conn.execute("CREATE REL TABLE likes(FROM person TO person);")

    # insert some vertices and edges
    conn.execute("CREATE (:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (:person {id: 2, name: 'Bob', age: 32});")
    conn.execute("CREATE (:person {id: 3, name: 'Charlie', age: 25});")
    conn.execute("ALTER TABLE knows ADD weight DOUBLE;")
    conn.execute("ALTER TABLE likes ADD weight DOUBLE;")
    conn.execute(
        "MATCH (a:person {id: 1}), (b:person {id: 2}) CREATE (a)-[:knows {weight: 0.5}]->(b);"
    )
    conn.execute(
        "MATCH (a:person {id: 1}), (c:person {id: 3}) CREATE (a)-[:likes {weight: 0.8}]->(c);"
    )

    res = conn.execute("MATCH (a:person)-[e]->(b:person) RETURN e.weight;")
    assert list(res) == [[0.5], [0.8]]

    conn.execute("ALTER TABLE knows ADD since DATE;")
    conn.execute("ALTER TABLE likes ADD since DATE;")
    conn.execute("ALTER TABLE knows ADD strength INT64;")
    conn.execute("ALTER TABLE likes ADD strength INT64;")

    res = conn.execute(
        "MATCH (a:person)-[e]->(b:person) RETURN e.weight, e.since, e.strength;"
    )
    import datetime

    assert list(res) == [
        [0.5, datetime.date(1970, 1, 1), 0],
        [0.8, datetime.date(1970, 1, 1), 0],
    ]

    # Then open from the directory, expect the schema and data are correct
    conn.close()
    db.close()

    db2 = Database(db_dir, "r")
    conn2 = db2.connect()
    res2 = conn2.execute(
        "MATCH (a:person)-[e]->(b:person) RETURN e.weight, e.since, e.strength;"
    )
    assert list(res2) == [
        [0.5, datetime.date(1970, 1, 1), 0],
        [0.8, datetime.date(1970, 1, 1), 0],
    ]
    conn2.close()
    db2.close()


def test_drop_vertex_table():
    db_dir = "/tmp/test_drop_vertex_table"
    shutil.rmtree(db_dir, ignore_errors=True)

    db = Database(db_dir, "w")
    conn = db.connect()
    # First create the graph schema
    with pytest.raises(Exception):
        conn.execute("DROP TABLE TestNode")
    conn.execute(
        """
            CREATE NODE TABLE IF NOT EXISTS TestNode(
                id INT64 PRIMARY KEY,
                thread_id INT64,
                iteration INT64,
                timestamp INT64,
                random_value INT64
            )
        """
    )
    conn.close()
    db.close()

    db2 = Database(db_dir, "w")
    conn2 = db2.connect()
    conn2.execute("DROP TABLE IF EXISTS TestNode")
    conn2.execute(
        """
            CREATE NODE TABLE IF NOT EXISTS TestNode(
                id INT64 PRIMARY KEY,
                thread_id INT64,
                iteration INT64,
                timestamp INT64,
                random_value INT64
            )
        """
    )
    conn2.close()
    db2.close()


def test_drop_edge_table():
    db_dir = "/tmp/test_drop_edge_table"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    # First create the graph schema
    with pytest.raises(Exception):
        conn.execute("DROP TABLE TestEdge")
    with pytest.raises(Exception):
        conn.execute("DROP TABLE TestEdge2")
    conn.execute(
        """
            CREATE NODE TABLE IF NOT EXISTS TestNode(
                id INT64 PRIMARY KEY,
                thread_id INT64
            )
        """
    )
    conn.execute(
        """
                 CREATE NODE TABLE IF NOT EXISTS TestNode2(
                     id INT64 PRIMARY KEY,
                     thread_id INT64
                 )
             """
    )
    conn.execute(
        """
            CREATE REL TABLE IF NOT EXISTS TestEdge(
                FROM TestNode TO TestNode,
                iteration INT64,
                timestamp INT64,
                random_value INT64
            )
        """
    )
    conn.execute(
        """
            CREATE REL TABLE IF NOT EXISTS TestEdge2(
                FROM TestNode TO TestNode2,
                iteration INT64,
                timestamp INT64,
                random_value INT64
            )
        """
    )
    conn.close()
    db.close()

    db2 = Database(db_dir, "w")
    conn2 = db2.connect()
    conn2.execute("DROP TABLE IF EXISTS TestEdge")
    conn2.execute(
        """
            CREATE REL TABLE IF NOT EXISTS TestEdge(
                FROM TestNode TO TestNode,
                iteration INT64,
                timestamp INT64,
                random_value INT64
            )
        """
    )
    conn2.execute("DROP TABLE IF EXISTS TestEdge2")
    conn2.execute(
        """
            CREATE REL TABLE IF NOT EXISTS TestEdge2(
                FROM TestNode TO TestNode2,
                iteration INT64,
                timestamp INT64,
                random_value INT64
            )
        """
    )
    conn2.close()
    db2.close()


def test_create_varchar_type():
    db_dir = "/tmp/test_create_varchar_type"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE TestNode(id INT64 PRIMARY KEY, name VARCHAR(10));")
    conn.execute("CREATE (:TestNode {id: 1, name: 'Alice'});")
    res = conn.execute("Match (n:TestNode) Return n.name;")
    assert list(res) == [["Alice"]]

    conn.execute(
        "CREATE (:TestNode {id: 2, name: 'this is a string longer than 10 characters, should be truncated'});"
    )
    res = conn.execute("Match (n:TestNode {id: 2}) Return n.name;")
    assert list(res) == [["this is a "]]
    conn.close()
    db.close()


def test_alter_varchar_type():
    db_dir = "/tmp/test_alter_varchar_type"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE TestNode(id INT64 PRIMARY KEY);")
    conn.execute("ALTER TABLE TestNode ADD name VARCHAR(10);")
    conn.execute("CREATE (:TestNode {id: 1, name: 'Alice'});")
    res = conn.execute("Match (n:TestNode) Return n.id, n.name;")
    assert list(res) == [[1, "Alice"]]
    conn.close()
    db.close()


def test_get_varchar_default_value_1():
    db_dir = "/tmp/test_get_varchar_default_value_1"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE TestNode(id INT64 PRIMARY KEY, name VARCHAR(20) DEFAULT 'default_name');"
    )
    conn.execute("CREATE (:TestNode {id: 1});")
    conn.execute("CREATE (:TestNode {id: 2});")
    conn.execute("CREATE (:TestNode {id: 3});")
    res = conn.execute("Match (n:TestNode) Return n.name;")
    assert list(res) == [["default_name"], ["default_name"], ["default_name"]]
    conn.close()
    db.close()


def test_get_varchar_default_value_2():
    db_dir = "/tmp/test_get_varchar_default_value_2"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE TestNode(id INT64 PRIMARY KEY);")
    conn.execute("CREATE REL TABLE TestEdge(FROM TestNode TO TestNode);")
    conn.execute("CREATE (:TestNode {id: 1});")
    conn.execute("CREATE (:TestNode {id: 2});")
    conn.execute("CREATE (:TestNode {id: 3});")
    conn.execute(
        "MATCH (a:TestNode {id: 1}), (b:TestNode {id: 2}) CREATE (a)-[:TestEdge]->(b);"
    )
    conn.execute(
        "MATCH (a:TestNode {id: 2}), (b:TestNode {id: 3}) CREATE (a)-[:TestEdge]->(b);"
    )
    conn.execute("ALTER TABLE TestNode ADD name VARCHAR(20) DEFAULT 'default_name';")
    conn.execute("CREATE (:TestNode {id: 4});")
    conn.execute("CREATE (:TestNode {id: 5, name: 'custom_name'});")
    res = conn.execute("Match (n:TestNode) Return n.name ORDER BY n.name;")
    assert list(res) == [
        ["custom_name"],
        ["default_name"],
        ["default_name"],
        ["default_name"],
        ["default_name"],
    ]
    conn.execute("ALTER TABLE TestEdge ADD date INT64;")
    conn.execute(
        "MATCH (a:TestNode {id: 1})-[e:TestEdge]->(b:TestNode {id: 2}) SET e.date = 1234567890;"
    )
    conn.execute(
        "MATCH (a:TestNode {id: 1}), (b:TestNode { id: 3 }) CREATE (a)-[:TestEdge {date: 9876543210}]->(b);"
    )
    res = conn.execute(
        "MATCH (a:TestNode {id: 1})-[e:TestEdge]->(b:TestNode) RETURN e.date;"
    )
    assert list(res) == [[1234567890], [9876543210]]
    conn.close()
    db.close()


def test_drop_add_edge_table_column():
    db_dir = "/tmp/test_drop_add_edge_table_column"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    # First create the graph schema
    conn.execute(
        """
            CREATE NODE TABLE IF NOT EXISTS TestNode(
                id INT64 PRIMARY KEY,
                thread_id INT64
            )
        """
    )
    conn.execute(
        """
            CREATE REL TABLE IF NOT EXISTS TestEdge(
                FROM TestNode TO TestNode
            )
        """
    )
    conn.close()
    db.close()

    db2 = Database(db_dir, "w")
    conn2 = db2.connect()
    conn2.execute("CREATE (v:TestNode {id: 1, thread_id: 1});")
    conn2.execute("CREATE (v:TestNode {id: 2, thread_id: 2});")
    conn2.execute("CREATE (v:TestNode {id: 3, thread_id: 3});")
    conn2.execute(
        "MATCH (v:TestNode {id: 1}), (v2:TestNode {id: 2}) CREATE (v)-[:TestEdge]->(v2);"
    )
    conn2.execute(
        "MATCH (v:TestNode {id: 1}), (v2:TestNode {id: 3}) CREATE (v)-[:TestEdge]->(v2);"
    )
    conn2.execute("ALTER TABLE TestEdge ADD iteration INT64;")
    conn2.execute(
        "MATCH (v:TestNode {id: 1}), (v2:TestNode {id: 2}) CREATE (v)-[:TestEdge {iteration: 1}]->(v2);"
    )
    ret = conn2.execute(
        "MATCH (v1:TestNode)-[e:TestEdge]->(v2:TestNode) RETURN e.iteration;"
    )
    assert list(ret) == [[0], [0], [1]]
    conn2.execute("ALTER TABLE TestEdge DROP iteration;")
    conn2.execute("ALTER TABLE TestEdge ADD iteration INT64;")
    conn2.execute("ALTER TABLE TestEdge ADD iteration2 INT64;")
    conn2.execute(
        "MATCH (v:TestNode {id: 1}), (v2:TestNode {id: 2}) CREATE (v)-[:TestEdge {iteration: 2, iteration2: 3}]->(v2);"
    )
    ret = conn2.execute(
        "MATCH (v1:TestNode)-[e:TestEdge]->(v2:TestNode) RETURN e.iteration, e.iteration2;"
    )
    assert list(ret) == [[0, 0], [0, 0], [0, 0], [2, 3]]
    conn2.execute("ALTER TABLE TestEdge DROP iteration;")
    conn2.execute("ALTER TABLE TestEdge DROP iteration2;")
    # TODO(zhanglei): Turn on the test after issue #85 is fixed.
    # conn2.execute("ALTER TABLE TestEdge ADD description STRING DEFAULT 'unknown';")
    # conn2.execute(
    #     "MATCH (v:TestNode {id: 1}), (v2:TestNode {id: 2}) CREATE (v)-[:TestEdge {description: 'test'}]->(v2);"
    # )
    # ret = conn2.execute(
    #     "MATCH (v1:TestNode)-[e:TestEdge]->(v2:TestNode) RETURN e.description ORDER BY e.description; "
    # )
    # assert list(ret) == [["unknown"], ["unknown"], ["unknown"], ["unknown"], ["test"]]
    conn2.close()
    db2.close()
