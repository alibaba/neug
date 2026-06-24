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
import shutil

import pytest

from neug.database import Database
from neug.proto.error_pb2 import ERR_COMPILATION
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_SCHEMA_MISMATCH

logger = logging.getLogger(__name__)


# DB-003-08 DML-create node
def test_insert_node(tmp_path):
    db_dir = tmp_path / "insert_node"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    # 准备schema
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    # case 1: insert with all properties
    conn.execute("CREATE (u:person{name:'Alice',age:35});")
    # case 2: insert with partial properties, age should be NULL
    # TODO(zhanglei): storage currently does not support NULL value
    # conn.execute("CREATE (u:person{name:'Josh'});")
    # case 3: insert without primary key value, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{age:36});")
    assert str(ERR_COMPILATION) in str(excinfo.value)
    # case 4: duplicate primary key value, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{name:'Alice', age:26});")
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # case 5: insert values inconsistent with schema, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{name:'Alice', age:26, addr:'aa'});")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-09 DML-create edge
def test_insert_edge(tmp_path):
    db_dir = tmp_path / "insert_edge"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, since INT64, MANY_TO_MANY);"
    )
    # 插入端点
    conn.execute("CREATE (u:person{name:'Alice'});")
    conn.execute("CREATE (u:person{name:'Josh'});")
    # case 1: insert edge with specified two endpoints
    conn.execute(
        "MATCH (u1:person), (u2:person) WHERE u1.name = 'Alice' "
        "AND u2.name = 'Josh' CREATE (u1)-[:follows {since: 2011}]->(u2);"
    )
    # case 2: insert edge with one endpoint specified
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.name = 'Alice' CREATE (a)-[:follows {since:2022}]->(b);"
    )
    # case 3: insert edge with endpoints not existing, will NOT create edge
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.name = 'nobody' CREATE (a)-[:follows {since:2022}]->(b);"
    )
    # case 4: create edge together with new endpoints
    conn.execute(
        "CREATE (u:person {name: 'Alice1'})-[:follows {since:2022}]->(b:person {name: 'Josh1'});"
    )
    # case 5: create edge with existing endpoints, will FAIL
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE (u:person {name: 'Alice'})-[:follows {since:2022}]->(b:person {name: 'Josh2'});"
        )
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # case 6: edge property schema mismatch
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE (u:person {name: 'Alice2'})-[:follows {nonprop:2022}]->(b:person {name: 'Josh2'});"
        )
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    conn.close()
    db.close()


def test_create_edge_return_edge_property(tmp_path):
    db_dir = tmp_path / "create_edge_return_edge_property"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    conn.execute(
        "CREATE REL TABLE knows(FROM person TO person, since INT64, MANY_TO_MANY);"
    )
    conn.execute("CREATE (a:person {id: 1});")
    conn.execute("CREATE (b:person {id: 2});")

    result = conn.execute(
        "MATCH (a:person {id: 1}), (b:person {id: 2}) "
        "CREATE (a)-[e:knows {since: 2024}]->(b) "
        "RETURN e.since;"
    )

    records = list(result)
    assert records == [[2024]], f"Expected [[2024]], got {records}"

    conn.close()
    db.close()


# DB-003-10 DML-SET node property
def test_set_node_property(tmp_path):
    db_dir = tmp_path / "set_node_prop"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (u:person{name:'Alice',age:35});")
    result = conn.execute("MATCH (u:person) WHERE u.name = 'Alice' RETURN u.age;")
    assert result.__next__()[0] == 35

    # case 1: valid update
    result = conn.execute(
        "MATCH (u:person) WHERE u.name = 'Alice' SET u.age = 50 RETURN u.age;"
    )
    assert result.__next__()[0] == 50
    # case 2: update property to NULL
    # TODO(zhanglei): storage currently does not support NULL value
    # result = conn.execute(
    #     "MATCH (u:person) WHERE u.name = 'Alice' SET u.age = NULL RETURN u.*;"
    # )
    # assert result.__next__()[1] is None
    # case 3: add new property
    conn.execute("ALTER TABLE person ADD population INT64;")
    # TODO(zhanglei): currently uproject only support projecting single var,
    # we should reuse the code for project operator for read pipeline.
    result = conn.execute("MATCH (u) SET u.population = 0 RETURN u.population;")
    assert result.__next__()[0] == 0
    # case 4: update non-existing node, should not affect anything
    result = conn.execute(
        "MATCH (u:person) WHERE u.name = 'nobody' SET u.age = 50 RETURN u.name;"
    )
    assert len(result) == 0
    # case 5: update with property schema mismatch, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "MATCH (u:person) WHERE u.name = 'Alice' SET u.addr = '' RETURN u.*;"
        )
    assert "Cannot find property" in str(excinfo.value)
    conn.close()
    db.close()


def test_set_multi_edge_property(tmp_path):
    db_dir = tmp_path / "set_multi_edge_prop"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
        conn.execute("CREATE NODE TABLE software(name STRING, PRIMARY KEY(name));")
        conn.execute(
            "CREATE REL TABLE create_software(FROM person TO software, since INT64, weight DOUBLE, MANY_TO_MANY);"
        )
        conn.execute("CREATE (u:person{name:'Alice'});")
        conn.execute("CREATE (u:person{name:'Bob'});")
        conn.execute("CREATE (s:software{name:'Neug'});")
        conn.execute(
            "MATCH (u1:person), (s1:software) WHERE u1.name = 'Alice' AND s1.name = 'Neug' "
            "CREATE (u1)-[:create_software {since: 2022, weight: 1.0}]->(s1);"
        )
        conn.execute(
            "MATCH (u1:person), (s1:software) WHERE u1.name = 'Bob' AND s1.name = 'Neug' "
            "CREATE (u1)-[:create_software {since: 2023, weight: 2.0}]->(s1);"
        )
        # case 1: valid query results
        result = conn.execute(
            "MATCH (u0:person)-[c:create_software]->(s1:software) return c.since, c.weight;"
        )
        assert result.__next__() == [2022, 1.0]
        assert result.__next__() == [2023, 2.0]
        # case 2: valid update with single label relationship
        result = conn.execute(
            "MATCH (u0:person)-[c:create_software]->(s1:software) "
            "WHERE u0.name = 'Alice' AND s1.name = 'Neug' "
            "SET c.since = 1999, c.weight = 3.0 RETURN c.since;"
        )
        assert result.__next__() == [1999]
        # case 3: test query result
        result = conn.execute(
            "MATCH (u0:person)-[c:create_software]->(s1:software) WHERE u0.name = 'Alice' "
            "AND s1.name = 'Neug' RETURN c.since, c.weight;"
        )
        assert result.__next__() == [1999, 3.0]
    finally:
        conn.close()
        db.close()


# DB-003-11 DML-SET edge property
def test_set_edge_property(tmp_path):
    db_dir = tmp_path / "set_edge_prop"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, since INT64, MANY_TO_MANY);"
    )
    conn.execute("CREATE REL TABLE likes(FROM person TO person, since INT64);")
    conn.execute("CREATE (u:person{name:'Alice'});")
    conn.execute("CREATE (u:person{name:'Josh'});")
    conn.execute("CREATE (u:person{name:'Bob'});")
    conn.execute(
        "MATCH (u1:person), (u2:person) WHERE u1.name = 'Alice' AND"
        " u2.name = 'Josh' CREATE (u1)-[:follows {since: 2012}]->(u2);"
    )
    conn.execute(
        "MATCH (u1:person), (u2:person) WHERE u1.name = 'Alice' AND"
        " u2.name = 'Bob' CREATE (u1)-[:follows {since: 2009}]->(u2);"
    )
    # case 1: valid update with single label relationship
    result = conn.execute(
        "MATCH (u0:person)-[f:follows]->(u1:person)"
        " WHERE u0.name = 'Alice' AND u1.name = 'Josh' SET f.since = 1999 RETURN f.since;"
    )
    assert result.__next__()[0] == 1999
    # case 2: valid update with multiple label relationship
    result = conn.execute(
        "MATCH (u0)-[f]->() WHERE u0.name = 'Alice' SET f.since = 1999 RETURN f.since;"
    )
    assert result.__next__()[0] == 1999
    assert result.__next__()[0] == 1999
    # case 3: update with property schema mismatch, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "MATCH (u0)-[f]->() WHERE u0.name = 'Alice' SET f.noprop = 1999 RETURN f.noprop;"
        )
    assert "Cannot find property noprop" in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-22
def test_insert_vertex_edge(tmp_path):
    db_dir = tmp_path / "insert_vertex_edge"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")

    # Insert vertex
    conn.execute("CREATE (p:person {id: 1, name: 'Alice'});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob'});")
    conn.execute("CREATE (p:person {id: 3, name: 'Charlie'});")

    # Insert edge
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.name = 'Alice' AND b.name = 'Bob' "
        "CREATE (a)-[:knows {weight: 1.0}]->(b);"
    )
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.name = 'Alice' AND b.name = 'Charlie' "
        "CREATE (a)-[:knows {weight: 2.0}]->(b);"
    )

    # Verify insertion
    result = conn.execute("MATCH (n) RETURN n;")
    assert len(result) == 3

    result = conn.execute("MATCH (a:person)-[r:knows]->(b:person) RETURN a, r, b;")
    assert len(result) == 2  # Only one edge should be present
    result = conn.execute(
        "MATCH (a:person)-[b:knows]->(c:person) WHERE c.name = 'Charlie' RETURN b.weight;"
    )
    assert len(result) == 1
    assert result.__next__()[0] == 2.0  # Weight of the
    conn.close()
    db.close()


def test_delete_edges(tmp_path):
    db_dir = str(tmp_path / "test_delete_edges")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
        conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, id INT64);")
        conn.execute("CREATE (p: Person {id: 111});")
        conn.execute("CREATE (p: Person {id: 222});")
        conn.execute("CREATE (p: Person {id: 333});")
        conn.execute(
            "MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 333}]->(p2);"
        )
        conn.execute(
            "MATCH (p1: Person {id: 111}), (p2: Person {id: 333}) CREATE (p1)-[k:Knows {id: 444}]->(p2);"
        )

        conn.execute(
            """
            MATCH (p1: Person)-[k: Knows]->(p2:Person) WHERE k.id = 333 DELETE k
            """
        )
        res = conn.execute("MATCH (p1: Person)-[k: Knows]->(p2:Person) RETURN count(k)")
        records = list(res)
        assert records == [[1]]
    finally:
        conn.close()
        db.close()


def test_delete_vertex_detach_edge(tmp_path):
    db_dir = str(tmp_path / "test_delete_vertex_detach_edge")
    logger.info("Starting test_delete_vertex_detach_edge")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, id INT64);")
    conn.execute("CREATE (p: Person {id: 111});")
    conn.execute("CREATE (p: Person {id: 222});")
    conn.execute(
        "MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 333}]->(p2);"
    )

    conn.execute(
        """
        MATCH (p1: Person)-[k: Knows]->(p2:Person) WHERE p1.id = 111 DETACH DELETE p1
        """
    )
    res = conn.execute("MATCH (p: Person) RETURN p.id;")
    records = list(res)
    assert records == [[222]], f"Expected value [[222]], got {records}"
    # Drop person
    conn.execute("DROP TABLE IF EXISTS Person;")

    res = conn.execute("MATCH ()-[e]->() RETURN count(e);")
    records = list(res)
    # TODO(zhanglei): Should return [[0]], but now returns empty list
    assert records == [], f"Expected value [], got {records}"

    with pytest.raises(Exception):
        conn.execute("MATCH (p: Person) RETURN count(p);")
    with pytest.raises(Exception):
        conn.execute("MATCH ()-[e: Knows]->() RETURN count(e);")
    logger.info("test_delete_vertex_detach_edge passed")
    conn.close()
    db.close()


def test_delete_vertex_detach_edge2(tmp_path):
    db_dir = str(tmp_path / "test_delete_vertex_detach_edge2")
    logger.info("Starting test_delete_vertex_detach_edge2")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE NODE TABLE City(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE LivesIn(FROM Person TO City, id INT64);")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, id INT64);")
    conn.execute("CREATE (p: Person {id: 1});")
    conn.execute("CREATE (p: Person {id: 2});")
    conn.execute("CREATE (p: Person {id: 3});")
    conn.execute("CREATE (c: City {id: 100});")
    conn.execute("CREATE (c: City {id: 200});")
    conn.execute(
        "MATCH (p1: Person {id: 1}), (p2: Person {id: 2}) CREATE (p1)-[k:Knows {id: 10}]->(p2);"
    )
    conn.execute(
        "MATCH (p1: Person {id: 2}), (p2: Person {id: 3}) CREATE (p1)-[k:Knows {id: 20}]->(p2);"
    )
    conn.execute(
        "MATCH (p: Person {id: 3}), (p2: Person {id: 1}) CREATE (p)-[k:Knows {id: 30}]->(p2);"
    )
    conn.execute(
        "MATCH (p: Person {id: 1}), (c: City {id: 100}) CREATE (p)-[k:LivesIn {id: 1000}]->(c);"
    )
    conn.execute(
        "MATCH (p: Person {id: 2}), (c: City {id: 200}) CREATE (p)-[k:LivesIn {id: 2000}]->(c);"
    )
    conn.execute(
        "MATCH (p: Person {id: 3}), (c: City {id: 100}) CREATE (p)-[k:LivesIn {id: 3000}]->(c);"
    )
    conn.execute(
        "MATCH (p: Person {id: 3}), (c: City {id: 200}) CREATE (p)-[k:LivesIn {id: 4000}]->(c);"
    )
    conn.execute("MATCH (p1: Person {id: 3}) DELETE p1;")
    res = conn.execute("MATCH (p: Person) RETURN count(p);")
    assert list(res) == [[2]], f"Expected value [[2]], got {list(res)}"
    res = conn.execute("MATCH ()-[e: Knows]->() RETURN count(e);")
    assert list(res) == [[1]], f"Expected value [[1]], got {list(res)}"
    res = conn.execute("MATCH ()-[e: LivesIn]->() RETURN count(e);")
    assert list(res) == [[2]], f"Expected value [[2]], got {list(res)}"
    logger.info("test_delete_vertex_detach_edge2 passed")
    conn.close()
    db.close()


def test_delete_all_rows_then_reinsert_visible(tmp_path):
    """After deleting all rows, newly inserted rows must be visible."""
    db_dir = tmp_path / "delete_reinsert"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE IF NOT EXISTS Person(id STRING PRIMARY KEY);")
    conn.execute("CREATE (p:Person {id: 'alice'});")
    conn.execute("CREATE (p:Person {id: 'bob'});")
    conn.execute("CHECKPOINT")
    assert sorted(list(conn.execute("MATCH (p:Person) RETURN p.id;"))) == [
        ["alice"],
        ["bob"],
    ]

    # Delete all rows
    conn.execute("MATCH (a:Person) DELETE a;")
    assert list(conn.execute("MATCH (p:Person) RETURN p.id;")) == []

    # Re-insert — new data must be visible
    conn.execute("CREATE (p:Person {id: 'charlie'});")
    assert list(conn.execute("MATCH (p:Person) RETURN p.id;")) == [["charlie"]]

    conn.close()
    db.close()


def test_default_value(tmp_path):
    db_dir = str(tmp_path / "test_default_value")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE Person(id INT64 PRIMARY KEY, age INT32 DEFAULT 18, name STRING DEFAULT 'unknown');"
        )
        conn.execute(
            "CREATE REL TABLE Knows(FROM Person TO Person, since INT32 DEFAULT 2020, NOTE STRING DEFAULT 'none');"
        )
        conn.execute("CREATE (p: Person {id: 111});")
        res = conn.execute("MATCH (p: Person) RETURN p.id, p.age, p.name;")
        records = list(res)
        assert records == [
            [111, 18, "unknown"]
        ], f"Expected value [[111, 18, 'unknown']], got {records}"
        conn.execute("CREATE (p: Person {id: 222, age: 25});")
        res = conn.execute("MATCH (p: Person {id: 222}) RETURN p.id, p.age, p.name;")
        records = list(res)
        assert records == [
            [222, 25, "unknown"]
        ], f"Expected value [[222, 25, 'unknown']], got {records}"
        conn.execute(
            "MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows]->(p2);"
        )
        conn.execute(
            "MATCH (p1: Person {id: 222}), (p2: Person {id: 111}) CREATE (p1)-[k:Knows {since: 2022, NOTE: 'updated'}]->(p2);"
        )
        res = conn.execute("MATCH ()-[e: Knows]->() RETURN e.since, e.NOTE;")
        records = list(res)
        assert records == [
            [2020, "none"],
            [2022, "updated"],
        ], f"Expected value [[2020, ''], [2022, 'updated']], got {records}"
        logger.info("test_default_value passed")
    finally:
        conn.close()
        db.close()


def test_edge_default_value(tmp_path):
    db_dir = str(tmp_path / "test_edge_default_value")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    try:
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
        conn.execute('ALTER TABLE TestEdge ADD description STRING DEFAULT "unknown"')
        conn.execute(
            "CREATE (n1: TestNode {id: 1, thread_id: 1}), (n2: TestNode {id: 2, thread_id: 1}) CREATE (n1)-[:TestEdge]->(n2);"
        )
        res = conn.execute("MATCH ()-[e: TestEdge]->() RETURN e.description;")
        records = list(res)
        assert records == [["unknown"]], f"Expected value [['unknown']], got {records}"
    finally:
        conn.close()
        db.close()


def test_insert_many_vertices(tmp_path):
    db_dir = str(tmp_path / "test_insert_many_vertices")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    for i in range(10000):
        conn.execute(f"CREATE (p: Person {{id: {i}}});")
    res = conn.execute("MATCH (p: Person) RETURN count(p);")
    records = list(res)
    assert records == [[10000]], f"Expected value [[10000]], got {records}"
    conn.close()
    db.close()


def test_insert_many_edges(tmp_path):
    db_dir = str(tmp_path / "test_insert_many_edges")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person);")
    for i in range(100):
        conn.execute(f"CREATE (p: Person {{id: {i}}});")
    for i in range(100):
        for j in range(i + 1, 100):
            conn.execute(
                f"MATCH (p1: Person {{id: {i}}}), (p2: Person {{id: {j}}}) CREATE (p1)-[:Knows]->(p2);"
            )
    res = conn.execute("MATCH ()-[e: Knows]->() RETURN count(e);")
    records = list(res)
    assert records == [[4950]], f"Expected value [[4950]], got {records}"
    conn.close()
    db.close()


def test_copy_from(tmp_path):
    db_dir = str(tmp_path / "test_copy_from")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    try:
        # prepare file
        file = db_dir + "/test_data.csv"
        with open(file, "w") as f:
            f.write('"id","entity","entity_type"\n')
            f.write('1,"-1-10000","属性"\n')
            f.write('2,"-180°-180°","场景"\n')
            f.write('3,"-180°-180°","属性"\n')
            f.write('4,"0","属性"\n')
            f.write('5,"0-1","场景"\n')
            f.write('6,"0-1","属性"\n')
            f.write('7,"0-10","属性"\n')
            f.write('8,"0-100","属性"\n')
            f.write('9,"0-1000","属性"\n')
            f.write('10,"0-1000000","属性"\n')
            f.close()

        conn.execute(
            """
            CREATE NODE TABLE Entity(
                id STRING,
                entity STRING,
                entity_type STRING,
                PRIMARY KEY(id)
            )
        """
        )
        conn.execute(f"COPY Entity FROM '{file}' (HEADER TRUE, DELIMITER=',')")
    finally:
        conn.close()
        db.close()
