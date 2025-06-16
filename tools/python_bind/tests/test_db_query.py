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

import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from errors import ERR_COMPILATION
from errors import ERR_INVALID_SCHEMA
from errors import ERR_SCHEMA_MISMATCH
from errors import ERR_TYPE_CONVERSION
from errors import ERROR_STRINGS

from nexg.database import Database


# DB-003-01
def test_create_schema_basic_types(tmp_path):
    db_dir = tmp_path / "schema_basic_types"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(p1 INT32, p2 INT64, p3 UINT32, p4 UINT64, "
        "p5 FLOAT, p6 DOUBLE, p7 STRING, PRIMARY KEY (p1));"
    )
    conn.close()
    db.close()


@pytest.mark.skip(reason="not all types supported yet")
def test_create_schema_all_types(tmp_path):
    db_dir = tmp_path / "schema_types"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE Type (p1 INT32, p2 INT64, p3 UINT32, p4 UINT64, "
        "p5 FLOAT, p6 DOUBLE, p7 STRING, p8 Date, p9 DateTime, p10 Interval, "
        "p11 List, p12 Map, PRIMARY KEY (p1));"
    )
    # TODO: support "SHOW TABLES"?
    result = conn.execute("SHOW TABLES;")
    assert len(result) == 1
    conn.close()
    db.close()


# DB-003-02
@pytest.mark.skip(reason="DML is not fully supported yet")
def test_insert_basic_type_check(tmp_path):
    db_dir = tmp_path / "insert_basic_type"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT32, i64 INT64, u32 UINT32, u64 UINT64, "
        "f FLOAT, d DOUBLE, s STRING, PRIMARY KEY (id));"
    )
    # data insert
    conn.execute(
        "CREATE (t:person {id: 1, i64: 1234567890123, u32: 123, u64: 456, f: 1.23, d: 4.56, "
        "s: 'abc'});"
    )
    # INT32 invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 'abc'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # INT64 invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 2, i64: 'bad'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # UNSIGNED invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 3, u32: -1})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # FLOAT invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 4, f: 'bad'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(reason="not all types supported yet")
def test_insert_type_check(tmp_path):
    db_dir = tmp_path / "insert_type"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE T("
        "id INT32, i64 INT64, u32 UINT32, u64 UINT64, "
        "f FLOAT, d DOUBLE, s STRING, dt Date, dttm DateTime, ivl Interval, "
        "l List, m Map, PRIMARY KEY(id));"
    )
    # data insert
    conn.execute(
        "CREATE (t:T {id: 1, i64: 1234567890123, u32: 123, u64: 456, f: 1.23, d: 4.56, "
        "s: 'abc', dt: date('2023-01-01'), dttm: datetime('2023-01-01T12:00:00'), "
        "ivl: interval('1 year'), l: [1,2], m: {'k':1}});"
    )
    # INT32 invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 'abc'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # INT64 invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 1, i64: 'bad'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # UNSIGNED invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 2, u32: -1})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # FLOAT invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 3, f: 'bad'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # DATE invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 4, dt: 'notadate'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # DATETIME invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 5, dttm: 'notadatetime'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    # INTERVAL invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 6, ivl: 'notaninterval'})")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-03
@pytest.mark.skip(reason="expressions not yet supported")
def test_return_expression(tmp_path):
    db_dir = tmp_path / "expr"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    result = conn.execute("RETURN 1+2, date('2023-01-01'), interval('1 year 2 days');")
    assert result is not None
    assert len(result) == 1
    row = result.__next__()
    assert row[0] == 3  # 1 + 2
    assert row[1] == "2023-01-01"  # Date
    assert row[2] == "1 year 2 days"  # Interval
    conn.close()
    db.close()


# DB-003-04
def test_create_node_table(tmp_path):
    db_dir = tmp_path / "create_node"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY (name));"
    )
    # conn.execute("CREATE NODE TABLE city(name STRING, PRIMARY KEY (name));")
    conn.close()
    db.close()


def test_create_node_table_with_default_value(tmp_path):
    db_dir = tmp_path / "create_node_with_default"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person (name STRING, age INT64 DEFAULT 0, PRIMARY KEY (name));"
    )
    conn.close()
    db.close()


# TODO: error codes are not correctly handled yet
def test_create_node_table_errors(tmp_path):
    db_dir = tmp_path / "create_node_errors"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY (name));"
    )
    # 1. create duplicate node table
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY (name));")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. create node table without primary key
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE person1(name STRING, age INT64);")
    assert str("Failed to compile DDL plan") in str(excinfo.value)
    # 3. create node table with invalid property value
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE NODE TABLE person2(name STRING, age INT64 DEFAULT 'abc', PRIMARY KEY (name));"
        )
    assert str("Failed to compile DDL plan") in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-05
def test_create_rel_table(tmp_path):
    db_dir = tmp_path / "create_rel"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    # create single relationship edge table
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    conn.close()
    db.close()


@pytest.mark.skip(reason="not supported yet")
def test_create_rel_table_with_multiple_relationships(tmp_path):
    db_dir = tmp_path / "create_rel_multiple"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute("CREATE NODE TABLE city(name STRING, PRIMARY KEY(name));")
    # create edge table with multiple relationships
    conn.execute(
        "CREATE REL TABLE worksAt(FROM person TO city, FROM person TO person);"
    )
    conn.close()
    db.close()


# TODO: error codes are not correctly handled yet
def test_create_rel_table_errors(tmp_path):
    db_dir = tmp_path / "create_rel_errors"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # 1. create duplicate edge table
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_MANY);"
        )
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. create edge table without FROM/TO vertex tables
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE REL TABLE NewFollows(FROM person TO user, MANY_MANY);")
    assert str("Failed to compile DDL plan") in str(excinfo.value)
    conn.close()
    db.close()


# TODO: confirm if this is as expected behavior
def test_create_duplicated_rel_table_between_same_vertex_tables(tmp_path):
    db_dir = tmp_path / "create_duplicated_rel"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, MANY_MANY);")
    conn.close()
    db.close()


# DB-003-06 DDL-ALTER TABLE
@pytest.mark.skip(reason="success in commit 2e71a77, but failed after commit 92b3d80")
def test_alter_vertex_table(tmp_path):
    db_dir = tmp_path / "alter_table"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    # 1. add property
    # correctly add a new property
    conn.execute("ALTER TABLE person ADD grade INT64;")
    # incorrectly add a property that already exists
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person ADD age INT64;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    conn.execute("ALTER TABLE person RENAME age TO newAge;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person RENAME age1 TO newAge1;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 3. drop property
    # correctly drop a property
    conn.execute("ALTER TABLE person DROP newAge;")
    # incorrectly drop a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person DROP age1;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(reason="success in commit 2e71a77, but failed after commit 92b3d80")
def test_alter_edge_table(tmp_path):
    db_dir = tmp_path / "alter_edge_table"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # 1. add property
    # correctly add a new property
    conn.execute("ALTER TABLE knows ADD since INT64;")
    # incorrectly add a property that already exists
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows ADD weight DOUBLE;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    conn.execute("ALTER TABLE knows RENAME weight TO newWeight;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows RENAME weight1 TO newWeight1;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(reason="drop property on edge table not yet supported")
def test_alter_edge_table_drop_property(tmp_path):
    db_dir = tmp_path / "alter_edge_table_drop_property"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # correctly drop a property
    conn.execute("ALTER TABLE knows DROP weight;")
    # incorrectly drop a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows DROP weight1;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-07 DDL-DROP TABLE
def test_drop_table(tmp_path):
    db_dir = tmp_path / "drop_table"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # 1. DROP edge table
    conn.execute("DROP TABLE knows;")
    # 2. DROP vertex table
    conn.execute("DROP TABLE person;")


# TODO: Currently drop vertex table will also drop all edges connected to it by default.
# Is this the expected behavior?
def test_drop_table_errors(tmp_path):
    db_dir = tmp_path / "drop_table_errors"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    # 1. DROP vertex table will also drop all edges connected to it by default
    conn.execute("DROP TABLE person;")
    # the edge table has already been dropped, so this will fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("DROP TABLE knows;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    # 2. DROP table that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("DROP TABLE person;")
    assert str(ERR_INVALID_SCHEMA) in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-08 DML-create node
@pytest.mark.skip(reason="DML not fully supported yet")
def test_insert_node(tmp_path):
    db_dir = tmp_path / "insert_node"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    # 准备schema
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    # case 1: insert with all properties
    conn.execute("CREATE (u:person{name:'Alice',age:35});")
    # case 2: insert with partial properties, age should be NULL
    conn.execute("CREATE (u:person{name:'Josh'});")
    # case 3: insert without primary key value, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{age:36});")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    # case 4: duplicate primary key value, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{name:'Alice', age:26});")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    # case 5: insert values inconsistent with schema, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{name:'Alice', age:26, addr:'aa'});")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-09 DML-create edge
@pytest.mark.skip(reason="DML not fully supported yet")
def test_insert_edge(tmp_path):
    db_dir = tmp_path / "insert_edge"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, since INT64, MANY_MANY);"
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
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    # case 6: edge property schema mismatch
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE (u:person {name: 'Alice'})-[:follows {nonprop:2022}]->(b:person {name: 'Josh2'});"
        )
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-10 DML-SET node property
@pytest.mark.skip(reason="DML not fully supported yet")
def test_set_node_property(tmp_path):
    db_dir = tmp_path / "set_node_prop"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE (u:person{name:'Alice',age:35});")
    # case 1: valid update
    result = conn.execute(
        "MATCH (u:person) WHERE u.name = 'Alice' SET u.age = 50 RETURN u.*;"
    )
    assert result.__next__()[1] == 50
    # case 2: update property to NULL
    result = conn.execute(
        "MATCH (u:person) WHERE u.name = 'Alice' SET u.age = NULL RETURN u.*;"
    )
    assert result.__next__()[1] is None
    # case 3: add new property
    result = conn.execute(
        "MATCH (u) SET u.population = 0 RETURN label(u), u.name, u.population;"
    )
    assert result.__next__()[2] == 0
    # case 4: update non-existing node, should not affect anything
    result = conn.execute(
        "MATCH (u:person) WHERE u.name = 'nobody' SET u.age = 50 RETURN u.*;"
    )
    assert len(result) == 0
    # case 5: update with property schema mismatch, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "MATCH (u:person) WHERE u.name = 'Alice' SET u.addr = '' RETURN u.*;"
        )
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-11 DML-SET edge property
@pytest.mark.skip(reason="DML not fully supported yet")
def test_set_edge_property(tmp_path):
    db_dir = tmp_path / "set_edge_prop"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, since INT64, MANY_MANY);"
    )
    conn.execute("CREATE REL TABLE likes(FROM person TO person, since INT64);")
    conn.execute("CREATE (u:person{name:'Alice'});")
    conn.execute("CREATE (u:person{name:'Josh'});")
    conn.execute(
        "MATCH (u1:person), (u2:person) WHERE u1.name = 'Alice' AND"
        " u2.name = 'Josh' CREATE (u1)-[:follows {since: 2011}]->(u2);"
    )
    # case 1: valid update with single label relationship
    result = conn.execute(
        "MATCH (u0:person)-[f:follows]->(u1:person) WHERE u0.name = 'Alice' AND u1.name = 'Josh' SET f.since = 1999 RETURN f;"
    )
    assert result.__next__()[0] == 1999
    # case 2: valid update with multiple label relationship
    result = conn.execute(
        "MATCH (u0)-[f]->() WHERE u0.name = 'Alice' SET f.since = 1999 RETURN f;"
    )
    assert result.__next__()[0] == 1999
    assert result.__next__()[1] == 1999
    # case 3: update with property schema mismatch, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "MATCH (u0)-[f]->() WHERE u0.name = 'Alice' SET f.noprop = 1999 RETURN f;"
        )
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-13
@pytest.mark.skip(reason="DML not fully supported yet")
def test_query_sync(tmp_path):
    db_dir = tmp_path / "query_sync"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    conn.execute("INSERT INTO T(id) VALUES (1);")
    result = conn.execute("MATCH (n:T) RETURN n;")
    assert result is not None
    conn.close()
    db.close()


@pytest.mark.skip(reason="async_execute not implemented yet")
def test_query_async(tmp_path):
    db_dir = tmp_path / "query_async"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    fut = conn.async_execute("MATCH (n) RETURN n;")
    result = fut.result()
    assert result is not None
    conn.close()
    db.close()


# DB-003-20
def test_query_syntax_error(tmp_path):
    db_dir = tmp_path / "syntax_error"
    db_dir.mkdir()
    db = Database(str(db_dir), "w", 0, "gopt", "", "")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        conn.execute("MATCH (n RETURN n;")
    assert str(ERR_COMPILATION) in str(excinfo.value)
    conn.close()
    db.close()
