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
import shutil
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from neug import Session
from neug.database import Database
from neug.proto.error_pb2 import ERR_COMPILATION
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_INVALID_SCHEMA
from neug.proto.error_pb2 import ERR_QUERY_SYNTAX
from neug.proto.error_pb2 import ERR_SCHEMA_MISMATCH
from neug.proto.error_pb2 import ERR_TYPE_CONVERSION
from neug.proto.error_pb2 import ERR_TYPE_OVERFLOW


# DB-003-01
def test_create_schema_basic_types(tmp_path):
    db_dir = tmp_path / "schema_basic_types"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE PERSON(int32_prop INT32, uint32_prop UINT32, "
        "int64_prop INT64, uint64_prop UINT64, string_prop STRING, "
        "bool_prop BOOL, float_prop FLOAT, double_prop DOUBLE, "
        "PRIMARY KEY(int32_prop));"
    )

    conn.execute(
        "CREATE (n:PERSON {int32_prop: 1, uint32_prop: 2, "
        "int64_prop: 3, uint64_prop: 4, string_prop: 'test', "
        "bool_prop: true, float_prop: 1.23, double_prop: 2.34});"
    )

    result = conn.execute(
        "MATCH (n:PERSON) RETURN n.int32_prop, n.uint32_prop, "
        "n.int64_prop, n.uint64_prop, n.string_prop, "
        "n.bool_prop, n.float_prop, n.double_prop;"
    )
    record = result.__next__()
    assert record[0] == 1
    assert record[1] == 2
    assert record[2] == 3
    assert record[3] == 4
    assert record[4] == "test"
    assert record[5] is True
    assert (record[6] == 1.23) or (abs(record[6] - 1.23) < 1e-6)  # float comparison
    assert (record[7] == 2.34) or (abs(record[7] - 2.34) < 1e-6)  # double comparison

    conn.close()
    db.close()


def test_create_schema_float_types(tmp_path):
    db_dir = tmp_path / "schema_basic_types"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE PERSON(int32_prop INT32, float_prop FLOAT, "
        "PRIMARY KEY(int32_prop));"
    )

    conn.execute("CREATE (n:PERSON {int32_prop: 1, float_prop: 2.3});")

    result = conn.execute("MATCH (n:PERSON) RETURN *;")
    assert result is not None and len(result) == 1

    result = conn.execute(
        "MATCH (n:PERSON) WHERE n.float_prop > 4.5 RETURN n.float_prop;"
    )

    conn.close()
    db.close()


@pytest.mark.skip(reason="complex types are not supported yet")
def test_create_schema_complex_types(tmp_path):
    db_dir = tmp_path / "schema_types"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE Type (p1 INT32, p8 Date, p9 DateTime, p10 Interval, "
        "p11 List, p12 Map, PRIMARY KEY (p1));"
    )
    conn.close()
    db.close()


# DB-003-02
def test_insert_basic_type_check(tmp_path):
    db_dir = tmp_path / "insert_basic_type"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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
    # TODO(shirly): throw ERR_TYPE_CONVERSION instead of BinderException
    # assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    assert str(ERR_COMPILATION) in str(excinfo.value)
    # INT64 invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 2, i64: 'bad'})")
    # assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # TODO(shirly): throw ERR_TYPE_CONVERSION instead of BinderException
    assert str(ERR_COMPILATION) in str(excinfo.value)
    # UNSIGNED invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 3, u32: -1})")
    # assert str(ERR_TYPE_OVERFLOW) in str(excinfo.value)
    # TODO(shirly): throw ERR_TYPE_OVERFLOW instead of BinderException
    assert str(ERR_COMPILATION) in str(excinfo.value)
    # FLOAT invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 4, f: 'bad'})")
    # assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # TODO(shirly): throw ERR_TYPE_CONVERSION instead of BinderException
    assert str(ERR_COMPILATION) in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(reason="not all types supported yet")
def test_insert_type_check(tmp_path):
    db_dir = tmp_path / "insert_type"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # INT64 invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 1, i64: 'bad'})")
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # UNSIGNED invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 2, u32: -1})")
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # FLOAT invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 3, f: 'bad'})")
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # DATE invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 4, dt: 'notadate'})")
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # DATETIME invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 5, dttm: 'notadatetime'})")
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # INTERVAL invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:T {id: 6, ivl: 'notaninterval'})")
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-03
def test_return_expression():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    result = conn.execute(
        "Match (n) RETURN 1+2, date('2023-01-01'), interval('1 year 2 days') limit 1;"
    )
    assert result is not None
    print(result)
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
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY (name));"
    )
    # conn.execute("CREATE NODE TABLE city(name STRING, PRIMARY KEY (name));")
    conn.close()
    db.close()


def test_create_node_table_with_default_value(tmp_path):
    db_dir = tmp_path / "create_node_with_default"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person (name STRING, age INT64 DEFAULT 0, PRIMARY KEY (name));"
    )
    conn.close()
    db.close()


def test_create_node_table_errors(tmp_path):
    db_dir = tmp_path / "create_node_errors"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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
    assert str(ERR_QUERY_SYNTAX) in str(excinfo.value)
    # 3. create node table with invalid property value
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE NODE TABLE person2(name STRING, age INT64 DEFAULT 'abc', PRIMARY KEY (name));"
        )
    assert str(ERR_COMPILATION) in str(excinfo.value)
    conn.close()
    db.close()


# DB-003-05
def test_create_rel_table(tmp_path):
    db_dir = tmp_path / "create_rel"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute("CREATE NODE TABLE city(name STRING, PRIMARY KEY(name));")
    # create edge table with multiple relationships
    conn.execute(
        "CREATE REL TABLE worksAt(FROM person TO city, FROM person TO person);"
    )
    conn.close()
    db.close()


def test_create_rel_table_errors(tmp_path):
    db_dir = tmp_path / "create_rel_errors"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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
    assert str(ERR_COMPILATION) in str(excinfo.value)
    conn.close()
    db.close()


# TODO: confirm if this is as expected behavior
def test_create_duplicated_rel_table_between_same_vertex_tables(tmp_path):
    db_dir = tmp_path / "create_duplicated_rel"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_MANY);"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, MANY_MANY);")
    conn.close()
    db.close()


# DB-003-06 DDL-ALTER TABLE
def test_alter_vertex_table(tmp_path):
    db_dir = tmp_path / "alter_table"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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


def test_alter_edge_table(tmp_path):
    db_dir = tmp_path / "alter_edge_table"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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


def test_alter_edge_table_drop_property(tmp_path):
    db_dir = tmp_path / "alter_edge_table_drop_property"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
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
    assert str(ERR_QUERY_SYNTAX) in str(excinfo.value)
    # case 4: duplicate primary key value, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{name:'Alice', age:26});")
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # case 5: insert values inconsistent with schema, should fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (u:person{name:'Alice', age:26, addr:'aa'});")
    assert str(ERR_QUERY_SYNTAX) in str(excinfo.value)
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
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # case 6: edge property schema mismatch
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE (u:person {name: 'Alice2'})-[:follows {nonprop:2022}]->(b:person {name: 'Josh2'});"
        )
    assert str(ERR_COMPILATION) in str(excinfo.value)
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


# DB-003-11 DML-SET edge property
def test_set_multi_edge_property(tmp_path):
    db_dir = tmp_path / "set_multi_edge_prop"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute("CREATE NODE TABLE software(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE create_software(FROM person TO software, since INT64, weight DOUBLE, MANY_MANY);"
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


# DB-003-11 DML-SET edge property
def test_set_edge_property(tmp_path):
    db_dir = tmp_path / "set_edge_prop"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute(
        "CREATE REL TABLE follows(FROM person TO person, since INT64, MANY_MANY);"
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


# DB-003-12
def test_query_sync():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=str(db_dir), mode="r")
    conn = db.connect()
    result = conn.execute("MATCH (n) RETURN n;")
    assert len(result) == 6
    conn.close()
    db.close()


@pytest.mark.asyncio
async def test_query_async():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=str(db_dir), mode="r")
    conn = db.async_connect()
    result = await conn.execute("MATCH (n) RETURN n;")
    assert len(result) == 6
    conn.close()
    db.close()


# DB-003-20
def test_query_syntax_error(tmp_path):
    db_dir = tmp_path / "syntax_error"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        conn.execute("MATCH (n RETURN n;")
    assert str(ERR_QUERY_SYNTAX) in str(excinfo.value)
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


# DB-003-23
def test_complex_example(tmp_path):
    db_dir = tmp_path / "complex_example"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    # Create schema
    conn.execute(
        """
        CREATE NODE TABLE Person(
            id INT64 PRIMARY KEY,
            name STRING,
            age INT32,
            email STRING
        )
    """
    )

    conn.execute(
        """
        CREATE NODE TABLE Company(
            id INT64 PRIMARY KEY,
            name STRING,
            industry STRING,
            founded_year INT32
        )
    """
    )

    # Create edge tables
    conn.execute(
        """
        CREATE REL TABLE WORKS_FOR(
            FROM Person TO Company,
            position STRING,
            start_date DATE,
            salary DOUBLE
        )
    """
    )

    conn.execute(
        """
        CREATE REL TABLE KNOWS(
            FROM Person TO Person,
            since_year INT32,
            relationship_type STRING
        )
    """
    )

    conn.execute(
        """
    CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})
    """
    )

    conn.execute(
        """
        CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})
    """
    )

    conn.execute(
        """
        CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})
    """
    )

    # Insert relationships
    conn.execute(
        """
        MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 1
        CREATE (p)-[:WORKS_FOR {position: 'Software Engineer', start_date: date('2020-01-15'), salary: 75000.0}]->(c)
    """
    )

    conn.execute(
        """
        MATCH (p1:Person {id: 1}), (p2:Person {id: 2})
        CREATE (p1)-[:KNOWS {since_year: 2018, relationship_type: 'colleague'}]->(p2)
    """
    )

    conn.close()

    service_endpoint = db.serve(host="localhost", port=10000)
    print(f"Serving database at {service_endpoint}")

    session = Session("http://localhost:10000/")

    session.execute(
        """
        CREATE NODE TABLE User(
            id INT64 PRIMARY KEY,
            username STRING,
            created_at TIMESTAMP
        )
    """
    )

    session.execute(
        """
        CREATE (u:User {id: 1, username: 'user1', created_at: timestamp('2024-01-01 10:00:00')})
    """
    )

    result = session.execute("MATCH (u:User) RETURN u.username, u.created_at")
    for record in result:
        print(f"User: {record[0]}, Created: {record[1]}")

    session.close()

    db.stop_serving()


# DB-003-24
def test_query_on_empty_graph():
    db = Database()
    conn = db.connect()
    res = conn.execute("MATCH (n) RETURN n;")
    assert res is not None and len(res) == 0


def test_join_queries():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=str(db_dir), mode="r")
    conn = db.connect()
    res = conn.execute(
        """
        MATCH (a:person), (b:person) WHERE a.ID = b.ID AND a.ID = 1 RETURN a.id, b.id, a.age, b.age;
        """
    )
    assert res.__next__() == [1, 1, 29, 29]

    res = conn.execute(
        """
        MATCH (a:person) WHERE a.name = 'marko' OPTIONAL MATCH (b:person) WHERE b.name = 'm' RETURN a.ID, b.ID;
        """
    )
    assert res.__next__() == [1, None]


def test_path_expand():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=str(db_dir), mode="r")
    conn = db.connect()
    # Test path expansion with a simple query
    result = conn.execute("MATCH (p:person)-[k*1..2]->(f) RETURN k;")
    assert result is not None
    expected_result = [
        [
            [
                {"_ID": 0, "_LABEL": "person"},
                {
                    "_ID": 1,
                    "_LABEL": "knows",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "person",
                    "_SRC_ID": 0,
                    "_DST_ID": 1,
                },
                {"_ID": 1, "_LABEL": "person"},
            ]
        ],
        [
            [
                {"_ID": 0, "_LABEL": "person"},
                {
                    "_ID": 2,
                    "_LABEL": "knows",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "person",
                    "_SRC_ID": 0,
                    "_DST_ID": 2,
                },
                {"_ID": 2, "_LABEL": "person"},
            ]
        ],
        [
            [
                {"_ID": 0, "_LABEL": "person"},
                {
                    "_ID": 4294967296,
                    "_LABEL": "created",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "software",
                    "_SRC_ID": 0,
                    "_DST_ID": 72057594037927936,
                },
                {"_ID": 72057594037927936, "_LABEL": "software"},
            ]
        ],
        [
            [
                {"_ID": 2, "_LABEL": "person"},
                {
                    "_ID": 4297064448,
                    "_LABEL": "created",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "software",
                    "_SRC_ID": 2,
                    "_DST_ID": 72057594037927936,
                },
                {"_ID": 72057594037927936, "_LABEL": "software"},
            ]
        ],
        [
            [
                {"_ID": 2, "_LABEL": "person"},
                {
                    "_ID": 4297064449,
                    "_LABEL": "created",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "software",
                    "_SRC_ID": 2,
                    "_DST_ID": 72057594037927937,
                },
                {"_ID": 72057594037927937, "_LABEL": "software"},
            ]
        ],
        [
            [
                {"_ID": 3, "_LABEL": "person"},
                {
                    "_ID": 4298113024,
                    "_LABEL": "created",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "software",
                    "_SRC_ID": 3,
                    "_DST_ID": 72057594037927936,
                },
                {"_ID": 72057594037927936, "_LABEL": "software"},
            ]
        ],
        [
            [
                {"_ID": 0, "_LABEL": "person"},
                {
                    "_ID": 2,
                    "_LABEL": "knows",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "person",
                    "_SRC_ID": 0,
                    "_DST_ID": 2,
                },
                {"_ID": 2, "_LABEL": "person"},
                {
                    "_ID": 4297064448,
                    "_LABEL": "created",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "software",
                    "_SRC_ID": 2,
                    "_DST_ID": 72057594037927936,
                },
                {"_ID": 72057594037927936, "_LABEL": "software"},
            ]
        ],
        [
            [
                {"_ID": 0, "_LABEL": "person"},
                {
                    "_ID": 2,
                    "_LABEL": "knows",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "person",
                    "_SRC_ID": 0,
                    "_DST_ID": 2,
                },
                {"_ID": 2, "_LABEL": "person"},
                {
                    "_ID": 4297064449,
                    "_LABEL": "created",
                    "_SRC_LABEL": "person",
                    "_DST_LABEL": "software",
                    "_SRC_ID": 2,
                    "_DST_ID": 72057594037927937,
                },
                {"_ID": 72057594037927937, "_LABEL": "software"},
            ]
        ],
    ]

    for i, record in enumerate(result):
        assert (
            record[0] == expected_result[i][0]
        ), f"Record {i} does not match expected result"


def test_query_cyclic():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    res = conn.execute(
        """Match (a:person)-[:created]->(b:software), (c:person)-[:created]->(b:software),
           (a:person)-[:knows]->(c:person) Where a.name <> b.name AND b.name <> c.name
           Return count(*);
        """
    )
    assert res.__next__()[0] == 1


def test_no_existing_property():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=str(db_dir), mode="r")
    conn = db.connect()
    res = conn.execute(
        """
        MATCH (a:person)-[e1:knows|:studyAt|:workAt]->(b:person:organisation) WHERE a.age > 35 RETURN b.fName, b.name;
        """
    )
    for record in res:
        print(record)


def test_result_getitem():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=str(db_dir), mode="r")
    conn = db.connect()
    res = conn.execute("MATCH (n) RETURN count(n);")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 6  # Assuming there are 6 nodes
    assert res[-1][0] == 6  # Testing negative indexing


def test_return_literal():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=str(db_dir), mode="r")
    conn = db.connect()
    res = conn.execute("MATCH (a:person) RETURN 1 + 1, label(a) LIMIT 2")
    assert res is not None
    assert len(res) == 2
    assert res[0] == [2, "person"]
    assert res[1] == [2, "person"]  # Assuming there are at
