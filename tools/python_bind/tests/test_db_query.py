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
from unittest import result

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from ctypes import sizeof

from conftest import ensure_result_cnt_eq
from conftest import ensure_result_cnt_gt_zero
from conftest import submit_cypher_query

from neug import Session
from neug.database import Database
from neug.proto.error_pb2 import ERR_COMPILATION
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_INVALID_SCHEMA
from neug.proto.error_pb2 import ERR_PROPERTY_NOT_FOUND
from neug.proto.error_pb2 import ERR_QUERY_SYNTAX
from neug.proto.error_pb2 import ERR_SCHEMA_MISMATCH
from neug.proto.error_pb2 import ERR_TYPE_CONVERSION
from neug.proto.error_pb2 import ERR_TYPE_OVERFLOW

# Import conftest functions for IC tests

logger = logging.getLogger(__name__)


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


def test_session_create_schema_basic_types(tmp_path):
    db_dir = tmp_path / "schema_basic_types"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    endpoint = db.serve(host="127.0.0.1", port=10010, blocking=False)
    sess = Session.open(endpoint=endpoint, timeout="30s", num_threads=5)

    sess.execute(
        "CREATE NODE TABLE PERSON(int32_prop INT32, uint32_prop UINT32, "
        "int64_prop INT64, uint64_prop UINT64, string_prop STRING, "
        "bool_prop BOOL, float_prop FLOAT, double_prop DOUBLE, "
        "PRIMARY KEY(int32_prop));"
    )

    sess.execute(
        "CREATE (n:PERSON {int32_prop: 1, uint32_prop: 2, "
        "int64_prop: 3, uint64_prop: 4, string_prop: 'test', "
        "bool_prop: true, float_prop: 1.23, double_prop: 2.34});"
    )

    result = sess.execute(
        "MATCH (n:PERSON) RETURN n.int32_prop, n.uint32_prop, "
        "n.int64_prop, n.uint64_prop, n.string_prop, "
        "n.bool_prop, n.float_prop, n.double_prop;"
    )
    record = result.__next__()
    logger.info(f"Record: {record}")
    assert record[0] == 1
    assert record[1] == 2
    assert record[2] == 3
    assert record[3] == 4
    assert record[4] == "test"
    assert record[5] is True
    assert (record[6] == 1.23) or (abs(record[6] - 1.23) < 1e-6)  # float comparison
    assert (record[7] == 2.34) or (abs(record[7] - 2.34) < 1e-6)  # double comparison

    sess.close()
    db.stop_serving()
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


# `List` and `Map` are not supported yet
def test_create_schema_complex_types(tmp_path):
    db_dir = tmp_path / "schema_types"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE Type (p1 INT32, p8 Date, p9 Timestamp, p10 Interval, "
        "PRIMARY KEY (p1));"
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
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # INT64 invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 2, i64: 'bad'})")
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    # UNSIGNED invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 3, u32: -1})")
    assert str(ERR_TYPE_OVERFLOW) in str(excinfo.value)
    # FLOAT invalid
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (t:person {id: 4, f: 'bad'})")
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    conn.close()
    db.close()


def test_insert_type_check(tmp_path):
    db_dir = tmp_path / "insert_type"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE T ("
        "id INT32, i64 INT64, u32 UINT32, u64 UINT64, "
        "f FLOAT, d DOUBLE, s STRING, dt Date, tms Timestamp, ivl Interval, "
        "PRIMARY KEY(id));"
    )
    # data insert
    conn.execute(
        "CREATE (t:T {id: 1, i64: 1234567890123, u32: 123, u64: 456, f: 1.23, d: 4.56, "
        "s: 'abc', dt: date('2023-01-01'), tms: Timestamp('2023-01-01 12:00:00'), "
        "ivl: interval('1 year')});"
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
    assert str(ERR_TYPE_OVERFLOW) in str(excinfo.value)
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
    assert str(ERR_PROPERTY_NOT_FOUND) in str(excinfo.value)
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
    assert str(row[1]) == "2023-01-01"  # Date
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
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # 2. create node table without primary key
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE person1(name STRING, age INT64);")
    assert str(ERR_QUERY_SYNTAX) in str(excinfo.value)
    # 3. create node table with invalid property value
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE NODE TABLE person2(name STRING, age INT64 DEFAULT 'abc', PRIMARY KEY (name));"
        )
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
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


def test_create_rel_table_with_multiple_src_dst(tmp_path):
    db_dir = tmp_path / "create_rel_multi_src_dst"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(name STRING, PRIMARY KEY(name));")
    conn.execute("CREATE NODE TABLE comment(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE NODE TABLE post(id INT64, PRIMARY KEY(id));")
    # create edge table with multiple src/dst vertex tables
    conn.execute("CREATE REL TABLE likes(FROM person TO comment, FROM person TO post);")
    conn.close()


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
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # 2. create edge table without FROM/TO vertex tables
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE REL TABLE NewFollows(FROM person TO user, MANY_MANY);")
    assert str(ERR_COMPILATION) in str(excinfo.value)
    conn.close()
    db.close()


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
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    conn.execute("ALTER TABLE person RENAME age TO newAge;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person RENAME age1 TO newAge1;")
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # 3. drop property
    # correctly drop a property
    conn.execute("ALTER TABLE person DROP newAge;")
    # incorrectly drop a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person DROP age1;")
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    conn.close()
    db.close()


def test_session_alter_vertex_table(tmp_path):
    db_dir = tmp_path / "alter_table"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    endpoint = db.serve(host="localhost", port=10010, blocking=False)
    sess = Session.open(endpoint=endpoint, timeout="30s", num_threads=5)
    sess.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    # 1. add property
    # correctly add a new property
    sess.execute("ALTER TABLE person ADD grade INT64;")
    # incorrectly add a property that already exists
    with pytest.raises(Exception) as excinfo:
        sess.execute("ALTER TABLE person ADD age INT64;")
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    sess.execute("ALTER TABLE person RENAME age TO newAge;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        sess.execute("ALTER TABLE person RENAME age1 TO newAge1;")
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # 3. drop property
    # correctly drop a property
    sess.execute("ALTER TABLE person DROP newAge;")
    # incorrectly drop a property that does not exist
    with pytest.raises(Exception) as excinfo:
        sess.execute("ALTER TABLE person DROP age1;")
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    sess.close()
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
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    conn.execute("ALTER TABLE knows RENAME weight TO newWeight;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows RENAME weight1 TO newWeight1;")
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
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
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
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
    assert str(ERR_QUERY_SYNTAX) in str(excinfo.value)
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

    service_endpoint = db.serve(host="localhost", port=10010, blocking=False)
    print(f"Serving database at {service_endpoint}")

    session = Session("http://localhost:10010/")

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
    db.close()


# DB-003-24
def test_query_on_empty_graph():
    db = Database()
    conn = db.connect()
    res = conn.execute("MATCH (n) RETURN n;")
    assert res is not None and len(res) == 0


@pytest.mark.skip(reason="TODO(zhanglei,lexiao): get view from invalid vid")
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
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 1, "_LABEL": "person", "id": 2, "name": "vadas", "age": 27},
                ],
                "rels": [
                    {
                        "_ID": 1,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 1,
                        "weight": 0.5,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                ],
                "rels": [
                    {
                        "_ID": 2,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 2,
                        "weight": 1.0,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {
                        "_ID": 72057594037927936,
                        "_LABEL": "software",
                        "id": 3,
                        "name": "lop",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 1103806595072,
                        "_LABEL": "created",
                        "_SRC_ID": 0,
                        "_DST_ID": 72057594037927936,
                        "weight": 0.4,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                    {
                        "_ID": 72057594037927936,
                        "_LABEL": "software",
                        "id": 3,
                        "name": "lop",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 1103808692224,
                        "_LABEL": "created",
                        "_SRC_ID": 2,
                        "_DST_ID": 72057594037927936,
                        "weight": 0.4,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                    {
                        "_ID": 72057594037927937,
                        "_LABEL": "software",
                        "id": 5,
                        "name": "ripple",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 1103808692225,
                        "_LABEL": "created",
                        "_SRC_ID": 2,
                        "_DST_ID": 72057594037927937,
                        "weight": 1.0,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 3, "_LABEL": "person", "id": 6, "name": "peter", "age": 35},
                    {
                        "_ID": 72057594037927936,
                        "_LABEL": "software",
                        "id": 3,
                        "name": "lop",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 1103809740800,
                        "_LABEL": "created",
                        "_SRC_ID": 3,
                        "_DST_ID": 72057594037927936,
                        "weight": 0.2,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                    {
                        "_ID": 72057594037927936,
                        "_LABEL": "software",
                        "id": 3,
                        "name": "lop",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 2,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 2,
                        "weight": 1.0,
                    },
                    {
                        "_ID": 1103808692224,
                        "_LABEL": "created",
                        "_SRC_ID": 2,
                        "_DST_ID": 72057594037927936,
                        "weight": 0.4,
                    },
                ],
                "length": 2,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                    {
                        "_ID": 72057594037927937,
                        "_LABEL": "software",
                        "id": 5,
                        "name": "ripple",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 2,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 2,
                        "weight": 1.0,
                    },
                    {
                        "_ID": 1103808692225,
                        "_LABEL": "created",
                        "_SRC_ID": 2,
                        "_DST_ID": 72057594037927937,
                        "weight": 1.0,
                    },
                ],
                "length": 2,
            }
        ],
    ]
    for i, record in enumerate(result):
        assert (
            record == expected_result[i]
        ), f"Record {i} does not match expected result"

    result = conn.execute(
        """MATCH (p:person {name: 'marko'})-[k:knows*1..2 (r, _ | WHERE r.weight <= 1.0)]->(f:person) Return k;"""
    )
    assert result is not None
    expected_result = [
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 1, "_LABEL": "person", "id": 2, "name": "vadas", "age": 27},
                ],
                "rels": [
                    {
                        "_ID": 1,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 1,
                        "weight": 0.5,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                ],
                "rels": [
                    {
                        "_ID": 2,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 2,
                        "weight": 1.0,
                    }
                ],
                "length": 1,
            }
        ],
    ]
    for i, record in enumerate(result):
        assert (
            record == expected_result[i]
        ), f"Record {i} does not match expected result"

    conn.close()
    db.close()


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


def test_count():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=str(db_dir), mode="r")
    conn = db.connect()
    res = conn.execute("MATCH (n) RETURN count(n)")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 14

    # test count edges
    res = conn.execute("MATCH ()-[e]->() RETURN count(e)")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 30

    res = conn.execute("MATCH ()-[e]-() RETURN count(e)")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 60


def test_list_return_basic(tmp_path):
    """Test basic list return functionality: RETURN [p.name, p.value]"""
    db_dir = tmp_path / "list_return_basic"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    # Create schema with list property
    conn.execute(
        "CREATE NODE TABLE Person ("
        "id INT32 PRIMARY KEY, "
        "name STRING, "
        "value FLOAT"
        ");"
    )

    # Insert test data
    conn.execute("CREATE (p:Person {id: 1, name: 'Alice', value: 1.11});")
    conn.execute("CREATE (p:Person {id: 2, name: 'Bob', value: 2.22});")
    conn.execute("CREATE (p:Person {id: 3, name: 'Charlie', value: 3.33});")

    # Test basic list return
    result = conn.execute("MATCH (p:Person) RETURN [p.name, p.value] ORDER BY p.id;")

    records = list(result)
    assert len(records) == 3
    assert records[0][0][0] == "Alice"
    assert records[1][0][0] == "Bob"
    assert records[2][0][0] == "Charlie"
    assert abs(records[0][0][1] - 1.11) < 1e-5
    assert abs(records[1][0][1] - 2.22) < 1e-5
    assert abs(records[2][0][1] - 3.33) < 1e-5
    conn.close()
    db.close()


@pytest.mark.skip(
    reason="TODO(zhanglei,lexiao): get prop from invalid vertex: column.h:570]"
    "Check failed: index < basic_size Index out of range: 4294967295 >= 4096"
)
def test_optional_match():
    db_dir = "/tmp/optional_match"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT32, PRIMARY KEY(id));")
    conn.execute("CREATE (p: Person {id: 1});")
    conn.execute("CREATE (p: Person {id: 2});")
    conn.execute("CREATE NODE TABLE Company(id INT32, PRIMARY KEY(id));")
    conn.execute("CREATE (c: Company {id: 1001});")
    conn.execute("CREATE REL TABLE WorkAt(FROM Person TO Company);")
    conn.execute(
        "MATCH (p:Person) WHERE p.id = 1"
        "MATCH (c:Company) WHERE c.id = 1001"
        "CREATE (p)-[:WorkAt]->(c);"
    )

    # ok
    res = conn.execute(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WorkAt]->(c:Company) RETURN p.id, c.id;"
    )
    res = list(res)
    assert len(res) == 2
    assert res[0] == [1, 1001]
    assert res[1] == [2, None]

    # return 1
    res = conn.execute(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WorkAt]->(c:Company) RETURN COUNT(c);"
    )
    res = list(res)
    assert res[0] == [1]

    res = conn.execute(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WorkAt]->(c:Company) RETURN COUNT(*);"
    )
    res = list(res)
    assert res[0] == [2]

    res = conn.execute(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WorkAt]->(c:Company) RETURN COUNT(p);"
    )
    res = list(res)
    assert res[0] == [2]

    conn.close()
    db.close()


def test_create_edge_with_prop_on_both_end():
    db_dir = "/tmp/test_create_edge_with_prop_on_both_end"
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
        MATCH (p1: Person {id: 111})-[k: Knows]-(p2:Person {id: 222})
        RETURN k.id
        """
    )


def test_copy_from():
    db_dir = "/tmp/test_copy_from"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
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


def test_tinysnb_path_expand():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        """
        MATCH (n:Person)-[:Meets*1..2]->(m:Person) return count(*);
        """
    )
    records = list(result)
    assert len(records) == 1
    assert records[0][0] == 13


def test_path_expand_with_filter():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt*2..2]->(b) WHERE (a.ID = 0) RETURN a.ID, b.ID"
    )
    records = list(result)
    assert records == [[0, 1], [0, 5], [0, 1], [0, 5]]

    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt*2..2]->(b) WHERE ((b.ID < 5)) AND (a.ID = 0) RETURN a.ID, b.ID"
    )
    records = list(result)
    assert records == [[0, 1], [0, 1]]

    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt*2..2]->(b) WHERE (b.ID < 5) RETURN a.ID, b.ID"
    )
    records = list(result)
    assert records == [[3, 3], [7, 3], [0, 1], [10, 1], [0, 1], [7, 1]]


def test_edge_expand_with_filter():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt]->(b) WHERE (a.ID = 0) RETURN a.ID, b.ID,label(e)"
    )
    records = list(result)
    sorted_ = sorted(records, key=lambda x: (x[1], x[2]))
    assert sorted_ == [[0, 1, "studyAt"], [0, 2, "marries"], [0, 2, "meets"]]

    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt]->(b) WHERE ((b.ID > 1)) AND (a.ID = 0) RETURN a.ID, b.ID,label(e);"
    )
    records = list(result)
    assert records == [[0, 2, "meets"], [0, 2, "marries"]]

    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt]->(b) WHERE (b.ID > 5) RETURN a.ID, b.ID"
    )
    records = list(result)
    assert records == [[3, 7], [7, 8]]


def test_upper():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute("MATCH (n:person) RETURN UPPER(n.name)")
    expected = {"MARKO", "VADAS", "JOSH", "PETER"}
    actual = {record[0] for record in result}
    assert actual == expected, f"Expected {expected}, got {actual}"
    conn.close()
    db.close()


def test_lower():
    """Test the LOWER() function on constant strings."""
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "RETURN LOWER('MARKO'), LOWER('VaDaS'), LOWER('Josh'), LOWER('PETER')"
    )
    row = next(iter(result))
    expected = ("marko", "vadas", "josh", "peter")
    assert tuple(row) == expected, f"Expected {expected}, got {row}"
    conn.close()
    db.close()


def test_reverse():
    """Test the REVERSE() function on Person names using /tmp/modern_graph."""
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute("MATCH (n:person) RETURN n.name, REVERSE(n.name)")
    expected_map = {
        "marko": "okram",
        "vadas": "sadav",
        "josh": "hsoj",
        "peter": "retep",
    }
    for record in result:
        original, reversed_str = record
        expected = expected_map[original]
        assert (
            reversed_str == expected
        ), f"Expected {expected} for {original}, got {reversed_str}"
    conn.close()
    db.close()


def test_distinct():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "MATCH (a:person)-[:knows]->(c:person) Return distinct a.fName;"
    )
    records = list(result)
    print(records)
    assert records == [["Alice"], ["Bob"], ["Carol"], ["Dan"], ["Elizabeth"]]
    conn.close()
    db.close()


def test_length():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "Match (n:PERSON {id: 933})-[k:KNOWS*1..3]->(m) Return LENGTH(k) as len Order by len Limit 1"
    )
    for record in result:
        assert record[0] == 1, f"Expected value 1, got {record[0]}"
    result = conn.execute(
        """
    MATCH (:TAGCLASS {name: "OfficeHolder"})<-[:HASTYPE]-(:TAG)<-[:HASTAG]-(message)-[:REPLYOF*0..30]->(p:POST)
        RETURN count(p) AS numPosts"""
    )
    for record in result:
        assert record[0] == 19519, f"Expected value 19519, got {record[0]}"
    conn.close()
    db.close()


def test_nodes_rels():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    submit_cypher_query(
        conn=conn,
        query="Match (n:PERSON {id: 933})-[k:KNOWS*1..3]-(m:PERSON {id: 2199023256668})"
        " Return nodes(k) as n1, rels(k) as n2 LIMIT 1;",
        lambda_func=ensure_result_cnt_gt_zero,
    )
    conn.close()
    db.close()


def test_case_expression():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "Match (n:PERSON {id: 933}) Return CASE WHEN n.id > 0 THEN n.id ELSE 0 END"
    )
    for record in result:
        assert record[0] == 933, f"Expected value 933, got {record[0]}"
    conn.close()
    db.close()


# test to_tuple function
# todo(engine): VariableKeys is deprecated by ToTuple in PB.
def test_to_tuple():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    submit_cypher_query(
        conn=conn,
        query="Match (n:PERSON {id: 933})"
        " Return [n.firstName, n.gender, n.birthday] as n2 LIMIT 1;",
        lambda_func=ensure_result_cnt_gt_zero,
    )
    conn.close()
    db.close()


# test dummy scan before projection
def test_dummy_scan():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute("Return 1002")
    for record in result:
        assert record[0] == 1002, f"Expected value 1002, got {record[0]}"
    conn.close()
    db.close()


def test_nested_tuple():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute("Match (n {name: 'marko'}) Return [[n.name, n.age], n.id]")
    for record in result:
        assert record[0] == [
            ["marko", 29],
            1,
        ], f"Expected value '[['marko', 29], 1]', got {record[0]}"
    conn.close()
    db.close()


def test_null_value_tuple():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute("Match (n {name: 'lop'}) Return [n.name, n.age]")
    for record in result:
        assert record[0] == [
            "lop",
            None,
        ], f"Expected value '['lop', None]', got {record[0]}"
    conn.close()
    db.close()


def test_starts_with():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    # todo: property value of `age` is null, engine will fail if the tuple contains null value
    result = conn.execute("Match (n) Where n.name starts with 'mar' Return n.name")
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result[0][0] == "marko", f"Expected value 'marko', got {result[0][0]}"
    conn.close()
    db.close()


def test_ends_with():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    # todo: property value of `age` is null, engine will fail if the tuple contains null value
    result = conn.execute("Match (n) Where n.name ends with 'rko' Return n.name")
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result[0][0] == "marko", f"Expected value 'marko', got {result[0][0]}"
    conn.close()
    db.close()


def test_contains():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    # todo: property value of `age` is null, engine will fail if the tuple contains null value
    result = conn.execute("Match (n) Where n.name contains 'ark' Return n.name")
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result[0][0] == "marko", f"Expected value 'marko', got {result[0][0]}"
    conn.close()
    db.close()


def test_ends_with_and_contains_with_slash_in_string(tmp_path):
    """Test that ends with and contains work correctly with strings containing '/'."""
    db_dir = str(tmp_path / "ends_with_contains_slash_db")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE path_node(path STRING, PRIMARY KEY(path));")
    conn.execute("CREATE (n:path_node {path: 'path/to/file'});")
    conn.execute("CREATE (n:path_node {path: 'a/b/c'});")
    conn.execute("CREATE (n:path_node {path: 'no_slash_here'});")
    conn.execute("CREATE (n:path_node {path: 'trailing/'});")

    # Test ends with: should match only 'path/to/file'
    result = conn.execute(
        "MATCH (n:path_node) WHERE n.path ends with '/file' RETURN n.path ORDER BY n.path"
    )
    rows = list(result)
    assert len(rows) == 1, f"Expected 1 row for ends with '/file', got {len(rows)}"
    assert rows[0][0] == "path/to/file", f"Expected 'path/to/file', got {rows[0][0]}"

    result = conn.execute(
        "MATCH (n:path_node) WHERE n.path ends with '/' RETURN n.path ORDER BY n.path"
    )
    rows = list(result)
    assert len(rows) == 1, f"Expected 1 row for ends with '/', got {len(rows)}"
    assert rows[0][0] == "trailing/", f"Expected 'trailing/', got {rows[0][0]}"

    # Test contains: should match all paths that have '/' in them
    result = conn.execute(
        "MATCH (n:path_node) WHERE n.path contains '/' RETURN n.path ORDER BY n.path"
    )
    rows = list(result)
    assert len(rows) == 3, f"Expected 3 rows for contains '/', got {len(rows)}: {rows}"
    assert rows[0][0] == "a/b/c"
    assert rows[1][0] == "path/to/file"
    assert rows[2][0] == "trailing/"

    # contains '/to/' should match only 'path/to/file'
    result = conn.execute(
        "MATCH (n:path_node) WHERE n.path contains '/to/' RETURN n.path"
    )
    rows = list(result)
    assert len(rows) == 1 and rows[0][0] == "path/to/file"

    conn.close()
    db.close()
    shutil.rmtree(db_dir, ignore_errors=True)


def test_date_time_to_string():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        """
    MATCH (m:POST:COMMENT {id: 1030792332314})
    RETURN
        CASE
            WHEN m.content = ""
                THEN m.imageFile
            ELSE m.content END as messageContent,
        m.creationDate as messageCreationDate
    """
    )
    result = list(result)
    from datetime import datetime

    datetime_obj = datetime.strptime("2012-07-23 02:25:02.068", "%Y-%m-%d %H:%M:%S.%f")
    assert result == [["photo1030792332314.jpg", datetime_obj]]


def test_create_interval():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    res = conn.execute("RETURN INTERVAL('5 DAY')")
    for record in res:
        assert record[0] == "5 days", f"Expected value '5 days', got {record[0]}"
    conn.close()
    db.close()


def test_intersect_predicate():
    db_dir = "/tmp/test_intersect_predicate"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE address(id INT32, name STRING, PRIMARY KEY(id))")
    conn.execute("CREATE REL TABLE structure(FROM address TO address, weight DOUBLE)")
    conn.execute("CREATE REL TABLE belong(FROM address TO address, weight DOUBLE)")

    conn.execute("CREATE (u: address {id: 1, name: 'address1' } )")
    conn.execute("CREATE (v: address {id: 2, name: 'address2' } )")
    conn.execute("CREATE (w: address {id: 3, name: 'address3' } )")
    conn.execute("CREATE (x: address {id: 4, name: 'address4' } )")
    conn.execute("CREATE (y: address {id: 5, name: 'address5' } )")
    conn.execute("CREATE (z: address {id: 6, name: 'address6' } )")

    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:structure {weight: 1.0}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:structure {weight: 2.2}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 1 AND b.id = 6 CREATE (a)-[:structure {weight: 2.3}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 2 AND b.id = 4 CREATE (a)-[:structure {weight: 1.3}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 2 AND b.id = 5 CREATE (a)-[:structure {weight: 1.4}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 3 AND b.id = 6 CREATE (a)-[:structure {weight: 1.5}]->(b)"
    )

    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 3 AND b.id = 6 CREATE (a)-[:belong {weight: 2.0}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 4 AND b.id = 5 CREATE (a)-[:belong {weight: 2.1}]->(b)"
    )

    res = conn.execute(
        """
        MATCH(n1: address)-[e1: structure]->(m1: address),
              (n1: address)-[e2: structure]->(m2: address),
              (m1)-[e3: belong]->(m2)
        WHERE n1.id = 1 AND e1.weight > 2.0 AND e2.weight > 2.0 AND e3.weight > 1.9
        RETURN e1.weight, e2.weight, e3.weight
    """
    )
    assert res.__next__() == [2.2, 2.3, 2.0]


def test_intersect_predicate_ml():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=db_dir)
    conn = db.connect()
    res = conn.execute(
        """
            MATCH(p1)<-[e1:studyAt]-(t2), (p1)<-[e2:studyAt]-(t1),  (t1)-[e3]-(t2)
            WHERE e1.year > 2020
            RETURN e1.year,e2.year
                       """
    )
    assert list(res) == [[2021, 2020], [2021, 2020], [2021, 2020], [2021, 2020]]


def test_where_not_subquery():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir)
    conn = db.connect()
    res = conn.execute(
        """
        Match (a:person)-[:created]->(b)<-[:created]-(c:person)
        Where NOT (a)-[:knows]->(c) AND a <> c
        Return count(a);
    """
    )
    records = list(res)
    assert records == [[5]]


def test_where_subquery():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir)
    conn = db.connect()
    res = conn.execute(
        """
        Match (a:person)-[:created]->(b)<-[:created]-(c:person)
        Where (a)-[:knows]->(c) AND a <> c
        Return count(a);
    """
    )
    records = list(res)
    assert records == [[1]]


def aggregate_dependent_key_1():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    result = conn.execute(
        """
        MATCH (a:person)-[:knows]->(b:person)
        RETURN a.ID, a.gender, b.gender, sum(b.age)
        ORDER BY a.ID, a.gender, b.gender
    """
    )

    records = list(result)
    assert records == [
        [0, 1, 1, 45],
        [0, 1, 2, 50],
        [2, 2, 1, 80],
        [2, 2, 2, 20],
        [3, 1, 1, 35],
        [3, 1, 2, 50],
        [5, 2, 1, 80],
        [5, 2, 2, 30],
        [7, 1, 2, 65],
    ]


def aggregate_dependent_key_2():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    result = conn.execute(
        """
        MATCH (a:person)
        WHERE a.ID > 4 WITH a, a.age AS foo
        MATCH (a)-[:knows]->(b:person)
        RETURN a.ID, foo, COUNT(*)
    """
    )

    records = list(result)
    assert records == [[5, 20, 3], [7, 20, 2]]


def test_checkpoint():
    db_dir = "/tmp/test_checkpoint"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT32, name STRING, PRIMARY KEY(id))")
    conn.execute("CREATE (p:Person {id: 1, name: 'Alice'});")
    conn.execute("CREATE (p:Person {id: 2, name: 'Bob'});")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)")
    conn.execute("CREATE REL TABLE Likes(FROM Person TO Person, weight DOUBLE)")
    conn.execute(
        "CREATE REL TABLE Visits(FROM Person TO Person, time STRING, location STRING)"
    )
    conn.execute(
        "MATCH (p1:Person), (p2:Person)  WHERE p1.id = 1 AND p2.id = 2 CREATE (p1)-[:Knows]->(p2);"
    )
    conn.execute(
        "MATCH (p1:Person), (p2:Person)  WHERE p1.id = 1 AND p2.id = 2 CREATE (p1)-[:Likes {weight: 0.5}]->(p2);"
    )
    conn.execute(
        "MATCH (p1:Person), (p2:Person)  WHERE p1.id = 2 AND p2.id = 1 "
        " CREATE (p1)-[:Visits {time: '2024-01-01', location: 'NYC'}]->(p2);"
    )
    res = conn.execute("MATCH (p1:Person)-[k:Knows]->(p2:Person) RETURN p1.id, p2.id;")
    records = list(res)
    assert records == [[1, 2]]
    res = conn.execute(
        "MATCH (p1:Person)-[k:Likes]->(p2:Person) RETURN p1.id, p2.id, k.weight;"
    )
    records = list(res)
    assert records == [[1, 2, 0.5]]
    res = conn.execute(
        "MATCH (p1:Person)-[k:Visits]->(p2:Person) RETURN p1.id, p2.id, k.time, k.location;"
    )
    records = list(res)
    assert records == [[2, 1, "2024-01-01", "NYC"]]
    conn.execute("CHECKPOINT;")
    res = conn.execute("MATCH (p:Person) RETURN p.id, p.name;")
    records = list(res)
    assert records == [[1, "Alice"], [2, "Bob"]]
    res = conn.execute("MATCH (p1:Person)-[k:Knows]->(p2:Person) RETURN p1.id, p2.id;")
    records = list(res)
    assert records == [[1, 2]]
    conn.close()
    db.close()


def test_return_date():
    db_dir = "/tmp/tinysnb"
    query = "MATCH (n) return n.birthdate limit 1"
    import datetime

    expected = [[datetime.date(1900, 1, 1)]]
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(query)
    records = list(result)
    assert records == expected, f"Expected {expected}, got {records}"


# test START_NODE and END_NODE
def test_start_end_node():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    submit_cypher_query(
        conn=conn,
        query="Match (n:PERSON {id: 933})-[k:KNOWS]->(m:PERSON {id: 2199023256077})"
        " Return START_NODE(k) as n1, END_NODE(k) as n2;",
        lambda_func=ensure_result_cnt_gt_zero,
    )
    conn.close()
    db.close()


# test undirected and unweighted shortest path
def test_shortest_path():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "Match (n:PERSON {id: 933})-[k:KNOWS* SHORTEST  1..3]-(m:PERSON {id: 2199023256668}) Return LENGTH(k) Limit 1;"
    )
    for record in result:
        assert record[0] == 3, f"Expected value 3, got {record[0]}"
    conn.close()
    db.close()


def test_properties():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    submit_cypher_query(
        conn=conn,
        query="Match (n:PERSON {id: 933})-[k:KNOWS*1..3]-(m:PERSON {id: 2199023256668})"
        " Return properties(nodes(k), 'firstName') as n1, properties(rels(k),'creationDate') as n2 LIMIT 1;",
        lambda_func=ensure_result_cnt_gt_zero,
    )
    conn.close()
    db.close()


def test_delete_edges():
    db_dir = "/tmp/test_delete_edges"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
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


def test_internal_id_filter():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "Match (n:PERSON {id: 933}) Where id(n) = 72057594037927936 Return n.id;"
    )
    for record in result:
        assert record[0] == 933, f"Expected value 933, got {record[0]}"
    conn.close()
    db.close()


def test_drop_person_if_exists():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    result = conn.execute("drop table if exists person2;")
    assert len(result) == 0
    db.close()


def test_drop_knows_if_exists():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    result = conn.execute("drop table if exists knows2;")
    assert len(result) == 0
    db.close()


def test_create_person_if_not_exists():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute(
        """
        create node table if not exists
        person(name STRING, PRIMARY KEY(name));
    """
    )
    res = conn.execute("match (p:person) return count(p.age);")
    records = list(res)
    assert records == [[4]]
    db.close()


def test_create_knows_if_not_exists():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute(
        """
        create rel table if not exists
        knows(FROM person TO person, name STRING);
    """
    )
    res = conn.execute(
        """
        match (p:person)-[r:knows]->(q:person)
        return count(r.weight);
    """
    )
    records = list(res)
    assert records == [[2]]
    db.close()


def test_undir_multi_label():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "MATCH (a:person:organisation)-[:meets|:marries|:workAt]-(b:person:organisation) RETURN COUNT(*);"
    )
    records = list(result)
    assert records == [[26]]


def test_mixed_match():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) MATCH (b)-[:knows]->(c:person) RETURN a.id,b.id,c.id;"
    )
    records = list(result)
    assert len(records) == 36


def test_mullti_label2():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "MATCH (a:person:organisation) OPTIONAL MATCH (a)-[:studyAt|:workAt]->(b:person:organisation) RETURN a.id,b.id;"
    )
    records = list(result)
    logger.info(f"records: {records}, len: {len(records)}")
    assert len(records) == 11


def test_recreate_vertex():
    db_dir = "/tmp/test_recreate_vertex"
    logger.info("Starting test_recreate_vertex")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE (p: Person {id: 111});")
    conn.execute("CREATE (p: Person {id: 222});")
    conn.execute("DROP TABLE IF EXISTS Person;")

    conn.execute("CREATE NODE TABLE Person(id INT64, age INT32, PRIMARY KEY(id));")
    val = conn.execute("MATCH (n) return count(n);")
    records = list(val)
    assert records == [[0]], f"Expected value [[0]], got {records}"
    conn.execute("CREATE (p: Person {id: 111, age: 20});")
    res = conn.execute("MATCH (p: Person) RETURN p.id, p.age;")
    records = list(res)
    assert records == [[111, 20]]
    logger.info("test_recreate_vertex passed first part")

    conn.execute("DROP TABLE IF EXISTS Person;")
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute("CREATE (p: Person {id: 111, name: 'Alice'});")
    res = conn.execute("MATCH (p: Person) RETURN p.id, p.name;")
    records = list(res)
    assert records == [[111, "Alice"]]
    logger.info("test_recreate_vertex passed second part")
    conn.close()
    db.close()


def test_recreate_edge():
    db_dir = "/tmp/test_recreate_edge"
    logger.info("Starting test_recreate_edge")
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
    conn.execute("DROP TABLE IF EXISTS Knows;")

    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weight DOUBLE);")
    val = conn.execute("MATCH ()-[e:Knows]->() return count(e);")
    records = list(val)
    assert records == [[0]], f"Expected value [[0]], got {records}"
    conn.execute(
        "MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {weight: 1.5}]->(p2);"
    )
    res = conn.execute("MATCH (p1: Person)-[k: Knows]->(p2:Person) RETURN k.weight;")
    records = list(res)
    assert records == [[1.5]]
    logger.info("test_recreate_edge passed first part")

    conn.execute("DROP TABLE IF EXISTS Knows;")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, id INT64);")
    conn.execute(
        "MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 444}]->(p2);"
    )
    res = conn.execute("MATCH (p1: Person)-[k: Knows]->(p2:Person) RETURN k.id;")
    records = list(res)
    assert records == [[444]]
    logger.info("test_recreate_edge passed second part")
    conn.close()
    db.close()


def test_delete_vertex_detach_edge():
    db_dir = "/tmp/test_delete_vertex_detach_edge"
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


def test_default_value():
    db_dir = "/tmp/test_default_value"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
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
    conn.close()


def test_delete_vertex_detach_edge2():
    db_dir = "/tmp/test_delete_vertex_detach_edge2"
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


def test_list_extract_function():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    res = conn.execute(
        """
        Match (a)
        WITH a ORDER BY a.name
        RETURN labels(a) as label, collect(a.name)[0];
    """
    )
    records = list(res)
    assert records == [["person", "josh"], ["software", "lop"]]


def test_weight_shortest_path():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    res = conn.execute(
        """
        Match (a:person {name : 'marko'})-[k * WSHORTEST(weight)]-(b:person {name: 'josh'})
        Return a.name, b.name, cost(k);
        """
    )
    records = list(res)
    assert records == [["marko", "josh", 0.8]]
    db.close()


def test_optional_match_person_software():
    """Test OPTIONAL MATCH with multi-label pattern on modern graph.

    Query: MATCH (p: PERSON) WHERE p.id=1
           OPTIONAL MATCH (p)-[]-(other:PERSON:SOFTWARE)
           WHERE other.id>0
           RETURN other;
    """
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    res = conn.execute(
        """
        MATCH (p: PERSON) WHERE p.id=1
        OPTIONAL MATCH (p)-[]-(other:PERSON:SOFTWARE)
        WHERE other.id>0
        RETURN other;
        """
    )
    records = list(res)
    print(records)
    # TODO(zhanglei): fix the output format
    assert records == [
        [{"_ID": 1, "id": 2, "name": "vadas", "age": 27, "_LABEL": "person"}],
        [{"_ID": 2, "id": 4, "name": "josh", "age": 32, "_LABEL": "person"}],
        [
            {
                "_ID": 72057594037927936,
                "id": 3,
                "name": "lop",
                "lang": "java",
                "_LABEL": "software",
            }
        ],
    ]
    conn.close()
    db.close()


def test_optional_match_person_software_with_edge_weight():
    """Test OPTIONAL MATCH with multi-label pattern and edge weight condition on modern graph.

    Query: MATCH (p: PERSON) WHERE p.id=1
           OPTIONAL MATCH (p)-[e]->(other:PERSON:Software)
           WHERE e.weight>10 and other.id>10
           RETURN other;
    """
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    res = conn.execute(
        """
        MATCH (p: PERSON) WHERE p.id=1
        OPTIONAL MATCH (p)-[e]->(other:PERSON:Software)
        WHERE e.weight>10 and other.id>10
        RETURN other;
        """
    )
    records = list(res)
    assert records == [[None]]
    conn.close()
    db.close()


def test_multi_ddl_queries():
    db_dir = "/tmp/multi_ddl_queries"
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            """
       CREATE NODE TABLE N (id SERIAL, PRIMARY KEY(id));
        """
        )
    assert str("Unsupported basic type for conversion: SERIAL") in str(excinfo.value)
    conn.close()
    db.close()


def test_parameterized_query():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    params = {"person_id": 1}
    res = conn.execute(
        """
        MATCH (n:PERSON {id: $person_id})-[:KNOWS]->(m:PERSON)
        RETURN m.name;
        """,
        parameters=params,
    )
    records = list(res)
    assert records == [
        ["vadas"],
        ["josh"],
    ], f"Expected value [['vadas'], ['josh']], got {records}"
    conn.close()
    db.close()


def test_alter_table_add_property_with_default_tinysnb():
    """Test ALTER TABLE to add property with default value on tinysnb person table."""
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    # Alter table person to add property propy with default value 10
    conn.execute("ALTER TABLE person ADD propy INT64 DEFAULT 10;")

    # Query to verify the new property exists and has default value
    result = conn.execute("MATCH (c:person) RETURN c.propy LIMIT 1;")
    records = list(result)
    assert records == [[10]]

    conn.close()
    db.close()


def test_filtering():
    db_dir = "/tmp/tinysnb"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute(
        "MATCH (a:person)-[e1:knows]->(b:person) WHERE a.age > 35 RETURN b.fName"
    )
    records = list(result)
    assert records == [["Alice"], ["Bob"], ["Dan"]]
    conn.close()
    db.close()


def test_result():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=db_dir, mode="r")
    conn = db.connect()
    result = conn.execute("Match (n: person) return n")
    logger.info(list(result))
    logger.info(result.column_names())


def test_parameterized_where_on_edge_string_property():
    """Test that parameterized WHERE on edge STRING property works (not just literals)."""
    db = Database(db_path=":memory", mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE IF NOT EXISTS A(id STRING PRIMARY KEY)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS R(FROM A TO A, tag STRING)")

    conn.execute("CREATE (a:A {id: 'n1'})")
    conn.execute("CREATE (a:A {id: 'n2'})")
    conn.execute(
        "MATCH (a:A), (b:A) WHERE a.id = 'n1' AND b.id = 'n2' CREATE (a)-[:R {tag: 'hello'}]->(b)"
    )

    # Literal string should work
    res_literal = conn.execute(
        "MATCH (a:A)-[e:R]->(b:A) WHERE e.tag = 'hello' RETURN e.tag"
    )
    assert list(res_literal) == [["hello"]]

    # Parameterized string should also work
    res_param = conn.execute(
        "MATCH (a:A)-[e:R]->(b:A) WHERE e.tag = $t RETURN e.tag",
        parameters={"t": "hello"},
    )
    assert list(res_param) == [["hello"]]

    conn.close()
    db.close()


def test_insert_many_vertices():
    db_dir = "/tmp/test_insert_many_vertices"
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


def test_insert_many_edges():
    db_dir = "/tmp/test_insert_many_edges"
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


def test_insert_string_column_exhaustion():
    logging.disable(logging.CRITICAL)
    try:
        db_dir = "/tmp/test_insert_string_column_exhaustion"
        shutil.rmtree(db_dir, ignore_errors=True)
        db = Database(db_path=db_dir, mode="w")
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));"
        )
        # by default the string column has maximum length 256
        conn.execute("CREATE (p: Person {id: 1, name: 'a'});")
        conn.execute("CREATE (p: Person {id: 2, name: 'b'});")
        conn.execute("CHECKPOINT;")
        conn.close()
        db.close()

        db2 = Database(db_path=db_dir, mode="w")
        conn2 = db2.connect()
        str_prop = "a" * 255
        for i in range(10000):
            conn2.execute(f"CREATE (p: Person {{id: {i+3}, name: '{str_prop}'}});")
        res = conn2.execute("MATCH (p: Person) RETURN count(p);")
        records = list(res)
        assert records == [[10002]], f"Expected value [[10002]], got {records}"
        conn2.close()
        db2.close()

        db3 = Database(db_path=db_dir, mode="w")
        conn3 = db3.connect()
        conn3.execute("CREATE REL TABLE Knows(FROM Person TO Person, note STRING);")
        conn3.execute(
            "MATCH (a: Person), (b: Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:Knows {note: '12'}]->(b);"
        )
        conn3.execute(
            "MATCH (a: Person), (b: Person) WHERE a.id = 3 AND b.id = 4 CREATE (a)-[:Knows {note: '34'}]->(b);"
        )
        conn3.execute("CHECKPOINT;")
        db3.close()

        db4 = Database(db_path=db_dir, mode="w")
        conn4 = db4.connect()
        res4 = conn4.execute(
            "MATCH (a: Person)-[k: Knows]->(b: Person) RETURN k.note ORDER BY k.note;"
        )
        records = list(res4)
        assert records == [
            ["12"],
            ["34"],
        ], f"Expected value [['12'], ['34']], got {records}"
        str_prop = "a" * 255
        for i in range(100):
            conn4.execute(
                f"MATCH (a: Person {{id: 1}}), (b: Person {{id: 2}}) CREATE (a)-[:Knows {{note: '{str_prop}'}}]->(b);"
            )
        conn4.close()
        db4.close()
    except Exception as e:
        raise AssertionError(f"Test failed with exception: {e}")
    finally:
        logging.disable(logging.NOTSET)
