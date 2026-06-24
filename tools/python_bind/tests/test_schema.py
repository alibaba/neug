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

"""DDL / schema-management tests extracted from test_db_query.py.

Tests that previously opened ``/tmp/modern_graph`` or ``/tmp/tinysnb``
directly now use the ``modern_graph`` / ``tinysnb`` pytest fixtures
defined in ``conftest.py``.  Self-contained tests that create their own
database under ``tmp_path`` are kept verbatim.
"""

import shutil

import pytest

from neug.database import Database
from neug.proto.error_pb2 import ERR_QUERY_SYNTAX
from neug.proto.error_pb2 import ERR_SCHEMA_MISMATCH
from neug.proto.error_pb2 import ERR_TYPE_CONVERSION


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
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
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
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);"
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
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);"
    )
    # 1. create duplicate edge table
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);"
        )
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    # 2. create edge table without FROM/TO vertex tables
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE REL TABLE NewFollows(FROM person TO user, MANY_TO_MANY);")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
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
        "CREATE REL TABLE follows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, MANY_TO_MANY);")
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
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    conn.execute("ALTER TABLE person RENAME age TO newAge;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person RENAME age1 TO newAge1;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    # 3. drop property
    # correctly drop a property
    conn.execute("ALTER TABLE person DROP newAge;")
    # incorrectly drop a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE person DROP age1;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
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
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);"
    )
    # 1. add property
    # correctly add a new property
    conn.execute("ALTER TABLE knows ADD since INT64;")
    # incorrectly add a property that already exists
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows ADD weight DOUBLE;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    conn.execute("ALTER TABLE knows RENAME weight TO newWeight;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows RENAME weight1 TO newWeight1;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
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
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);"
    )
    # correctly drop a property
    conn.execute("ALTER TABLE knows DROP weight;")
    # incorrectly drop a property that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE knows DROP weight1;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
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
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);"
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
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE, MANY_TO_MANY);"
    )
    # 1. DROP vertex table will also drop all edges connected to it by default
    conn.execute("DROP TABLE person;")
    # the edge table has already been dropped, so this will fail
    with pytest.raises(Exception) as excinfo:
        conn.execute("DROP TABLE knows;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    # 2. DROP table that does not exist
    with pytest.raises(Exception) as excinfo:
        conn.execute("DROP TABLE person;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    conn.close()
    db.close()


def test_multi_ddl_queries(tmp_path):
    db_dir = str(tmp_path / "multi_ddl_queries")
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            """
       CREATE NODE TABLE N (id SERIAL, PRIMARY KEY(id));
        """
        )
    assert "SERIAL" in str(excinfo.value)
    conn.close()
    db.close()


def test_alter_table_add_property_with_default(tinysnb):
    """Test ALTER TABLE to add property with default value on tinysnb person table."""
    # Alter table person to add property propy with default value 10
    tinysnb.execute("ALTER TABLE person ADD propy INT64 DEFAULT 10;")

    # Query to verify the new property exists and has default value
    result = tinysnb.execute("MATCH (c:person) RETURN c.propy LIMIT 1;")
    records = list(result)
    assert records == [[10]]


def test_drop_and_recreate_table_same_name(tmp_path):
    """Test that dropping node tables with relationships and recreating
    with the same name but different schema does not crash (SIGSEGV)."""
    db_dir = tmp_path / "drop_recreate"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    try:
        queries = [
            "CREATE NODE TABLE Y0(id STRING, p0 INT32, PRIMARY KEY(id));",
            "CREATE NODE TABLE Y1(id STRING, p1 STRING, PRIMARY KEY(id));",
            "CREATE REL TABLE YR0(FROM Y0 TO Y1, rp0 DOUBLE);",
            'CREATE (a:Y0 {id: "a", p0: 1});',
            'CREATE (b:Y1 {id: "b", p1: "x"});',
            'MATCH (a:Y0 {id: "a"}), (b:Y1 {id: "b"}) CREATE (a)-[:YR0 {rp0: 1.5}]->(b);',
            "DROP TABLE IF EXISTS Y1;",
            "DROP TABLE IF EXISTS Y0;",
            "CREATE NODE TABLE Y0(id STRING, q DOUBLE, PRIMARY KEY(id));",
        ]

        for query in queries:
            conn.execute(query)

        # Verify the recreated table works correctly
        conn.execute('CREATE (c:Y0 {id: "c", q: 3.14});')
        result = conn.execute("MATCH (n:Y0) RETURN n.id, n.q;")
        rows = list(result)
        assert len(rows) == 1
        assert rows[0][0] == "c"
        assert rows[0][1] == pytest.approx(3.14, abs=1e-6)
    finally:
        conn.close()
        db.close()


def test_drop_and_recreate_node_table_no_stale_data(tmp_path):
    """After DROP + re-CREATE of a node table, old rows must not reappear."""
    db_dir = tmp_path / "drop_recreate_stale"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE IF NOT EXISTS Person(id STRING PRIMARY KEY);")
    conn.execute("CREATE (p:Person {id: 'alice'});")
    conn.execute("CHECKPOINT")
    assert list(conn.execute("MATCH (p:Person) RETURN p.id;")) == [["alice"]]

    # Drop and re-create with the same schema
    conn.execute("DROP TABLE IF EXISTS Person;")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Person(id STRING PRIMARY KEY);")

    # Old data must be gone
    assert list(conn.execute("MATCH (p:Person) RETURN p.id;")) == []

    # Only newly inserted data should be visible
    conn.execute("CREATE (p:Person {id: 'bob'});")
    assert list(conn.execute("MATCH (p:Person) RETURN p.id;")) == [["bob"]]

    conn.close()
    db.close()


def test_drop_person_if_exists(modern_graph):
    result = modern_graph.execute("drop table if exists person2;")
    assert len(result) == 0


def test_drop_knows_if_exists(modern_graph):
    result = modern_graph.execute("drop table if exists knows2;")
    assert len(result) == 0


def test_create_person_if_not_exists(modern_graph):
    modern_graph.execute(
        """
        create node table if not exists
        person(name STRING, PRIMARY KEY(name));
    """
    )
    res = modern_graph.execute("match (p:person) return count(p.age);")
    records = list(res)
    assert records == [[4]]


def test_create_knows_if_not_exists(modern_graph):
    modern_graph.execute(
        """
        create rel table if not exists
        knows(FROM person TO person, name STRING);
    """
    )
    res = modern_graph.execute(
        """
        match (p:person)-[r:knows]->(q:person)
        return count(r.weight);
    """
    )
    records = list(res)
    assert records == [[2]]
