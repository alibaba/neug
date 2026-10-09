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


def test_struct_field_names_must_be_unique(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    with pytest.raises(RuntimeError, match="Duplicate struct field name: x"):
        conn.execute(
            "CREATE NODE TABLE T(id INT64, value STRUCT(x INT64, x STRING), "
            "PRIMARY KEY(id));"
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


def test_string_type_length_boundaries(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE StringLengths("
        "id INT64 PRIMARY KEY,"
        "string_value STRING,"
        "min_value VARCHAR(1),"
        "max_value VARCHAR(65535));"
    )
    conn.execute("ALTER TABLE StringLengths ADD altered_string STRING;")
    conn.execute("ALTER TABLE StringLengths ADD altered_min VARCHAR(1);")
    conn.execute("ALTER TABLE StringLengths ADD altered_max VARCHAR(65535);")
    conn.execute(
        "CREATE (:StringLengths {"
        "id: 1, string_value: 'string', min_value: 'm', "
        "max_value: 'sentinel', altered_string: 'altered', "
        "altered_min: 'a', altered_max: 'persisted'});"
    )
    query = (
        "MATCH (n:StringLengths {id: 1}) "
        "RETURN n.string_value, n.min_value, n.max_value, "
        "n.altered_string, n.altered_min, n.altered_max;"
    )
    expected = [["string", "m", "sentinel", "altered", "a", "persisted"]]
    assert list(conn.execute(query)) == expected

    conn.execute("CHECKPOINT;")
    conn.close()
    db.close()

    db = Database(db_path=str(tmp_path), mode="r", checkpoint_on_close=False)
    conn = db.connect()
    assert list(conn.execute(query)) == expected

    conn.close()
    db.close()


@pytest.mark.parametrize("ddl_path", ["create", "alter"])
@pytest.mark.parametrize(
    "data_type",
    [
        "VARCHAR(0)",
        "VARCHAR(65536)",
        "VARCHAR(65536)[]",
        "VARCHAR(65536)[2]",
    ],
)
def test_invalid_string_type_lengths(tmp_path, ddl_path, data_type):
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    if ddl_path == "create":
        ddl = (
            "CREATE NODE TABLE InvalidStringLength("
            f"id INT64 PRIMARY KEY, value {data_type});"
        )
    else:
        conn.execute("CREATE NODE TABLE InvalidStringLength(id INT64 PRIMARY KEY);")
        ddl = f"ALTER TABLE InvalidStringLength ADD value {data_type};"

    with pytest.raises(
        RuntimeError,
        match="length of VARCHAR/STRING must be between 1 and 65535",
    ):
        conn.execute(ddl)

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


def test_alter_edge_struct_property_storage_transition(tmp_path):
    path = str(tmp_path / "alter_edge_struct")
    db = Database(db_path=path, mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE REL TABLE R(FROM T TO T)")
        conn.execute("CREATE (:T {id: 1})")
        conn.execute("CREATE (:T {id: 2})")
        conn.execute("MATCH (a:T {id: 1}), (b:T {id: 2}) CREATE (a)-[:R]->(b)")
        conn.execute("ALTER TABLE R ADD s STRUCT(x INT64)")
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.s")) == [[{"x": 0}]]
        conn.execute("MATCH ()-[e:R]->() SET e.s = {x: 7}")
        conn.execute("ALTER TABLE R ADD weight INT64")
        conn.execute("ALTER TABLE R DROP weight")
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.s.x")) == [[7]]
        conn.execute("CHECKPOINT")
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.s.x")) == [[7]]
    finally:
        conn.close()
        db.close()

    db = Database(db_path=path, mode="w")
    conn = db.connect()
    try:
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.s.x")) == [[7]]
    finally:
        conn.close()
        db.close()


def test_alter_edge_struct_preserves_bundled_property(tmp_path):
    path = str(tmp_path / "alter_edge_struct_from_bundled")
    db = Database(db_path=path, mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE REL TABLE R(FROM T TO T, weight INT64)")
        conn.execute("CREATE (:T {id: 1})")
        conn.execute("CREATE (:T {id: 2})")
        conn.execute(
            "MATCH (a:T {id: 1}), (b:T {id: 2}) " "CREATE (a)-[:R {weight: 9}]->(b)"
        )
        conn.execute("ALTER TABLE R ADD s STRUCT(x INT64)")
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.weight, e.s.x")) == [
            [9, 0]
        ]
        conn.execute("MATCH ()-[e:R]->() SET e.s = {x: 7}")
        conn.execute("ALTER TABLE R DROP weight")
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.s.x")) == [[7]]
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
        db.close()

    db = Database(db_path=path, mode="w")
    conn = db.connect()
    try:
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.s.x")) == [[7]]
    finally:
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


@pytest.mark.parametrize(
    "other_type",
    [
        "STRUCT(x INT64)",
        "STRUCT(x INT64, y STRING)",
        "STRUCT(x INT64, y STRUCT(z INT64))",
        "STRUCT(x INT64, y STRUCT(z INT64, value INT64))",
    ],
)
def test_struct_field_access_with_unrelated_layout(tmp_path, other_type):
    db = Database(db_path=str(tmp_path / "struct_layout"), mode="w")
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE T(id INT64, "
            "s STRUCT(x INT64, y STRUCT(z INT64, value INT64)), PRIMARY KEY(id))"
        )
        conn.execute(f"CREATE NODE TABLE U(id INT64, s {other_type}, PRIMARY KEY(id))")
        conn.execute("CREATE (:T {id:1, s:{x:10, y:{z:20, value:30}}})")
        assert list(conn.execute("MATCH (n:T) RETURN n.s.y.value")) == [[30]]
        assert list(conn.execute("MATCH (n:T) WHERE n.s.y.value = 30 RETURN n.id")) == [
            [1]
        ]
        assert list(conn.execute("MATCH (n:T) WITH n.s AS s RETURN s.y.value")) == [
            [30]
        ]
        # An unlabelled scan requires one consistent type for property s.
        if other_type == "STRUCT(x INT64, y STRUCT(z INT64, value INT64))":
            assert list(conn.execute("MATCH (n) RETURN n.s.x")) == [[10]]
        else:
            with pytest.raises(
                RuntimeError, match="Expected the same data type for property s"
            ):
                conn.execute("MATCH (n) RETURN n.s.x")
    finally:
        conn.close()
        db.close()


def test_struct_row_result_is_named_dict(tmp_path):
    """Row-based results return structs as dicts keyed by field name,
    consistent with the Arrow path (to_arrow().to_pylist())."""
    db = Database(db_path=str(tmp_path / "struct_row_dict"), mode="w")
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE T(id INT64, "
            "s STRUCT(x INT64, addr STRUCT(city STRING, zip INT64), "
            "tags STRUCT(v INT64)[]), PRIMARY KEY(id))"
        )
        conn.execute(
            "CREATE (:T {id: 1, s: {x: 10, addr: {city: 'hz', zip: 310000}, "
            "tags: CAST([{v: 1}, {v: 2}], 'STRUCT(v INT64)[]')}})"
        )
        records = list(conn.execute("MATCH (n:T) RETURN n.s"))
        assert records == [
            [
                {
                    "x": 10,
                    "addr": {"city": "hz", "zip": 310000},
                    "tags": [{"v": 1}, {"v": 2}],
                }
            ]
        ]
    finally:
        conn.close()
        db.close()


def test_full_node_edge_and_path_include_composite_properties(tmp_path):
    db = Database(db_path=str(tmp_path / "composite_entity_json"), mode="w")
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE T(id INT64, "
            "s STRUCT(x INT64, nested STRUCT(y STRING), tags STRUCT(v INT64)[]), "
            "nums INT64[2], PRIMARY KEY(id))"
        )
        conn.execute(
            "CREATE REL TABLE R(FROM T TO T, "
            "s STRUCT(ok BOOL, nested STRUCT(z INT64)), nums INT64[2])"
        )
        conn.execute(
            "CREATE (:T {id: 1, s: {x: 7, nested: {y: 'a'}, "
            "tags: CAST([{v: 1}, {v: 2}], 'STRUCT(v INT64)[]')}, "
            "nums: CAST([3, 4], 'INT64[2]')})"
        )
        conn.execute("CREATE (:T {id: 2})")
        conn.execute(
            "MATCH (a:T {id: 1}), (b:T {id: 2}) "
            "CREATE (a)-[:R {s: {ok: true, nested: {z: 9}}, "
            "nums: CAST([5, 6], 'INT64[2]')}]->(b)"
        )

        row = next(
            iter(
                conn.execute(
                    "MATCH p = (a:T {id: 1})-[e:R]->(b:T {id: 2}) RETURN a, e, p"
                )
            )
        )
        node, edge, path = row
        assert node["s"] == {"x": 7, "nested": {"y": "a"}, "tags": [{"v": 1}, {"v": 2}]}
        assert node["nums"] == [3, 4]
        assert edge["s"] == {"ok": True, "nested": {"z": 9}}
        assert edge["nums"] == [5, 6]
        assert any(item["s"] == node["s"] for item in path["nodes"] if "s" in item)
        assert any(item["s"] == edge["s"] for item in path["rels"] if "s" in item)
        assert path["nodes"][0]["nums"] == node["nums"]
        assert path["rels"][0]["nums"] == edge["nums"]
    finally:
        conn.close()
        db.close()


def test_copy_from_struct_column_rejected(tmp_path):
    """Bulk loading rejects STRUCT columns with a targeted error message."""
    db = Database(db_path=str(tmp_path / "struct_copy_reject"), mode="w")
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE T(id INT64, s STRUCT(x INT64), PRIMARY KEY(id))"
        )
        csv_path = tmp_path / "t.csv"
        csv_path.write_text("id,s\n1,2\n")
        with pytest.raises(
            RuntimeError,
            match="COPY/LOAD FROM does not support STRUCT columns yet",
        ):
            conn.execute(
                f'COPY T FROM "{csv_path.as_posix()}" (HEADER TRUE, ' f'DELIMITER=",");'
            )
    finally:
        conn.close()
        db.close()


@pytest.mark.parametrize(
    "other_type", [None, "STRUCT(x INT64)", "STRUCT(inner STRING)", "INT64"]
)
def test_struct_edge_property_field_access(tmp_path, other_type):
    """Struct edge fields work in direct, nested, and record contexts."""
    db = Database(db_path=str(tmp_path / "struct_edge_field"), mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))")
        conn.execute(
            "CREATE REL TABLE R(FROM T TO T, "
            "s STRUCT(x INT64, inner STRUCT(y INT64)))"
        )
        if other_type is not None:
            conn.execute(f"CREATE REL TABLE Other(FROM T TO T, s {other_type})")
        conn.execute("CREATE (:T {id: 1})")
        conn.execute("CREATE (:T {id: 2})")
        conn.execute(
            "MATCH (a:T {id: 1}), (b:T {id: 2}) "
            "CREATE (a)-[:R {s: {x: 10, inner: {y: 20}}}]->(b)"
        )
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.s.x")) == [[10]]
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.s.inner.y")) == [[20]]
        assert list(conn.execute("MATCH ()<-[e:R]-() RETURN e.s.inner.y")) == [[20]]
        assert list(
            conn.execute("MATCH ()-[e:R]->() WHERE e.s.inner.y = 20 RETURN e.s.x")
        ) == [[10]]
        assert list(
            conn.execute(
                "MATCH (n:T {id: 2}) OPTIONAL MATCH (n)-[e:R]->() " "RETURN e.s.inner.y"
            )
        ) == [[None]]
        assert list(conn.execute("MATCH ()-[e:R]->() WITH e.s AS s RETURN s.x")) == [
            [10]
        ]
        assert list(conn.execute("MATCH ()-[e:R]->() RETURN e.s")) == [
            [{"x": 10, "inner": {"y": 20}}]
        ]
    finally:
        conn.close()
        db.close()


@pytest.mark.parametrize("value", ["2", "NULL"])
def test_struct_implicit_cast_requires_matching_field_names(tmp_path, value):
    """A struct value cannot be implicitly retyped to a struct with different
    field names; the compatibility check must compare names, not just field
    counts and child types."""
    db = Database(db_path=str(tmp_path / "struct_cast_names"), mode="w")
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE T(id INT64, s STRUCT(x INT64), PRIMARY KEY(id))"
        )
        conn.execute("CREATE (:T {id: 1, s: {x: 1}})")
        with pytest.raises(RuntimeError, match="(?i)cast"):
            conn.execute(f"MATCH (n:T) SET n.s = {{y: {value}}}")
        assert list(conn.execute("MATCH (n:T) RETURN n.s.x")) == [[1]]
    finally:
        conn.close()
        db.close()


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("operation", ["update", "delete"])
@pytest.mark.parametrize("with_other_prop", [False, True])
def test_struct_parallel_edge_mutation(tmp_path, reverse, operation, with_other_prop):
    path = str(tmp_path / "struct_parallel_edges")
    outgoing = "MATCH (:T {id:1})-[e:R]->(:T {id:2})"
    incoming = "MATCH (:T {id:2})<-[e:R]-(:T {id:1})"
    initial = [(10, 110), (20, 120), (40, 140)]
    expected = (
        [(10, 110), (30, 120), (40, 140)]
        if operation == "update"
        else [(10, 110), (40, 140)]
    )

    def check_edges(conn, values):
        for match in (outgoing, incoming):
            projection = "e.s.inner.y, e.weight" if with_other_prop else "e.s.inner.y"
            assert list(
                conn.execute(match + " RETURN " + projection + " ORDER BY e.s.inner.y")
            ) == (
                [list(row) for row in values]
                if with_other_prop
                else [[row[0]] for row in values]
            )

    db = Database(db_path=path, mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))")
        props = "weight INT64, " if with_other_prop else ""
        conn.execute(
            "CREATE REL TABLE R(FROM T TO T, "
            + props
            + "s STRUCT(inner STRUCT(y INT64)))"
        )
        conn.execute("CREATE (:T {id:1}), (:T {id:2})")
        for value in (10, 20, 40):
            weight = f"weight:{value + 100}, " if with_other_prop else ""
            conn.execute(
                "MATCH (a:T {id:1}), (b:T {id:2}) "
                f"CREATE (a)-[:R {{{weight}s:{{inner:{{y:{value}}}}}}}]->(b)"
            )
        check_edges(conn, initial)
        if with_other_prop:
            assert list(
                conn.execute(
                    outgoing + " WITH e RETURN e.s.inner.y ORDER BY e.s.inner.y"
                )
            ) == [[10], [20], [40]]
        match = incoming if reverse else outgoing
        action = "SET e.s={inner:{y:30}}" if operation == "update" else "DELETE e"
        conn.execute(match + " WHERE e.s.inner.y = 20 " + action)
        check_edges(conn, expected)
        conn.execute("CHECKPOINT")
        check_edges(conn, expected)
    finally:
        conn.close()
        db.close()

    db = Database(db_path=path, mode="w")
    conn = db.connect()
    try:
        check_edges(conn, expected)
    finally:
        conn.close()
        db.close()


def test_struct_edge_field_refs_after_checkpoint_and_reopen(tmp_path):
    path = str(tmp_path / "edge_field_refs")
    query = "MATCH (a:T)-[e:R]->() RETURN a.id, e.s.inner.y ORDER BY a.id"
    db = Database(db_path=path, mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE REL TABLE R(FROM T TO T, s STRUCT(inner STRUCT(y INT64)))")
        for i in range(1, 4):
            conn.execute(f"CREATE (:T {{id:{i}}})")
        for i, value in [(1, 10), (2, 20)]:
            conn.execute(
                f"MATCH (a:T {{id:{i}}}), (b:T {{id:{i + 1}}}) "
                f"CREATE (a)-[:R {{s:{{inner:{{y:{value}}}}}}}]->(b)"
            )
        assert list(conn.execute(query)) == [[1, 10], [2, 20]]
        conn.execute("CHECKPOINT")
        assert list(conn.execute(query)) == [[1, 10], [2, 20]]
    finally:
        conn.close()
        db.close()

    db = Database(db_path=path, mode="w")
    conn = db.connect()
    try:
        assert list(conn.execute(query)) == [[1, 10], [2, 20]]
    finally:
        conn.close()
        db.close()


@pytest.mark.parametrize(
    "expression, expected",
    [
        ("[1, 'a']", [1, "a"]),
        ("{x: 1, text: 'a'}", {"x": 1, "text": "a"}),
        ("[{x: 1}, 'a']", [{"x": 1}, "a"]),
        ("{pair: [1, 'a'], nested: {x: 2}}", {"pair": [1, "a"], "nested": {"x": 2}}),
    ],
)
def test_tuple_and_struct_python_result_types(tmp_path, expression, expected):
    """Conversion through a query plan preserves positional vs named results."""

    def assert_value_and_type(actual, expected):
        assert type(actual) is type(expected)
        assert actual == expected
        if isinstance(expected, dict):
            assert list(actual) == list(expected)
            for key in expected:
                assert_value_and_type(actual[key], expected[key])
        elif isinstance(expected, list):
            for actual_child, expected_child in zip(actual, expected):
                assert_value_and_type(actual_child, expected_child)

    db = Database(db_path=str(tmp_path / "tuple_struct_results"), mode="w")
    conn = db.connect()
    try:
        for query in (
            f"RETURN {expression}",
            f"WITH {expression} AS value RETURN value",
        ):
            rows = list(conn.execute(query))
            assert len(rows) == 1
            assert_value_and_type(rows[0][0], expected)
    finally:
        conn.close()
        db.close()
