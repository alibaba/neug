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

"""Integration tests for Array<T, N> property type."""

import json

import pytest

from neug.database import Database


def _nested_list(value):
    if isinstance(value, (str, bytes)):
        return value
    try:
        return [_nested_list(item) for item in value]
    except TypeError:
        return value


def _approx_eq(actual, expected, tol=1e-5):
    """Recursively compare nested lists with tolerance for floats."""
    if isinstance(expected, list):
        assert len(actual) == len(expected)
        for a, e in zip(actual, expected):
            _approx_eq(a, e, tol)
    elif isinstance(expected, float):
        assert abs(actual - expected) < tol
    else:
        assert actual == expected


# ---------------------------------------------------------------------------
# Basic Create & Query (parametrized by element type)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "array_type,values,expected",
    [
        ("INT32[3]", "[10, 20, 30]", [10, 20, 30]),
        ("DOUBLE[2]", "[1.5, 2.5]", [1.5, 2.5]),
        ("STRING[2]", "['hello', 'world']", ["hello", "world"]),
        ("FLOAT[3]", "[0.0, 3.0, 6.0]", [0.0, 3.0, 6.0]),
    ],
)
def test_array_create_and_query(tmp_path, array_type, values, expected):
    """Create a vertex type with various array property types, insert and query."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(f"CREATE NODE TABLE T(id INT64, arr {array_type}, PRIMARY KEY(id));")
    conn.execute(f"CREATE (n:T {{id: 1, arr: {values}}});")

    rows = list(conn.execute("MATCH (n:T) RETURN n.arr;"))
    assert len(rows) == 1
    _approx_eq(_nested_list(rows[0][0]), expected)

    conn.close()
    db.close()


def test_array_multiple_rows(tmp_path):
    """Insert and query multiple rows with array properties."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Item(id INT64, features FLOAT[3], PRIMARY KEY(id));"
    )
    for i in range(5):
        vals = [float(i * 3 + j) for j in range(3)]
        conn.execute(
            f"CREATE (item:Item {{id: {i}, features: [{vals[0]}, {vals[1]}, {vals[2]}]}});"
        )

    rows = list(
        conn.execute(
            "MATCH (item:Item) RETURN item.id, item.features ORDER BY item.id;"
        )
    )
    assert len(rows) == 5
    for i, row in enumerate(rows):
        expected = [float(i * 3 + j) for j in range(3)]
        _approx_eq(list(row[1]), expected)

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Update (SET) tests
# ---------------------------------------------------------------------------


def test_array_update(tmp_path):
    """Update array property: single vertex and batch."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Point(id INT64, coords INT64[2], PRIMARY KEY(id));")
    for i in range(3):
        conn.execute(f"CREATE (p:Point {{id: {i}, coords: [{i}, {i + 1}]}});")

    # Single update
    conn.execute("MATCH (p:Point) WHERE p.id = 0 SET p.coords = [300, 400];")
    rows = list(conn.execute("MATCH (p:Point) WHERE p.id = 0 RETURN p.coords;"))
    assert list(rows[0][0]) == [300, 400]

    # Batch update
    conn.execute("MATCH (p:Point) SET p.coords = [999, 888];")
    rows = list(conn.execute("MATCH (p:Point) RETURN p.coords ORDER BY p.id;"))
    assert len(rows) == 3
    for row in rows:
        assert _nested_list(row[0]) == [999, 888]

    conn.close()
    db.close()


def test_array_set_with_variable_reference(tmp_path):
    """SET an array property from another vertex's property (variable ref)."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor(id INT64, readings INT32[2], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (s:Sensor {id: 1, readings: [10, 20]});")
    conn.execute("CREATE (s:Sensor {id: 2, readings: [0, 0]});")

    conn.execute(
        "MATCH (a:Sensor), (b:Sensor) "
        "WHERE a.id = 1 AND b.id = 2 "
        "SET b.readings = a.readings;"
    )

    rows = list(conn.execute("MATCH (s:Sensor) WHERE s.id = 2 RETURN s.readings;"))
    assert _nested_list(rows[0][0]) == [10, 20]

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# MERGE vertex tests
# ---------------------------------------------------------------------------


def test_merge_vertex_array(tmp_path):
    """MERGE vertex with array: ON CREATE, ON MATCH SET, ON CREATE SET paths."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor(id INT64, readings INT32[2], PRIMARY KEY(id));"
    )

    # MERGE creates new vertex (ON CREATE path)
    conn.execute("MERGE (s:Sensor {id: 1, readings: [10, 20]});")
    rows = list(conn.execute("MATCH (s:Sensor) WHERE s.id = 1 RETURN s.readings;"))
    assert _nested_list(rows[0][0]) == [10, 20]

    # MERGE matches existing vertex, updates via ON MATCH SET
    conn.execute("MERGE (s:Sensor {id: 1}) ON MATCH SET s.readings = [30, 40];")
    rows = list(conn.execute("MATCH (s:Sensor) WHERE s.id = 1 RETURN s.readings;"))
    assert _nested_list(rows[0][0]) == [30, 40]

    # MERGE with ON CREATE SET (new vertex)
    conn.execute(
        "MERGE (s:Sensor {id: 2, readings: [0, 0]}) "
        "ON CREATE SET s.readings = [7, 8];"
    )
    rows = list(conn.execute("MATCH (s:Sensor) WHERE s.id = 2 RETURN s.readings;"))
    assert _nested_list(rows[0][0]) == [7, 8]

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Edge Array tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "prop_def,insert_val,expected",
    [
        ("weights INT64[2]", "[7, 9]", [7, 9]),
        ("coords DOUBLE[2]", "[1.5, 2.5]", [1.5, 2.5]),
        ("tags STRING[2]", "['friend', 'colleague']", ["friend", "colleague"]),
    ],
)
def test_edge_array_create_query(tmp_path, prop_def, insert_val, expected):
    """Create, insert, and query array property on a relationship."""
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute(f"CREATE REL TABLE Knows(FROM Person TO Person, {prop_def});")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")

    prop_name = prop_def.split()[0]
    conn.execute(
        f"MATCH (a:Person {{id: 1}}), (b:Person {{id: 2}}) "
        f"CREATE (a)-[:Knows {{{prop_name}: {insert_val}}}]->(b);"
    )

    rows = list(
        conn.execute(f"MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.{prop_name};")
    )
    assert len(rows) == 1
    _approx_eq(_nested_list(rows[0][0]), expected)

    conn.close()
    db.close()


def test_edge_array_set_and_merge(tmp_path):
    """SET and MERGE operations on edge array properties."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weights INT64[2]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute("CREATE (p:Person {id: 3});")

    # CREATE + SET
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {weights: [1, 2]}]->(b);"
    )
    conn.execute(
        "MATCH (a:Person {id: 1})-[e:Knows]->(b:Person {id: 2}) "
        "SET e.weights = [99, 88];"
    )
    rows = list(
        conn.execute(
            "MATCH (a:Person {id: 1})-[e:Knows]->(b:Person {id: 2}) RETURN e.weights;"
        )
    )
    assert _nested_list(rows[0][0]) == [99, 88]

    # MERGE ON CREATE
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 3}) "
        "MERGE (a)-[e:Knows]->(b) "
        "ON CREATE SET e.weights = [10, 20];"
    )
    rows = list(
        conn.execute(
            "MATCH (a:Person {id: 1})-[e:Knows]->(b:Person {id: 3}) RETURN e.weights;"
        )
    )
    assert _nested_list(rows[0][0]) == [10, 20]

    # MERGE ON MATCH SET
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 3}) "
        "MERGE (a)-[e:Knows]->(b) "
        "ON MATCH SET e.weights = [55, 66];"
    )
    rows = list(
        conn.execute(
            "MATCH (a:Person {id: 1})-[e:Knows]->(b:Person {id: 3}) RETURN e.weights;"
        )
    )
    assert _nested_list(rows[0][0]) == [55, 66]

    conn.close()
    db.close()


def test_edge_array_multi_properties(tmp_path):
    """Edge with array + scalar properties (unbundled path)."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute(
        "CREATE REL TABLE Knows(FROM Person TO Person, weights INT64[2], since INT64);"
    )
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {weights: [7, 8], since: 2024}]->(b);"
    )

    rows = list(
        conn.execute(
            "MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.weights, e.since;"
        )
    )
    assert _nested_list(rows[0][0]) == [7, 8]
    assert rows[0][1] == 2024

    conn.execute(
        "MATCH (a:Person {id: 1})-[e:Knows]->(b:Person {id: 2}) "
        "SET e.weights = [100, 200], e.since = 2025;"
    )
    rows = list(
        conn.execute(
            "MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.weights, e.since;"
        )
    )
    assert _nested_list(rows[0][0]) == [100, 200]
    assert rows[0][1] == 2025

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Nested Array tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "array_type,values,expected",
    [
        ("INT32[2][2]", "[[1, 2], [3, 4]]", [[1, 2], [3, 4]]),
        ("INT32[2][3]", "[[1, 2], [3, 4], [5, 6]]", [[1, 2], [3, 4], [5, 6]]),
        ("DOUBLE[2][2]", "[[1.5, 2.5], [3.5, 4.5]]", [[1.5, 2.5], [3.5, 4.5]]),
    ],
)
def test_nested_array_create_query(tmp_path, array_type, values, expected):
    """Create and query nested array properties of various types."""
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    conn.execute(
        f"CREATE NODE TABLE Matrix(id INT64, grid {array_type}, PRIMARY KEY(id));"
    )
    conn.execute(f"CREATE (m:Matrix {{id: 1, grid: {values}}});")

    rows = list(conn.execute("MATCH (m:Matrix) WHERE m.id = 1 RETURN m.grid;"))
    assert len(rows) == 1
    _approx_eq(_nested_list(rows[0][0]), expected)

    conn.close()
    db.close()


def test_nested_array_set_property(tmp_path):
    """SET a nested array property on an existing node."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Matrix(id INT64, grid INT32[2][3], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (m:Matrix {id: 1, grid: [[1, 2], [3, 4], [5, 6]]});")

    conn.execute("MATCH (m:Matrix {id: 1}) SET m.grid = [[9, 8], [7, 6], [5, 4]];")

    rows = list(conn.execute("MATCH (m:Matrix) WHERE m.id = 1 RETURN m.grid;"))
    assert _nested_list(rows[0][0]) == [[9, 8], [7, 6], [5, 4]]

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Checkpoint & Reopen tests
# ---------------------------------------------------------------------------


def test_checkpoint_reopen_array(tmp_path):
    """Array properties survive checkpoint and reopen (vertex, edge, nested)."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    # Vertex array
    conn.execute(
        "CREATE NODE TABLE Vector(id INT64, coords DOUBLE[2], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (v:Vector {id: 1, coords: [1.25, 2.5]});")

    # Edge array
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weights INT64[3]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {weights: [3, 6, 9]}]->(b);"
    )

    # Nested array
    conn.execute(
        "CREATE NODE TABLE Matrix(id INT64, grid INT32[2][2], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (m:Matrix {id: 1, grid: [[10, 20], [30, 40]]});")

    conn.close()
    db.close()

    # Reopen and verify all
    db2 = Database(db_path=str(tmp_path), mode="r")
    conn2 = db2.connect()

    rows = list(conn2.execute("MATCH (v:Vector) RETURN v.coords;"))
    _approx_eq(_nested_list(rows[0][0]), [1.25, 2.5])

    rows = list(
        conn2.execute("MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.weights;")
    )
    assert _nested_list(rows[0][0]) == [3, 6, 9]

    rows = list(conn2.execute("MATCH (m:Matrix) RETURN m.grid;"))
    assert _nested_list(rows[0][0]) == [[10, 20], [30, 40]]

    conn2.close()
    db2.close()


# ---------------------------------------------------------------------------
# DDL tests
# ---------------------------------------------------------------------------


def test_array_ddl_operations(tmp_path):
    """ALTER ADD/DROP array columns and explicit array DEFAULT literals."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Device(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE (d:Device {id: 1});")

    # ALTER ADD initializes with zero defaults
    conn.execute("ALTER TABLE Device ADD readings INT32[2];")
    rows = list(conn.execute("MATCH (d:Device) RETURN d.readings;"))
    assert _nested_list(rows[0][0]) == [0, 0]

    # ALTER DROP removes the column
    conn.execute("ALTER TABLE Device DROP readings;")
    with pytest.raises(Exception):
        list(conn.execute("MATCH (d:Device) RETURN d.readings;"))

    # Explicit DEFAULT literal is used when the property is omitted.
    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[3] DEFAULT [0, 1, 0],"
        "  PRIMARY KEY(id)"
        ");"
    )

    conn.execute("CREATE (s:Sensor {id: 1});")
    rows = list(conn.execute("MATCH (s:Sensor {id: 1}) RETURN s.readings;"))
    assert _nested_list(rows[0][0]) == [0, 1, 0]

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Filter & Query Operation tests
# ---------------------------------------------------------------------------


def test_array_equality_filter(tmp_path):
    """Filter rows by exact array equality."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor(id INT64, readings INT32[3], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (s:Sensor {id: 1, readings: [1, 2, 3]});")
    conn.execute("CREATE (s:Sensor {id: 2, readings: [4, 5, 6]});")

    rows = list(
        conn.execute(
            "MATCH (s:Sensor) WHERE s.readings = [4, 5, 6] RETURN s.id, s.readings;"
        )
    )
    assert len(rows) == 1
    assert rows[0][0] == 2
    assert _nested_list(rows[0][1]) == [4, 5, 6]

    conn.close()
    db.close()


def test_array_collect_and_unwind(tmp_path):
    """collect() aggregates array properties; UNWIND expands array elements."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Vector(id INT64, coords INT64[2], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (v:Vector {id: 1, coords: [1, 10]});")
    conn.execute("CREATE (v:Vector {id: 2, coords: [2, 20]});")

    # collect()
    rows = list(conn.execute("MATCH (v:Vector) RETURN collect(v.coords);"))
    assert sorted(_nested_list(rows[0][0])) == [[1, 10], [2, 20]]

    # UNWIND
    conn.execute(
        "CREATE NODE TABLE Sensor(id INT64, readings INT32[3], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (s:Sensor {id: 1, readings: [3, 1, 2]});")
    rows = list(
        conn.execute(
            "MATCH (s:Sensor) UNWIND s.readings AS reading "
            "RETURN reading ORDER BY reading;"
        )
    )
    assert [row[0] for row in rows] == [1, 2, 3]

    conn.close()
    db.close()


def test_join_returns_array_property(tmp_path):
    """Array-valued properties survive joins."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Hub(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute(
        "CREATE NODE TABLE Device("
        "  id INT64, hub_id INT64, readings INT32[2], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (h:Hub {id: 10, name: 'west'});")
    conn.execute("CREATE (h:Hub {id: 20, name: 'east'});")
    conn.execute("CREATE (d:Device {id: 1, hub_id: 10, readings: [3, 4]});")
    conn.execute("CREATE (d:Device {id: 2, hub_id: 20, readings: [5, 6]});")

    rows = list(
        conn.execute(
            "MATCH (d:Device), (h:Hub) "
            "WHERE d.hub_id = h.id AND h.name = 'east' "
            "RETURN h.name, d.readings;"
        )
    )
    assert len(rows) == 1
    assert rows[0][0] == "east"
    assert _nested_list(rows[0][1]) == [5, 6]

    conn.close()
    db.close()


def test_bolt_response_contains_array_field(tmp_path):
    """Bolt JSON response encodes array-valued properties as JSON arrays."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Vector(id INT64, coords INT32[2], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (v:Vector {id: 1, coords: [3, 4]});")

    result = conn.execute("MATCH (v:Vector) WHERE v.id = 1 RETURN v.coords AS coords;")
    payload = json.loads(result.get_bolt_response())
    assert payload["table"] == [{"coords": [3, 4]}]
    assert payload["raw"]["records"][0]["_fields"] == [[3, 4]]

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Corner cases & type safety
# ---------------------------------------------------------------------------


def test_array_null_handling(tmp_path):
    """NULL array property uses defaults; SET to NULL is unsupported."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor(id INT64, readings INT32[2], PRIMARY KEY(id));"
    )

    # CREATE with NULL -> defaults
    conn.execute("CREATE (s:Sensor {id: 1, readings: NULL});")
    rows = list(conn.execute("MATCH (s:Sensor) WHERE s.id = 1 RETURN s.readings;"))
    assert _nested_list(rows[0][0]) == [0, 0]

    # SET to NULL -> error
    conn.execute("CREATE (s:Sensor {id: 2, readings: [10, 20]});")
    with pytest.raises(Exception):
        conn.execute("MATCH (s:Sensor) WHERE s.id = 2 SET s.readings = NULL;")

    conn.close()
    db.close()


def test_array_wrong_size_throws(tmp_path):
    """Inserting an array with wrong number of elements should fail."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor(id INT64, readings INT32[3], PRIMARY KEY(id));"
    )

    with pytest.raises(Exception):
        conn.execute("CREATE (s:Sensor {id: 1, readings: [1, 2]});")

    with pytest.raises(Exception):
        conn.execute("CREATE (s:Sensor {id: 2, readings: [1, 2, 3, 4]});")

    conn.close()
    db.close()


def test_array_zero_size_rejected(tmp_path):
    """Declaring an array with size 0 should be rejected at parse time."""
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    with pytest.raises(Exception, match="greater than 0"):
        conn.execute("CREATE NODE TABLE Bad(id INT64, arr INT32[0], PRIMARY KEY(id));")

    conn.close()
    db.close()


def test_cast_does_not_convert_between_list_and_array(tmp_path):
    """CAST should not normalize LIST and fixed-size ARRAY values."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor(id INT64, readings INT32[3], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (s:Sensor {id: 1, readings: [1, 2, 3]});")

    with pytest.raises(Exception):
        list(conn.execute("MATCH (s:Sensor) RETURN CAST(s.readings, 'INT32[]');"))

    with pytest.raises(Exception):
        list(
            conn.execute(
                "UNWIND [1, 2, 3] AS v "
                "WITH collect(v) AS values "
                "RETURN CAST(values, 'INT64[3]');"
            )
        )

    conn.close()
    db.close()


def test_array_create_with_mixed_types(tmp_path):
    """CREATE with INT literals into a DOUBLE array (implicit numeric cast)."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Vector(id INT64, embedding DOUBLE[2], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (v:Vector {id: 1, embedding: [1, 2]});")

    rows = list(conn.execute("MATCH (v:Vector) RETURN v.embedding;"))
    _approx_eq(_nested_list(rows[0][0]), [1.0, 2.0])

    conn.close()
    db.close()


def test_list_and_array_index_supported(tmp_path):
    """List and fixed-size array extraction both use zero-based indexes."""
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    rows = list(conn.execute("RETURN [10, 20, 30][0];"))
    assert rows[0][0] == 10

    conn.execute(
        "CREATE NODE TABLE Sensor(id INT64, readings INT32[3], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (s:Sensor {id: 1, readings: [10, 20, 30]});")
    res = list(conn.execute("MATCH (s:Sensor) RETURN s.readings[2];"))
    assert res[0][0] == 30

    conn.close()
    db.close()
