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


def test_array_int32_create_and_query(tmp_path):
    """Create a vertex type with INT32[3] array property, insert and query."""
    db_dir = tmp_path / "array_int32"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[3],"
        "  PRIMARY KEY(id)"
        ");"
    )

    conn.execute("CREATE (s:Sensor {id: 1, readings: [10, 20, 30]});")
    conn.execute("CREATE (s:Sensor {id: 2, readings: [40, 50, 60]});")

    result = conn.execute("MATCH (s:Sensor) WHERE s.id = 1 RETURN s.readings;")
    record = result.__next__()
    assert list(record[0]) == [10, 20, 30]

    result = conn.execute("MATCH (s:Sensor) WHERE s.id = 2 RETURN s.readings;")
    record = result.__next__()
    assert list(record[0]) == [40, 50, 60]

    conn.close()
    db.close()


def test_edge_array_create_query(tmp_path):
    """Create, insert, and query an array property on a relationship."""
    db_dir = tmp_path / "array_edge"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weights INT64[2]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person), (b:Person) "
        "WHERE a.id = 1 AND b.id = 2 "
        "CREATE (a)-[:Knows {weights: [7, 9]}]->(b);"
    )

    rows = list(
        conn.execute(
            "MATCH (a:Person)-[e:Knows]->(b:Person) " "RETURN a.id, b.id, e.weights;"
        )
    )
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert rows[0][1] == 2
    assert _nested_list(rows[0][2]) == [7, 9]

    conn.close()
    db.close()


def test_edge_array_set_property(tmp_path):
    """SET an edge array property after creation."""
    db_dir = tmp_path / "edge_array_set"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weights INT64[2]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {weights: [1, 2]}]->(b);"
    )

    conn.execute(
        "MATCH (a:Person {id: 1})-[e:Knows]->(b:Person {id: 2}) "
        "SET e.weights = [99, 88];"
    )

    rows = list(
        conn.execute("MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.weights;")
    )
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [99, 88]

    conn.close()
    db.close()


def test_edge_array_merge_on_create(tmp_path):
    """MERGE edge ON CREATE with array property."""
    db_dir = tmp_path / "edge_array_merge_create"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weights INT64[2]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")

    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "MERGE (a)-[e:Knows]->(b) "
        "ON CREATE SET e.weights = [10, 20];"
    )

    rows = list(
        conn.execute("MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.weights;")
    )
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [10, 20]

    conn.close()
    db.close()


def test_edge_array_merge_on_match_set(tmp_path):
    """MERGE edge ON MATCH SET with array property."""
    db_dir = tmp_path / "edge_array_merge_match"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weights INT64[2]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {weights: [1, 2]}]->(b);"
    )

    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "MERGE (a)-[e:Knows]->(b) "
        "ON MATCH SET e.weights = [55, 66];"
    )

    rows = list(
        conn.execute("MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.weights;")
    )
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [55, 66]

    conn.close()
    db.close()


def test_edge_array_checkpoint_reopen(tmp_path):
    """Edge array property survives checkpoint and reopen."""
    db_dir = tmp_path / "edge_array_checkpoint"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weights INT64[3]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {weights: [3, 6, 9]}]->(b);"
    )

    conn.close()
    db.close()

    db2 = Database(db_path=str(db_dir), mode="r")
    conn2 = db2.connect()
    rows = list(
        conn2.execute("MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.weights;")
    )
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [3, 6, 9]

    conn2.close()
    db2.close()


def test_edge_array_double_type(tmp_path):
    """DOUBLE[2] edge array property."""
    db_dir = tmp_path / "edge_array_double"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, coords DOUBLE[2]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {coords: [1.5, 2.5]}]->(b);"
    )

    rows = list(conn.execute("MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.coords;"))
    assert len(rows) == 1
    coords = list(rows[0][0])
    assert abs(coords[0] - 1.5) < 1e-5
    assert abs(coords[1] - 2.5) < 1e-5

    conn.close()
    db.close()


def test_edge_array_string_type(tmp_path):
    """STRING[2] edge array property."""
    db_dir = tmp_path / "edge_array_string"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, tags STRING[2]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {tags: ['friend', 'colleague']}]->(b);"
    )

    rows = list(conn.execute("MATCH (a:Person)-[e:Knows]->(b:Person) RETURN e.tags;"))
    assert len(rows) == 1
    tags = _nested_list(rows[0][0])
    assert tags == ["friend", "colleague"]

    conn.close()
    db.close()


def test_edge_array_multi_properties(tmp_path):
    """Edge with array + scalar properties (unbundled path)."""
    db_dir = tmp_path / "edge_array_multi"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute(
        "CREATE REL TABLE Knows(FROM Person TO Person, "
        "weights INT64[2], since INT64);"
    )
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {weights: [7, 8], since: 2024}]->(b);"
    )

    rows = list(
        conn.execute(
            "MATCH (a:Person)-[e:Knows]->(b:Person) " "RETURN e.weights, e.since;"
        )
    )
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [7, 8]
    assert rows[0][1] == 2024

    conn.execute(
        "MATCH (a:Person {id: 1})-[e:Knows]->(b:Person {id: 2}) "
        "SET e.weights = [100, 200], e.since = 2025;"
    )
    rows = list(
        conn.execute(
            "MATCH (a:Person)-[e:Knows]->(b:Person) " "RETURN e.weights, e.since;"
        )
    )
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [100, 200]
    assert rows[0][1] == 2025

    conn.close()
    db.close()


def test_array_equality_filter(tmp_path):
    """Filter rows by exact array equality."""
    db_dir = tmp_path / "array_equality"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[3],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (s:Sensor {id: 1, readings: [1, 2, 3]});")
    conn.execute("CREATE (s:Sensor {id: 2, readings: [4, 5, 6]});")

    rows = list(
        conn.execute(
            "MATCH (s:Sensor) "
            "WHERE s.readings = [4, 5, 6] "
            "RETURN s.id, s.readings;"
        )
    )
    assert len(rows) == 1
    assert rows[0][0] == 2
    assert _nested_list(rows[0][1]) == [4, 5, 6]

    conn.close()
    db.close()


def test_alter_add_drop_array_column(tmp_path):
    """ALTER ADD initializes array defaults, and ALTER DROP removes the column."""
    db_dir = tmp_path / "array_alter"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Device(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE (d:Device {id: 1});")
    conn.execute("ALTER TABLE Device ADD readings INT32[2];")

    rows = list(conn.execute("MATCH (d:Device) RETURN d.readings;"))
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [0, 0]

    conn.execute("ALTER TABLE Device DROP readings;")
    with pytest.raises(Exception):
        list(conn.execute("MATCH (d:Device) RETURN d.readings;"))

    conn.close()
    db.close()


def test_checkpoint_reopens_array_column(tmp_path):
    """Array column descriptors and child files survive checkpoint/reopen."""
    db_dir = tmp_path / "array_checkpoint"
    db_dir.mkdir()

    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE Vector("
        "  id INT64,"
        "  coords DOUBLE[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (v:Vector {id: 1, coords: [1.25, 2.5]});")
    conn.execute("CHECKPOINT;")
    conn.close()
    db.close()

    db = Database(db_path=str(db_dir), mode="r")
    conn = db.connect()
    rows = list(conn.execute("MATCH (v:Vector) RETURN v.id, v.coords;"))
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert _nested_list(rows[0][1]) == [1.25, 2.5]

    conn.close()
    db.close()


def test_nested_array_property(tmp_path):
    """Create and query a fixed-size array whose element type is another array."""
    db_dir = tmp_path / "array_nested"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Matrix("
        "  id INT64,"
        "  values INT32[2][2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (m:Matrix {id: 1, values: [[1, 2], [3, 4]]});")

    rows = list(conn.execute("MATCH (m:Matrix) RETURN m.values;"))
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [[1, 2], [3, 4]]

    conn.close()
    db.close()


def test_collect_array_property(tmp_path):
    """collect() can aggregate array-valued properties."""
    db_dir = tmp_path / "array_collect"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Vector("
        "  id INT64,"
        "  coords INT64[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (v:Vector {id: 1, coords: [1, 10]});")
    conn.execute("CREATE (v:Vector {id: 2, coords: [2, 20]});")

    rows = list(conn.execute("MATCH (v:Vector) RETURN collect(v.coords);"))
    assert len(rows) == 1
    assert sorted(_nested_list(rows[0][0])) == [[1, 10], [2, 20]]

    conn.close()
    db.close()


def test_join_returns_array_property(tmp_path):
    """Array-valued properties survive joins."""
    db_dir = tmp_path / "array_join"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Hub(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute(
        "CREATE NODE TABLE Device("
        "  id INT64,"
        "  hub_id INT64,"
        "  readings INT32[2],"
        "  PRIMARY KEY(id)"
        ");"
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
    db_dir = tmp_path / "array_bolt"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Vector("
        "  id INT64,"
        "  coords INT32[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (v:Vector {id: 1, coords: [3, 4]});")

    result = conn.execute("MATCH (v:Vector) WHERE v.id = 1 RETURN v.coords AS coords;")
    payload = json.loads(result.get_bolt_response())

    assert payload["table"] == [{"coords": [3, 4]}]
    assert payload["raw"]["records"][0]["_fields"] == [[3, 4]]

    conn.close()
    db.close()


def test_array_double_create_and_query(tmp_path):
    """Create a vertex type with DOUBLE[2] array property, insert and query."""
    db_dir = tmp_path / "array_double"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Vector("
        "  id INT64,"
        "  embedding DOUBLE[2],"
        "  PRIMARY KEY(id)"
        ");"
    )

    conn.execute("CREATE (v:Vector {id: 1, embedding: [1.5, 2.5]});")

    result = conn.execute("MATCH (v:Vector) WHERE v.id = 1 RETURN v.embedding;")
    record = result.__next__()
    values = list(record[0])
    assert abs(values[0] - 1.5) < 1e-6
    assert abs(values[1] - 2.5) < 1e-6

    conn.close()
    db.close()


def test_array_update(tmp_path):
    """Update an array property value."""
    db_dir = tmp_path / "array_update"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Point("
        "  id INT64,"
        "  coords INT64[2],"
        "  PRIMARY KEY(id)"
        ");"
    )

    conn.execute("CREATE (p:Point {id: 1, coords: [100, 200]});")

    conn.execute("MATCH (p:Point) WHERE p.id = 1 SET p.coords = [300, 400];")

    result = conn.execute("MATCH (p:Point) WHERE p.id = 1 RETURN p.coords;")
    record = result.__next__()
    assert list(record[0]) == [300, 400]

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# MERGE with Array tests
# ---------------------------------------------------------------------------


def test_merge_vertex_on_create_array(tmp_path):
    """MERGE creates a new vertex with an array property (ON CREATE path)."""
    db_dir = tmp_path / "array_merge_create"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[2],"
        "  PRIMARY KEY(id)"
        ");"
    )

    conn.execute("MERGE (s:Sensor {id: 1, readings: [10, 20]});")

    rows = list(conn.execute("MATCH (s:Sensor) RETURN s.id, s.readings;"))
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert _nested_list(rows[0][1]) == [10, 20]

    conn.close()
    db.close()


def test_merge_vertex_on_match_set_array(tmp_path):
    """MERGE matches an existing vertex and updates array via ON MATCH SET."""
    db_dir = tmp_path / "array_merge_match_set"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (s:Sensor {id: 1, readings: [1, 2]});")

    conn.execute("MERGE (s:Sensor {id: 1}) " "ON MATCH SET s.readings = [99, 88];")

    rows = list(conn.execute("MATCH (s:Sensor) RETURN s.id, s.readings;"))
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert _nested_list(rows[0][1]) == [99, 88]

    conn.close()
    db.close()


def test_merge_vertex_on_create_set_array(tmp_path):
    """MERGE creates a vertex and sets array via ON CREATE SET."""
    db_dir = tmp_path / "array_merge_create_set"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Vector("
        "  id INT64,"
        "  coords INT64[3],"
        "  PRIMARY KEY(id)"
        ");"
    )

    conn.execute(
        "MERGE (v:Vector {id: 1, coords: [0, 0, 0]}) "
        "ON CREATE SET v.coords = [7, 8, 9];"
    )

    rows = list(conn.execute("MATCH (v:Vector) RETURN v.id, v.coords;"))
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert _nested_list(rows[0][1]) == [7, 8, 9]

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Corner case tests
# ---------------------------------------------------------------------------


def test_array_set_with_variable_reference(tmp_path):
    """SET an array property from another vertex's property (variable ref)."""
    db_dir = tmp_path / "array_set_var"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (s:Sensor {id: 1, readings: [10, 20]});")
    conn.execute("CREATE (s:Sensor {id: 2, readings: [0, 0]});")

    conn.execute(
        "MATCH (a:Sensor), (b:Sensor) "
        "WHERE a.id = 1 AND b.id = 2 "
        "SET b.readings = a.readings;"
    )

    rows = list(conn.execute("MATCH (s:Sensor) WHERE s.id = 2 RETURN s.readings;"))
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [10, 20]

    conn.close()
    db.close()


def test_array_create_with_null_property(tmp_path):
    """CREATE vertex with NULL array property uses default values."""
    db_dir = tmp_path / "array_null_create"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (s:Sensor {id: 1, readings: NULL});")

    rows = list(conn.execute("MATCH (s:Sensor) RETURN s.readings;"))
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [0, 0]

    conn.close()
    db.close()


@pytest.mark.xfail(
    reason=(
        "Storage layer does not support setting an ARRAY property to NULL; "
        "ArrayValue::GetChildren throws on null values. "
        "Needs storage-level NULL array support first."
    )
)
def test_array_update_to_null(tmp_path):
    """SET an array property to NULL should use default values."""
    db_dir = tmp_path / "array_update_null"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Point("
        "  id INT64,"
        "  coords INT64[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (p:Point {id: 1, coords: [100, 200]});")

    conn.execute("MATCH (p:Point) WHERE p.id = 1 SET p.coords = NULL;")

    rows = list(conn.execute("MATCH (p:Point) RETURN p.coords;"))
    assert len(rows) == 1
    assert _nested_list(rows[0][0]) == [0, 0]

    conn.close()
    db.close()


def test_array_wrong_size_throws(tmp_path):
    """Inserting an array with wrong number of elements should fail."""
    db_dir = tmp_path / "array_wrong_size"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[3],"
        "  PRIMARY KEY(id)"
        ");"
    )

    # Too few elements
    with pytest.raises(Exception):
        conn.execute("CREATE (s:Sensor {id: 1, readings: [1, 2]});")

    # Too many elements
    with pytest.raises(Exception):
        conn.execute("CREATE (s:Sensor {id: 2, readings: [1, 2, 3, 4]});")

    conn.close()
    db.close()


def test_array_update_multiple_vertices_batch(tmp_path):
    """Batch SET array property on multiple vertices at once."""
    db_dir = tmp_path / "array_batch_update"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Point("
        "  id INT64,"
        "  coords INT64[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    for i in range(5):
        conn.execute(f"CREATE (p:Point {{id: {i}, coords: [{i}, {i + 1}]}});")

    conn.execute("MATCH (p:Point) SET p.coords = [999, 888];")

    rows = list(conn.execute("MATCH (p:Point) RETURN p.coords ORDER BY p.id;"))
    assert len(rows) == 5
    for row in rows:
        assert _nested_list(row[0]) == [999, 888]

    conn.close()
    db.close()


def test_array_create_with_mixed_types(tmp_path):
    """CREATE with INT literals into a DOUBLE array (implicit numeric cast)."""
    db_dir = tmp_path / "array_mixed_types"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Vector("
        "  id INT64,"
        "  embedding DOUBLE[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (v:Vector {id: 1, embedding: [1, 2]});")

    rows = list(conn.execute("MATCH (v:Vector) RETURN v.embedding;"))
    assert len(rows) == 1
    vals = list(rows[0][0])
    assert abs(vals[0] - 1.0) < 1e-6
    assert abs(vals[1] - 2.0) < 1e-6

    conn.close()
    db.close()


def test_array_merge_create_then_match_update(tmp_path):
    """End-to-end: MERGE CREATE then MERGE MATCH UPDATE on same vertex."""
    db_dir = tmp_path / "array_merge_e2e"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[2],"
        "  PRIMARY KEY(id)"
        ");"
    )

    # First MERGE: creates the vertex
    conn.execute("MERGE (s:Sensor {id: 1, readings: [10, 20]});")

    # Second MERGE: matches by PK only and updates via ON MATCH SET
    conn.execute("MERGE (s:Sensor {id: 1}) " "ON MATCH SET s.readings = [30, 40];")

    rows = list(conn.execute("MATCH (s:Sensor) RETURN s.id, s.readings;"))
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert _nested_list(rows[0][1]) == [30, 40]

    conn.close()
    db.close()


def test_array_string_type(tmp_path):
    """Create, insert, and query a STRING array property."""
    db_dir = tmp_path / "array_string"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Tag("
        "  id INT64,"
        "  labels STRING[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (t:Tag {id: 1, labels: ['hello', 'world']});")

    rows = list(conn.execute("MATCH (t:Tag) RETURN t.id, t.labels;"))
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert _nested_list(rows[0][1]) == ["hello", "world"]

    conn.close()
    db.close()


def test_array_multiple_rows(tmp_path):
    """Insert and query multiple rows with array properties."""
    db_dir = tmp_path / "array_multi"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Item("
        "  id INT64,"
        "  features FLOAT[3],"
        "  PRIMARY KEY(id)"
        ");"
    )

    for i in range(5):
        vals = [float(i * 3 + j) for j in range(3)]
        conn.execute(
            f"CREATE (item:Item {{id: {i}, features: [{vals[0]}, {vals[1]}, {vals[2]}]}});"
        )

    result = conn.execute(
        "MATCH (item:Item) RETURN item.id, item.features ORDER BY item.id;"
    )
    rows = list(result)
    assert len(rows) == 5
    for i, row in enumerate(rows):
        expected = [float(i * 3 + j) for j in range(3)]
        actual = list(row[1])
        for a, e in zip(actual, expected):
            assert abs(a - e) < 1e-5, f"Row {i}: expected {e}, got {a}"

    conn.close()
    db.close()


def test_nested_array_int32_create_query(tmp_path):
    """Create a node with INT32[2][3] nested array, insert and query."""
    db_dir = tmp_path / "nested_array_int32"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Matrix("
        "  id INT64,"
        "  grid INT32[2][3],"
        "  PRIMARY KEY(id)"
        ");"
    )

    conn.execute("CREATE (m:Matrix {id: 1, grid: [[1, 2, 3], [4, 5, 6]]});")

    result = conn.execute("MATCH (m:Matrix) WHERE m.id = 1 RETURN m.grid;")
    rows = list(result)
    assert len(rows) == 1
    outer = list(rows[0][0])
    assert len(outer) == 2
    assert list(outer[0]) == [1, 2, 3]
    assert list(outer[1]) == [4, 5, 6]

    conn.close()
    db.close()


def test_nested_array_double_create_query(tmp_path):
    """Create a node with DOUBLE[2][2] nested array."""
    db_dir = tmp_path / "nested_array_double"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Tensor("
        "  id INT64,"
        "  coords DOUBLE[2][2],"
        "  PRIMARY KEY(id)"
        ");"
    )

    conn.execute("CREATE (t:Tensor {id: 1, coords: [[1.5, 2.5], [3.5, 4.5]]});")

    result = conn.execute("MATCH (t:Tensor) WHERE t.id = 1 RETURN t.coords;")
    rows = list(result)
    assert len(rows) == 1
    outer = list(rows[0][0])
    assert len(outer) == 2
    assert abs(list(outer[0])[0] - 1.5) < 0.01
    assert abs(list(outer[0])[1] - 2.5) < 0.01
    assert abs(list(outer[1])[0] - 3.5) < 0.01
    assert abs(list(outer[1])[1] - 4.5) < 0.01

    conn.close()
    db.close()


def test_nested_array_set_property(tmp_path):
    """SET a nested array property on an existing node."""
    db_dir = tmp_path / "nested_array_set"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Matrix("
        "  id INT64,"
        "  grid INT32[2][3],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (m:Matrix {id: 1, grid: [[1, 2, 3], [4, 5, 6]]});")

    conn.execute("MATCH (m:Matrix {id: 1}) SET m.grid = [[9, 8, 7], [6, 5, 4]];")

    result = conn.execute("MATCH (m:Matrix) WHERE m.id = 1 RETURN m.grid;")
    rows = list(result)
    assert len(rows) == 1
    outer = list(rows[0][0])
    assert list(outer[0]) == [9, 8, 7]
    assert list(outer[1]) == [6, 5, 4]

    conn.close()
    db.close()


def test_nested_array_checkpoint_reopen(tmp_path):
    """Nested array survives checkpoint and reopen."""
    db_dir = tmp_path / "nested_array_checkpoint"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Matrix("
        "  id INT64,"
        "  grid INT32[2][2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (m:Matrix {id: 1, grid: [[10, 20], [30, 40]]});")

    conn.close()
    db.close()

    db2 = Database(db_path=str(db_dir), mode="r")
    conn2 = db2.connect()
    result = conn2.execute("MATCH (m:Matrix) WHERE m.id = 1 RETURN m.grid;")
    rows = list(result)
    assert len(rows) == 1
    outer = list(rows[0][0])
    assert list(outer[0]) == [10, 20]
    assert list(outer[1]) == [30, 40]

    conn2.close()
    db2.close()


def test_unwind_array_property(tmp_path):
    """UNWIND accepts fixed-size array properties."""
    db_dir = tmp_path / "array_unwind"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[3],"
        "  PRIMARY KEY(id)"
        ");"
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
