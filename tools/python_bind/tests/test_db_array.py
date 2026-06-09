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

import pytest

from neug.database import Database


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

    conn.execute(
        "CREATE (s:Sensor {id: 1, readings: [10, 20, 30]});"
    )
    conn.execute(
        "CREATE (s:Sensor {id: 2, readings: [40, 50, 60]});"
    )

    result = conn.execute(
        "MATCH (s:Sensor) WHERE s.id = 1 RETURN s.readings;"
    )
    record = result.__next__()
    assert list(record[0]) == [10, 20, 30]

    result = conn.execute(
        "MATCH (s:Sensor) WHERE s.id = 2 RETURN s.readings;"
    )
    record = result.__next__()
    assert list(record[0]) == [40, 50, 60]

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

    conn.execute(
        "CREATE (v:Vector {id: 1, embedding: [1.5, 2.5]});"
    )

    result = conn.execute(
        "MATCH (v:Vector) WHERE v.id = 1 RETURN v.embedding;"
    )
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

    conn.execute(
        "CREATE (p:Point {id: 1, coords: [100, 200]});"
    )

    conn.execute(
        "MATCH (p:Point) WHERE p.id = 1 SET p.coords = [300, 400];"
    )

    result = conn.execute(
        "MATCH (p:Point) WHERE p.id = 1 RETURN p.coords;"
    )
    record = result.__next__()
    assert list(record[0]) == [300, 400]

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
