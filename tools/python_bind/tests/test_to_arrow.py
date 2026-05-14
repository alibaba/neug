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

"""
Integration tests: verify that neug and pyarrow can coexist in the same
process, and that PyQueryResult.to_arrow() produces correct pyarrow.Table
objects.

Prerequisites:
    - neug Python package installed (pip install -e .)
    - pyarrow installed (pip install pyarrow)
"""

import io
import logging
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

pa = pytest.importorskip("pyarrow")

from neug.database import Database

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Symbol isolation
# ---------------------------------------------------------------------------


def test_import_coexistence():
    """neug and pyarrow can be imported in the same process without crash."""
    import pyarrow

    import neug  # noqa: F401

    logger.info("pyarrow version: %s", pyarrow.__version__)


def test_symbol_isolation():
    """pyarrow internals work correctly when neug is loaded (no symbol clash)."""
    import pyarrow.csv

    import neug  # noqa: F401 – ensure neug is loaded first

    arr = pa.array([1, 2, 3, None, 5], type=pa.int32())
    table = pa.table({"values": arr})
    assert table.num_rows == 5
    assert table.column("values").null_count == 1

    # Exercise arrow::csv (neug also links arrow::csv statically)
    csv_data = "a,b\n1,hello\n2,world\n"
    table = pa.csv.read_csv(io.BytesIO(csv_data.encode()))
    assert table.num_rows == 2


# ---------------------------------------------------------------------------
# to_arrow() basic
# ---------------------------------------------------------------------------


def test_to_arrow_basic(tmp_path):
    """to_arrow() returns a correct pyarrow.Table for a simple query."""
    db_dir = tmp_path / "to_arrow_basic"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE PERSON(name STRING, age INT32, PRIMARY KEY(name));")
    conn.execute("CREATE (n:PERSON {name: 'Alice', age: 30});")
    conn.execute("CREATE (n:PERSON {name: 'Bob', age: 25});")

    result = conn.execute(
        "MATCH (n:PERSON) RETURN n.name AS name, n.age AS age ORDER BY n.name;"
    )

    table = result.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert table.num_columns == 2
    assert table.column_names == ["name", "age"]

    names = table.column("name").to_pylist()
    ages = table.column("age").to_pylist()
    assert names == ["Alice", "Bob"]
    assert ages == [30, 25]

    conn.close()
    db.close()


def test_to_arrow_empty_result(tmp_path):
    """to_arrow() returns an empty pyarrow.Table when no rows match."""
    db_dir = tmp_path / "to_arrow_empty"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE PERSON(name STRING, PRIMARY KEY(name));")

    result = conn.execute("MATCH (n:PERSON) RETURN n.name AS name;")
    table = result.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 0

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# to_arrow() data types
# ---------------------------------------------------------------------------


def test_to_arrow_basic_types(tmp_path):
    """to_arrow() handles INT32, UINT32, INT64, UINT64, STRING, BOOL,
    FLOAT, DOUBLE columns correctly."""
    db_dir = tmp_path / "to_arrow_types"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE T("
        "int32_prop INT32, uint32_prop UINT32, "
        "int64_prop INT64, uint64_prop UINT64, "
        "string_prop STRING, bool_prop BOOL, "
        "float_prop FLOAT, double_prop DOUBLE, "
        "PRIMARY KEY(int32_prop));"
    )
    conn.execute(
        "CREATE (n:T {int32_prop: 1, uint32_prop: 2, "
        "int64_prop: 3, uint64_prop: 4, string_prop: 'test', "
        "bool_prop: true, float_prop: 1.23, double_prop: 2.34});"
    )

    result = conn.execute(
        "MATCH (n:T) RETURN n.int32_prop, n.uint32_prop, "
        "n.int64_prop, n.uint64_prop, n.string_prop, "
        "n.bool_prop, n.float_prop, n.double_prop;"
    )

    table = result.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1

    row = {col: table.column(col).to_pylist()[0] for col in table.column_names}
    logger.info("Row: %s", row)

    # Verify values via column index (column names may be expressions)
    cols = [table.column(i).to_pylist()[0] for i in range(table.num_columns)]
    assert cols[0] == 1
    assert cols[1] == 2
    assert cols[2] == 3
    assert cols[3] == 4
    assert cols[4] == "test"
    assert cols[5] is True
    assert abs(cols[6] - 1.23) < 1e-5
    assert abs(cols[7] - 2.34) < 1e-10

    conn.close()
    db.close()


def test_to_arrow_multiple_rows(tmp_path):
    """to_arrow() correctly handles multiple rows."""
    db_dir = tmp_path / "to_arrow_multi"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE NUM(val INT32, PRIMARY KEY(val));")
    for i in range(10):
        conn.execute(f"CREATE (n:NUM {{val: {i}}});")

    result = conn.execute("MATCH (n:NUM) RETURN n.val AS val ORDER BY n.val;")
    table = result.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 10
    assert table.column("val").to_pylist() == list(range(10))

    conn.close()
    db.close()


def test_to_arrow_string_values(tmp_path):
    """to_arrow() correctly handles various string values including empty."""
    db_dir = tmp_path / "to_arrow_strings"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE S(id INT32, text STRING, PRIMARY KEY(id));")
    conn.execute("CREATE (n:S {id: 1, text: 'hello world'});")
    conn.execute("CREATE (n:S {id: 2, text: ''});")
    conn.execute("CREATE (n:S {id: 3, text: 'special chars: @#$%'});")

    result = conn.execute(
        "MATCH (n:S) RETURN n.id AS id, n.text AS text ORDER BY n.id;"
    )
    table = result.to_arrow()

    assert table.num_rows == 3
    texts = table.column("text").to_pylist()
    assert texts[0] == "hello world"
    assert texts[1] == ""
    assert texts[2] == "special chars: @#$%"

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# to_arrow() round-trip with pyarrow operations
# ---------------------------------------------------------------------------


def test_to_arrow_pyarrow_compute(tmp_path):
    """The pyarrow.Table returned by to_arrow() works with pyarrow.compute."""
    db_dir = tmp_path / "to_arrow_compute"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE V(id INT32, score DOUBLE, PRIMARY KEY(id));")
    conn.execute("CREATE (n:V {id: 1, score: 10.0});")
    conn.execute("CREATE (n:V {id: 2, score: 20.0});")
    conn.execute("CREATE (n:V {id: 3, score: 30.0});")

    result = conn.execute("MATCH (n:V) RETURN n.score AS score ORDER BY n.id;")
    table = result.to_arrow()

    # Use pyarrow compute to verify the data is usable
    import pyarrow.compute as pc

    assert pc.sum(table.column("score")).as_py() == 60.0
    assert pc.mean(table.column("score")).as_py() == 20.0

    conn.close()
    db.close()


def _make_db(tmp_path, name):
    """Helper: create a fresh writable database and return (db, conn)."""
    db_dir = tmp_path / name
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    return db, db.connect()


def test_to_arrow_null_handling(tmp_path):
    """to_arrow() produces the same values as getNext() for columns with
    missing properties, verifying that validity bitmaps (if present) are
    correctly propagated through the Arrow C Data Interface."""
    db, conn = _make_db(tmp_path, "nulls")
    conn.execute(
        "CREATE NODE TABLE N(id INT32, val INT64, label STRING, "
        "flag BOOL, score DOUBLE, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (n:N {id:1, val:100, label:'a', flag:true, score:1.5});")
    conn.execute("CREATE (n:N {id:2});")
    conn.execute("CREATE (n:N {id:3, val:300, label:'c', flag:false, score:3.5});")

    table = conn.execute(
        "MATCH (n:N) RETURN n.id AS id, n.val AS val, n.label AS label, "
        "n.flag AS flag, n.score AS score ORDER BY n.id;"
    ).to_arrow()

    assert table.num_rows == 3
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert table.column("val").to_pylist() == [100, 0, 300]
    assert table.column("label").to_pylist() == ["a", "", "c"]
    assert table.column("flag").to_pylist() == [True, True, False]
    scores = table.column("score").to_pylist()
    assert (
        abs(scores[0] - 1.5) < 1e-10
        and scores[1] == 0.0
        and abs(scores[2] - 3.5) < 1e-10
    )
    conn.close()
    db.close()


def test_to_arrow_date_column(tmp_path):
    """to_arrow() handles DATE columns (mapped to timestamp[ms])."""
    db, conn = _make_db(tmp_path, "date")
    conn.execute("CREATE NODE TABLE D(id INT32, d DATE, PRIMARY KEY(id));")
    conn.execute("CREATE (n:D {id:1, d:date('2024-01-15')});")
    conn.execute("CREATE (n:D {id:2, d:date('2000-06-30')});")

    dates = (
        conn.execute("MATCH (n:D) RETURN n.d AS d ORDER BY n.id;")
        .to_arrow()
        .column("d")
        .to_pylist()
    )

    assert (dates[0].year, dates[0].month, dates[0].day) == (2024, 1, 15)
    assert (dates[1].year, dates[1].month, dates[1].day) == (2000, 6, 30)
    conn.close()
    db.close()


def test_to_arrow_timestamp_column(tmp_path):
    """to_arrow() handles TIMESTAMP columns."""
    db, conn = _make_db(tmp_path, "ts")
    conn.execute("CREATE NODE TABLE TS(id INT32, ts TIMESTAMP, PRIMARY KEY(id));")
    conn.execute("CREATE (n:TS {id:1, ts:timestamp('2024-03-15 10:30:00')});")
    conn.execute("CREATE (n:TS {id:2, ts:timestamp('2000-01-01 00:00:00')});")

    timestamps = (
        conn.execute("MATCH (n:TS) RETURN n.ts AS ts ORDER BY n.id;")
        .to_arrow()
        .column("ts")
        .to_pylist()
    )

    assert (timestamps[0].year, timestamps[0].month, timestamps[0].day) == (2024, 3, 15)
    assert (timestamps[0].hour, timestamps[0].minute) == (10, 30)
    conn.close()
    db.close()


@pytest.mark.skip(reason="LIST support not implemented yet")
def test_to_arrow_list_column(tmp_path):
    """to_arrow() handles LIST columns with nested offsets."""
    db, conn = _make_db(tmp_path, "list")
    conn.execute("CREATE NODE TABLE Person(id INT32, nums INT64[], PRIMARY KEY(id));")
    conn.execute("CREATE (n:Person {id:1, nums:[10,20,30]});")
    conn.execute("CREATE (n:Person {id:2, nums:[40]});")
    conn.execute("CREATE (n:Person {id:3, nums:[]});")

    lists = (
        conn.execute("MATCH (n:Person) RETURN n.nums AS nums ORDER BY n.id;")
        .to_arrow()
        .column("nums")
        .to_pylist()
    )

    assert lists == [[10, 20, 30], [40], []]
    conn.close()
    db.close()


@pytest.mark.skip(reason="STRUCT support not implemented yet")
def test_to_arrow_struct_column(tmp_path):
    """to_arrow() handles STRUCT columns with multiple fields."""
    db, conn = _make_db(tmp_path, "struct")
    conn.execute(
        "CREATE NODE TABLE ST(id INT32, info STRUCT(x INT32, y STRING), "
        "PRIMARY KEY(id));"
    )
    conn.execute("CREATE (n:ST {id:1, info:{x:10, y:'hello'}});")
    conn.execute("CREATE (n:ST {id:2, info:{x:20, y:'world'}});")

    structs = (
        conn.execute("MATCH (n:ST) RETURN n.info AS info ORDER BY n.id;")
        .to_arrow()
        .column("info")
        .to_pylist()
    )

    assert structs[0].get("f0", structs[0].get("x")) == 10
    assert structs[1].get("f0", structs[1].get("x")) == 20
    conn.close()
    db.close()


def test_to_arrow_to_pandas(tmp_path):
    """The pyarrow.Table returned by to_arrow() can be converted to pandas."""
    pd = pytest.importorskip("pandas")  # noqa: F841

    db_dir = tmp_path / "to_arrow_pandas"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE P(id INT32, name STRING, PRIMARY KEY(id));")
    conn.execute("CREATE (n:P {id: 1, name: 'Alice'});")
    conn.execute("CREATE (n:P {id: 2, name: 'Bob'});")

    result = conn.execute(
        "MATCH (n:P) RETURN n.id AS id, n.name AS name ORDER BY n.id;"
    )
    table = result.to_arrow()
    df = table.to_pandas()

    assert len(df) == 2
    assert list(df["name"]) == ["Alice", "Bob"]
    assert list(df["id"]) == [1, 2]

    conn.close()
    db.close()
