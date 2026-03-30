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

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from neug.database import Database
from neug.proto.error_pb2 import ERR_BAD_ENCODING
from neug.proto.error_pb2 import ERR_COMPILATION
from neug.proto.error_pb2 import ERR_DIRECTORY_NOT_EXIST
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_INVALID_FILE
from neug.proto.error_pb2 import ERR_IO_ERROR
from neug.proto.error_pb2 import ERR_PERMISSION
from neug.proto.error_pb2 import ERR_QUERY_SYNTAX
from neug.proto.error_pb2 import ERR_SCHEMA_MISMATCH
from neug.proto.error_pb2 import ERR_TYPE_CONVERSION

EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
extension_test = pytest.mark.skipif(
    not EXTENSION_TESTS_ENABLED,
    reason="Extension tests disabled; set NEUG_RUN_EXTENSION_TESTS=1 to enable.",
)


def _neug_repo_root() -> Path:
    """tests/ -> python_bind/ -> tools/ -> repo root."""
    return Path(__file__).resolve().parents[3]


# DB-005-01
def test_import_default(tmp_path):
    db_dir = tmp_path / "import_default"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "person.csv"
    print(f"Creating CSV file at {csv_path}")
    with open(csv_path, "w") as f:
        f.write("id|name\n1|Alice\n2|Bob\n")
    conn.execute(f'COPY person FROM "{csv_path}";')
    res1 = conn.execute("MATCH (n:person) RETURN n;")
    assert len(res1) == 2
    conn.close()
    db.close()


def test_import_config(tmp_path):
    db_dir = tmp_path / "import_config"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "person.csv"
    with open(csv_path, "w") as f:
        f.write("1,Alice\n2,Bob\n3,Charlie\n")
    conn.execute(f'COPY person FROM "{csv_path}" (HEADER FALSE, DELIMITER=",");')
    res = conn.execute("MATCH (n:person) RETURN n;")
    assert len(res) == 3
    conn.close()
    db.close()


def test_double_quote(tmp_path):
    db_dir = tmp_path / "double_quote"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "person.csv"
    # create a csv where the name contains quotes, using double quote when copy from
    with open(csv_path, "w") as f:
        f.write('"1","["Alice"]"\n"2","["Bob"]"\n"3","["Charlie"]"\n')
    conn.execute(
        f'COPY person FROM "{csv_path}" (HEADER FALSE, DELIMITER=",", DOUBLE_QUOTE=true);'
    )
    res = conn.execute("MATCH (n:person) RETURN n;")
    assert len(res) == 3
    conn.close()
    db.close()
    # remove csv_path
    csv_path.unlink()


# DB-005-02
def test_import_bad_csv(tmp_path):
    db_dir = tmp_path / "bad_csv"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    csv_path = tmp_path / "bad.csv"
    with open(csv_path, "wb") as f:
        f.write(b"id\n1\n\xff\n2\n")

    with pytest.raises(Exception) as excinfo:
        conn.execute(f'COPY person FROM "{csv_path}";')
    assert str(ERR_IO_ERROR) in str(excinfo.value)
    conn.close()
    db.close()


# DB-005-03
def test_import_null(tmp_path):
    db_dir = tmp_path / "import_null"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "null.csv"
    with open(csv_path, "w") as f:
        f.write("id|name\n1|NULL\n2|NaN\n")
    conn.execute(f'COPY person FROM "{csv_path}";')
    res = conn.execute("MATCH (n:person) RETURN n")
    assert len(res) == 2
    conn.close()
    db.close()


# DB-005-04
def test_import_type_conversion1(tmp_path):
    db_dir = tmp_path / "import_type_conversion1"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "type.csv"
    with open(csv_path, "w") as f:
        f.write("id|name\n1|111\n2|222\n")
    conn.execute(f'COPY person FROM "{csv_path}"')
    conn.close()
    db.close()


def test_import_type_conversion2(tmp_path):
    db_dir = tmp_path / "import_type_conversion2"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person2(id INT64, age INT32, PRIMARY KEY(id));")
    csv_path2 = tmp_path / "type2.csv"
    with open(csv_path2, "w") as f:
        f.write("id|age\n1|30\n2|40\n")
    # This should raise an error due to type conversion failure
    conn.execute(f'COPY person2 FROM "{csv_path2}";')
    conn.close()
    db.close()


def test_import_type_conversion_overflow(tmp_path):
    db_dir = tmp_path / "import_type_conversion_overflow"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    csv_path = tmp_path / "type2.csv"
    with open(csv_path, "w") as f:
        f.write("id\n12345678901234567890\n")  # INT64 overflow
    # This should raise an error due to type conversion failure
    with pytest.raises(Exception) as excinfo:
        conn.execute(f'COPY person FROM "{csv_path}";')
    assert str(ERR_IO_ERROR) in str(excinfo.value)
    conn.close()
    db.close()


def test_import_string_pk(tmp_path):
    db_dir = tmp_path / "import_type"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "type.csv"
    with open(csv_path, "w") as f:
        f.write("id\nAlice\n")
    conn.execute(f'COPY person FROM "{csv_path}"')


def test_import_int32_pk(tmp_path):
    db_dir = tmp_path / "import_primary_key"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    # type of primary key is INT32
    conn.execute("CREATE NODE TABLE person(id INT32, name STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "person.csv"
    with open(csv_path, "w") as f:
        f.write("id|name\n1|Alice\n2|Bob\n")
    conn.execute(f'COPY person FROM "{csv_path}";')
    res = conn.execute("MATCH (n:person) RETURN n;")
    assert len(res) == 2
    conn.close()
    db.close()


def test_import_uint32_pk(tmp_path):
    db_dir = tmp_path / "import_uint32_pk"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    # type of primary key is UINT32
    conn.execute("CREATE NODE TABLE person(id UINT32, name STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "person.csv"
    with open(csv_path, "w") as f:
        f.write("id|name\n1|Alice\n2|Bob\n")
    conn.execute(f'COPY person FROM "{csv_path}";')
    res = conn.execute("MATCH (n:person) RETURN n;")
    assert len(res) == 2
    conn.close()
    db.close()


def test_import_uint64_pk(tmp_path):
    db_dir = tmp_path / "import_uint64_pk"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    # type of primary key is UINT64
    conn.execute("CREATE NODE TABLE person(id UINT64, name STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "person.csv"
    with open(csv_path, "w") as f:
        f.write("id|name\n1|Alice\n2|Bob\n")
    conn.execute(f'COPY person FROM "{csv_path}";')
    res = conn.execute("MATCH (n:person) RETURN n;")
    assert len(res) == 2
    conn.close()
    db.close()


# DB-005-05
def test_export_config(tmp_path):
    db_dir = tmp_path / "export_config"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE (u:person {id: 1}), (u2:person {id: 2});")
    out_path = tmp_path / "out.csv"
    # delimiter, header, quotechar, encoding
    conn.execute(
        f'COPY (MATCH (p:person) RETURN *) TO "{out_path}" (DELIMITER=",", HEADER=TRUE)'
    )
    assert out_path.exists()
    conn.close()
    db.close()


# DB-005-07
def test_import_file_not_found(tmp_path):
    db_dir = tmp_path / "import_file_not_found"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    with pytest.raises(Exception) as excinfo:
        conn.execute('COPY person FROM "/not/exist.csv";')
    assert str(ERR_IO_ERROR) in str(excinfo.value)
    conn.close()
    db.close()


# DB-005-08
def test_export_no_permission(tmp_path):
    db_dir = tmp_path / "export_no_permission"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    out_dir = tmp_path / "no_perm"
    out_dir.mkdir()
    os.chmod(out_dir, 0o400)
    out_path = out_dir / "out.csv"
    try:
        with pytest.raises(Exception) as excinfo:
            conn.execute(f'COPY (MATCH (v:person) RETURN v) to "{out_path}";')
            print(str(excinfo.value))
            assert str(ERR_PERMISSION) in str(excinfo.value)
    finally:
        os.chmod(out_dir, 0o700)
    conn.close()
    db.close()


# DB-005-09
def test_import_schema_mismatch(tmp_path):
    db_dir = tmp_path / "import_schema_mismatch"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    csv_path = tmp_path / "mismatch.csv"
    with open(csv_path, "w") as f:
        f.write("id|name\n1|Alice\n")
    with pytest.raises(Exception) as excinfo:
        conn.execute(f'COPY person FROM "{csv_path}";')
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    conn.close()
    db.close()


# DB-005-10
def test_import_bad_encoding(tmp_path):
    db_dir = tmp_path / "import_bad_encoding"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    csv_path = tmp_path / "badenc.csv"
    with open(csv_path, "wb") as f:
        f.write(b"id\n1\n\xff\n")
    with pytest.raises(Exception) as excinfo:
        conn.execute(f'COPY person FROM "{csv_path}";')
    assert str(ERR_IO_ERROR) in str(excinfo.value)
    conn.close()
    db.close()


# DB-005-11
def test_export_vertex_edge(tmp_path):
    db_dir = tmp_path / "syntax_error"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        conn.execute("COPY (MATCH (v:person) RETURN v) to 'person.csv';")
    assert str(ERR_QUERY_SYNTAX) in str(excinfo.value)
    with pytest.raises(Exception) as excinfo:
        conn.execute(
            "COPY (MATCH (:person)-[e:knows]->(:person) RETURN e) to 'person_knows_person.csv' (HEADER = true);"
        )
    assert str(ERR_QUERY_SYNTAX) in str(excinfo.value)
    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Module 5: Copy From With No Schema (auto-detect / schema-less COPY FROM)
# ---------------------------------------------------------------------------


def test_copy_from_no_schema_node_basic(tmp_path):
    """COPY <new_label> FROM 'file.csv' — node table does not exist yet.
    The engine should sniff the CSV, auto-create the node table, and load data.
    """
    db_dir = tmp_path / "no_schema_node_basic"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "people.csv"
    with open(csv_path, "w") as f:
        f.write("id|name\n1|Alice\n2|Bob\n3|Charlie\n")

    conn.execute(f'COPY ns_person FROM "{csv_path}";')

    res = conn.execute("MATCH (n:ns_person) RETURN n.id, n.name ORDER BY n.id;")
    rows = list(res)
    assert len(rows) == 3
    assert rows[0][0] == 1
    assert rows[0][1] == "Alice"
    assert rows[2][0] == 3
    assert rows[2][1] == "Charlie"

    conn.close()
    db.close()


def test_copy_from_no_schema_node_subquery(tmp_path):
    """COPY <new_label> FROM (LOAD FROM 'file.csv' RETURN col1, col2, ...)
    Uses a LOAD FROM sub-query to reorder / select columns before auto-creating
    the node table.
    """
    db_dir = tmp_path / "no_schema_node_subquery"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "users.csv"
    with open(csv_path, "w") as f:
        f.write("name|user_id\nAlice|1\nBob|2\nCharlie|3\n")

    conn.execute(f'COPY ns_user FROM (LOAD FROM "{csv_path}" RETURN user_id, name);')

    res = conn.execute("MATCH (u:ns_user) RETURN u.user_id, u.name ORDER BY u.user_id;")
    rows = list(res)
    assert len(rows) == 3
    assert rows[0][0] == 1
    assert rows[0][1] == "Alice"

    conn.close()
    db.close()


def test_copy_from_no_schema_edge_from_file(tmp_path):
    """COPY <new_edge> FROM 'file.csv' (from='src_label', to='dst_label')
    Vertex tables must already exist; edge table is auto-created.
    """
    db_dir = tmp_path / "no_schema_edge_file"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    person_csv = tmp_path / "person.csv"
    with open(person_csv, "w") as f:
        f.write("id|name\n1|Alice\n2|Bob\n3|Charlie\n")
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute(f'COPY person FROM "{person_csv}";')

    edge_csv = tmp_path / "knows.csv"
    with open(edge_csv, "w") as f:
        f.write("from|to|weight\n1|2|0.5\n2|3|0.8\n")

    conn.execute(f'COPY ns_knows FROM "{edge_csv}" (from="person", to="person");')

    res = conn.execute(
        "MATCH (a:person)-[k:ns_knows]->(b:person) "
        "RETURN a.id, b.id, k.weight ORDER BY a.id;"
    )
    rows = list(res)
    assert len(rows) == 2
    assert rows[0][0] == 1
    assert rows[0][1] == 2

    conn.close()
    db.close()


def test_copy_from_no_schema_edge_subquery(tmp_path):
    """COPY <new_edge> FROM (LOAD FROM 'file.csv' RETURN ...)
    (from='src_label', to='dst_label')
    Edge table auto-created via sub-query.
    """
    db_dir = tmp_path / "no_schema_edge_subquery"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    person_csv = tmp_path / "person.csv"
    with open(person_csv, "w") as f:
        f.write("id|name\n1|Alice\n2|Bob\n3|Charlie\n")
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute(f'COPY person FROM "{person_csv}";')

    edge_csv = tmp_path / "follows.csv"
    with open(edge_csv, "w") as f:
        # Physical column order differs from logical src/dst/since — RETURN reorders.
        f.write("since|dst|src\n2020|2|1\n2021|3|2\n")

    conn.execute(
        f'COPY ns_follows FROM (LOAD FROM "{edge_csv}" RETURN src, dst, since) '
        f'(from="person", to="person");'
    )

    res = conn.execute(
        "MATCH (a:person)-[f:ns_follows]->(b:person) "
        "RETURN a.id, b.id, f.since ORDER BY a.id;"
    )
    rows = list(res)
    assert len(rows) == 2
    assert rows[0][0] == 1
    assert rows[0][1] == 2

    conn.close()
    db.close()


def test_copy_from_no_schema_vertex_then_edge_no_ddl(tmp_path):
    """No-schema vertex COPY, then no-schema edge COPY; endpoints from first COPY."""
    db_dir = tmp_path / "no_schema_v_then_e"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    v_csv = tmp_path / "ns_vertices.csv"
    v_csv.write_text("id|name\n1|Alice\n2|Bob\n3|Charlie\n", encoding="utf-8")
    conn.execute(f'COPY ns5_person FROM "{v_csv}";')

    e_csv = tmp_path / "ns_edges.csv"
    e_csv.write_text("from|to|weight\n1|2|0.5\n2|3|0.8\n", encoding="utf-8")
    conn.execute(f'COPY ns5_knows FROM "{e_csv}" (from="ns5_person", to="ns5_person");')

    vrows = list(conn.execute("MATCH (n:ns5_person) RETURN n.id ORDER BY n.id;"))
    assert len(vrows) == 3
    erows = list(
        conn.execute(
            "MATCH (a:ns5_person)-[k:ns5_knows]->(b:ns5_person) "
            "RETURN a.id, b.id, k.weight ORDER BY a.id;"
        )
    )
    assert len(erows) == 2
    assert erows[0][0] == 1 and erows[0][1] == 2
    assert abs(erows[0][2] - 0.5) < 0.01

    conn.close()
    db.close()


def test_copy_from_no_schema_header_false_f_column_names(tmp_path):
    """HEADER FALSE: inferred column names f0, f1, … on vertex and edge props."""
    db_dir = tmp_path / "no_schema_f_cols"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    v_csv = tmp_path / "vnh.csv"
    v_csv.write_text("1|Alice\n2|Bob\n3|Charlie\n", encoding="utf-8")
    conn.execute(f'COPY ns_fvert FROM "{v_csv}" (HEADER FALSE, DELIMITER="|");')

    vrows = list(conn.execute("MATCH (n:ns_fvert) RETURN n.f0, n.f1 ORDER BY n.f0;"))
    assert vrows == [[1, "Alice"], [2, "Bob"], [3, "Charlie"]]

    e_csv = tmp_path / "enh.csv"
    e_csv.write_text("1|2|0.5\n2|3|0.8\n", encoding="utf-8")
    conn.execute(
        f'COPY ns_fedge FROM "{e_csv}" '
        f'(from="ns_fvert", to="ns_fvert", HEADER FALSE, DELIMITER="|");'
    )

    erows = list(
        conn.execute(
            "MATCH (a:ns_fvert)-[e:ns_fedge]->(b:ns_fvert) "
            "RETURN a.f0, b.f0, e.f2 ORDER BY a.f0;"
        )
    )
    assert len(erows) == 2
    assert erows[0][0] == 1 and erows[0][1] == 2
    assert abs(erows[0][2] - 0.5) < 0.01

    conn.close()
    db.close()


def test_copy_from_no_schema_multiple_types(tmp_path):
    """Verify type sniffing for INT64, STRING, and DOUBLE columns."""
    db_dir = tmp_path / "no_schema_multi_types"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "products.csv"
    with open(csv_path, "w") as f:
        f.write("product_id|product_name|price\n")
        f.write("1|Widget|9.99\n")
        f.write("2|Gadget|19.50\n")
        f.write("3|Doohickey|4.25\n")

    conn.execute(f'COPY ns_product FROM "{csv_path}";')

    res = conn.execute(
        "MATCH (p:ns_product) RETURN p.product_id, p.product_name, p.price "
        "ORDER BY p.product_id;"
    )
    rows = list(res)
    assert len(rows) == 3
    assert rows[0][1] == "Widget"
    assert isinstance(rows[0][2], float)
    assert abs(rows[0][2] - 9.99) < 0.01

    conn.close()
    db.close()


def test_copy_from_no_schema_pk_is_first_column(tmp_path):
    """The first column of the CSV should become the primary key.
    Inserting a duplicate first-column value should fail or be deduplicated.
    """
    db_dir = tmp_path / "no_schema_pk_first_col"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "items.csv"
    with open(csv_path, "w") as f:
        f.write("item_id|description\n10|Pen\n20|Pencil\n30|Eraser\n")

    conn.execute(f'COPY ns_item FROM "{csv_path}";')

    res = conn.execute("MATCH (i:ns_item) RETURN i.item_id ORDER BY i.item_id;")
    rows = list(res)
    assert len(rows) == 3
    assert rows[0][0] == 10
    assert rows[1][0] == 20
    assert rows[2][0] == 30

    conn.close()
    db.close()


def test_copy_from_no_schema_node_wide_row(tmp_path):
    """No-schema COPY: same column layout as comprehensive_graph node_a (all types)."""
    src = _neug_repo_root() / "example_dataset/comprehensive_graph/node_a.csv"
    if not src.is_file():
        pytest.skip(f"Missing comprehensive_graph dataset: {src}")
    lines = src.read_text(encoding="utf-8").splitlines()
    if len(lines) < 4:
        pytest.skip("node_a.csv too short")

    db_dir = tmp_path / "no_schema_wide"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "comp_node_slice.csv"
    # Header + id=1 + id=2 rows (skip id=0 to avoid large u64 sniff edge cases).
    csv_path.write_text(
        "\n".join([lines[0], lines[2], lines[3]]) + "\n", encoding="utf-8"
    )

    conn.execute(f'COPY ns_comp FROM "{csv_path}";')

    assert len(list(conn.execute("MATCH (n:ns_comp) RETURN n.id ORDER BY n.id;"))) == 2

    r1 = list(
        conn.execute(
            "MATCH (n:ns_comp) WHERE n.id = 1 RETURN n.i32_property, n.i64_property, "
            "n.f32_property, n.f64_property, n.str_property, n.interval_property;"
        )
    )[0]
    assert r1[0] == 987654321
    assert r1[1] == -9223372036854775808
    assert abs(r1[2] - (-1.4142135)) < 1e-5
    assert abs(r1[3] - 1.4142135623730951) < 1e-12
    assert r1[4] == "test_string_1"
    assert r1[5] == "3years"

    dt_row = list(
        conn.execute("MATCH (n:ns_comp) WHERE n.id = 1 RETURN n.datetime_property;")
    )[0]
    assert isinstance(dt_row[0], datetime)
    assert dt_row[0].year == 2023 and dt_row[0].month == 6 and dt_row[0].day == 22

    date_row = list(
        conn.execute("MATCH (n:ns_comp) WHERE n.id = 1 RETURN n.date_property;")
    )[0]
    assert "2023-06-22" in str(date_row[0])

    u32_u64 = list(
        conn.execute(
            "MATCH (n:ns_comp) WHERE n.id = 1 RETURN n.u32_property, n.u64_property;"
        )
    )[0]
    assert u32_u64[0] == 0
    assert u32_u64[1] == 0.0

    r2 = list(conn.execute("MATCH (n:ns_comp) WHERE n.id = 2 RETURN n.str_property;"))[
        0
    ]
    assert r2[0] == "test_string_2"

    conn.close()
    db.close()


def test_copy_from_no_schema_node_string_pk(tmp_path):
    """No-schema COPY: first column is STRING primary key."""
    db_dir = tmp_path / "no_schema_str_pk"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "skus.csv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("sku|qty\n")
        f.write("ABC-001|10\n")
        f.write("XYZ-9|20\n")

    conn.execute(f'COPY ns_sku FROM "{csv_path}";')

    res = conn.execute("MATCH (s:ns_sku) RETURN s.sku, s.qty ORDER BY s.sku;")
    rows = list(res)
    assert len(rows) == 2
    assert rows[0][0] == "ABC-001"
    assert rows[0][1] == 10
    assert rows[1][0] == "XYZ-9"

    conn.close()
    db.close()


def test_copy_from_no_schema_node_duplicate_pk_first_row_wins(tmp_path):
    """Duplicate PK in CSV: engine keeps one row (first wins for name)."""
    db_dir = tmp_path / "no_schema_dup_pk"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "dup.csv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("id|name\n1|First\n1|Second\n")

    conn.execute(f'COPY ns_dup_pk FROM "{csv_path}";')

    res = conn.execute("MATCH (n:ns_dup_pk) RETURN n.id, n.name;")
    rows = list(res)
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert rows[0][1] == "First"

    conn.close()
    db.close()


def test_copy_from_no_schema_node_reopen_database(tmp_path):
    """Persistence: COPY creates label, close DB, reopen read-only, MATCH."""
    db_dir = tmp_path / "no_schema_reopen"
    db_dir.mkdir()
    csv_path = tmp_path / "persist.csv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("id|name\n7|Reopen\n")

    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(f'COPY ns_persist FROM "{csv_path}";')
    conn.close()
    db.close()

    db2 = Database(db_path=str(db_dir), mode="r")
    conn2 = db2.connect()
    rows = list(
        conn2.execute("MATCH (n:ns_persist) RETURN n.id, n.name ORDER BY n.id;")
    )
    assert len(rows) == 1
    assert rows[0][0] == 7
    assert rows[0][1] == "Reopen"
    conn2.close()
    db2.close()


def test_copy_from_no_schema_node_second_copy_appends(tmp_path):
    """Second COPY into an existing no-schema label appends rows."""
    db_dir = tmp_path / "no_schema_second_copy"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    a = tmp_path / "batch_a.csv"
    b = tmp_path / "batch_b.csv"
    a.write_text("id|v\n1|a\n", encoding="utf-8")
    b.write_text("id|v\n2|b\n", encoding="utf-8")

    conn.execute(f'COPY ns_twice FROM "{a}";')
    conn.execute(f'COPY ns_twice FROM "{b}";')

    rows = list(conn.execute("MATCH (n:ns_twice) RETURN n.id, n.v ORDER BY n.id;"))
    assert len(rows) == 2
    assert rows[0] == [1, "a"]
    assert rows[1] == [2, "b"]

    conn.close()
    db.close()


def test_copy_from_no_schema_node_subquery_where_filter(tmp_path):
    """LOAD FROM subquery with WHERE filters rows before DDL + load."""
    db_dir = tmp_path / "no_schema_where"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "filtered.csv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("id|name|tag\n1|A|x\n2|B|y\n3|C|z\n")

    conn.execute(
        f'COPY ns_filt FROM (LOAD FROM "{csv_path}" WHERE id > 1 RETURN id, name);'
    )

    rows = list(conn.execute("MATCH (n:ns_filt) RETURN n.id ORDER BY n.id;"))
    assert len(rows) == 2
    assert [r[0] for r in rows] == [2, 3]

    conn.close()
    db.close()


def test_copy_from_no_schema_node_header_only_csv(tmp_path):
    """CSV with header only: auto-create table, zero vertices."""
    db_dir = tmp_path / "no_schema_header_only"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "empty_body.csv"
    csv_path.write_text("id|name\n", encoding="utf-8")

    conn.execute(f'COPY ns_empty FROM "{csv_path}";')

    rows = list(conn.execute("MATCH (n:ns_empty) RETURN n.id;"))
    assert len(rows) == 0

    conn.close()
    db.close()


def test_copy_from_no_schema_node_unicode_string(tmp_path):
    """UTF-8 non-ASCII in STRING column round-trip."""
    db_dir = tmp_path / "no_schema_unicode"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    csv_path = tmp_path / "uni.csv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("id|label\n")
        f.write("1|你好\n")
        f.write("2|English\n")

    conn.execute(f'COPY ns_uni FROM "{csv_path}";')

    rows = list(conn.execute("MATCH (u:ns_uni) RETURN u.id, u.label ORDER BY u.id;"))
    assert len(rows) == 2
    assert rows[0][1] == "你好"
    assert rows[1][1] == "English"

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Extension Tests: COPY FROM JSON / JSONL / Parquet
# ---------------------------------------------------------------------------


def _tinysnb_path() -> Path:
    """Return <repo>/example_dataset/tinysnb."""
    return _neug_repo_root() / "example_dataset" / "tinysnb"


@extension_test
def test_copy_from_json_node_no_schema(tmp_path):
    """COPY FROM JSON into a new label — auto-detect schema."""
    db_dir = tmp_path / "copy_json_no_schema"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("LOAD JSON")

    json_path = tmp_path / "employees.json"
    data = [
        {"emp_id": 100, "name": "Alice", "salary": 55000.0},
        {"emp_id": 200, "name": "Bob", "salary": 62000.5},
    ]
    with open(json_path, "w") as f:
        json.dump(data, f)

    conn.execute(f'COPY ns_employee FROM "{json_path}";')

    res = conn.execute(
        "MATCH (e:ns_employee) RETURN e.emp_id, e.name, e.salary ORDER BY e.emp_id;"
    )
    rows = list(res)
    assert len(rows) == 2
    assert rows[0][0] == 100
    assert rows[0][1] == "Alice"
    assert abs(rows[0][2] - 55000.0) < 0.01

    conn.close()
    db.close()


@extension_test
def test_copy_from_jsonl_node_no_schema(tmp_path):
    """COPY FROM JSONL into a new label — auto-detect schema."""
    db_dir = tmp_path / "copy_jsonl_no_schema"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("LOAD JSON")

    jsonl_path = tmp_path / "items.jsonl"
    records = [
        {"item_id": 1, "description": "Pen", "price": 1.5},
        {"item_id": 2, "description": "Pencil", "price": 0.75},
        {"item_id": 3, "description": "Eraser", "price": 0.50},
    ]
    with open(jsonl_path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    conn.execute(f'COPY ns_item FROM "{jsonl_path}";')

    res = conn.execute(
        "MATCH (i:ns_item) RETURN i.item_id, i.description, i.price ORDER BY i.item_id;"
    )
    rows = list(res)
    assert len(rows) == 3
    assert rows[0] == [1, "Pen", 1.5]

    conn.close()
    db.close()


@extension_test
def test_copy_from_json_with_subquery(tmp_path):
    """COPY FROM (LOAD FROM 'file.json' RETURN ...) — column reorder via subquery."""
    db_dir = tmp_path / "copy_json_subquery"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("LOAD JSON")

    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )

    json_path = tmp_path / "unordered.json"
    data = [
        {"name": "Alice", "age": 30, "user_id": 1},
        {"name": "Bob", "age": 25, "user_id": 2},
    ]
    with open(json_path, "w") as f:
        json.dump(data, f)

    conn.execute(
        f'COPY person FROM (LOAD FROM "{json_path}" RETURN user_id AS id, name, age);'
    )

    res = conn.execute("MATCH (n:person) RETURN n.id, n.name, n.age ORDER BY n.id;")
    rows = list(res)
    assert len(rows) == 2
    assert rows[0] == [1, "Alice", 30]
    assert rows[1] == [2, "Bob", 25]

    conn.close()
    db.close()


@extension_test
def test_copy_from_parquet_node_no_schema(tmp_path):
    """COPY FROM Parquet into a new label — auto-detect schema."""
    parquet_path = _tinysnb_path() / "parquet" / "vPerson.parquet"
    if not parquet_path.is_file():
        pytest.skip(f"Parquet file not found: {parquet_path}")

    db_dir = tmp_path / "copy_parquet_no_schema"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("LOAD PARQUET")

    conn.execute(f'COPY ns_person FROM "{parquet_path}";')

    res = conn.execute("MATCH (n:ns_person) RETURN n.ID, n.fName, n.age ORDER BY n.ID;")
    rows = list(res)
    assert len(rows) == 8
    assert rows[0][1] == "Alice"
    assert rows[0][2] == 35
    assert rows[-1][1] == "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff"

    conn.close()
    db.close()


@extension_test
def test_copy_from_parquet_edge_no_schema(tmp_path):
    """COPY FROM Parquet edge file with auto-detect, endpoints created first."""
    person_parquet = _tinysnb_path() / "parquet" / "vPerson.parquet"
    meets_parquet = _tinysnb_path() / "parquet" / "eMeets.parquet"
    if not person_parquet.is_file() or not meets_parquet.is_file():
        pytest.skip("Parquet test data not found")

    db_dir = tmp_path / "copy_parquet_edge"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("LOAD PARQUET")

    # Import person nodes first (auto-detect)
    conn.execute(f'COPY ns_person FROM "{person_parquet}";')

    # Import meets edges (auto-detect, need from/to)
    conn.execute(
        f'COPY ns_meets FROM "{meets_parquet}" (from="ns_person", to="ns_person");'
    )

    res = conn.execute(
        "MATCH (a:ns_person)-[m:ns_meets]->(b:ns_person) "
        "RETURN a.fName, b.fName ORDER BY a.fName;"
    )
    rows = list(res)
    assert len(rows) == 7

    conn.close()
    db.close()
