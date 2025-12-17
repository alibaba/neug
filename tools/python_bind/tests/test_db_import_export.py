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
