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
from errors import ERR_BAD_ENCODING
from errors import ERR_DIRECTORY_NOT_EXIST
from errors import ERR_INVALID_FILE
from errors import ERR_PERMISSION
from errors import ERR_QUERY_SYNTAX
from errors import ERR_SCHEMA_MISMATCH
from errors import ERR_TYPE_CONVERSION
from errors import ERROR_STRINGS

from neug.database import Database


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


# DB-005-02
@pytest.mark.skip(
    reason="IGNORE_ERRORS is not supported yet; and when it is false, no exception is raised"
)
def test_import_ignore_errors(tmp_path):
    db_dir = tmp_path / "import_ignore"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    csv_path = tmp_path / "bad.csv"
    with open(csv_path, "wb") as f:
        f.write(b"id\n1\n\xff\n2\n")
    # IGNORE_ERRORS True
    conn.execute(f'COPY person FROM "{csv_path}" (IGNORE_ERRORS TRUE);')
    res = conn.execute("MATCH (n:person) RETURN count(n);")
    assert res[0][0] == 2  # Two rows should be imported, ignoring the bad encoding row
    # IGNORE_ERRORS False
    with pytest.raises(Exception) as excinfo:
        conn.execute(f'COPY person FROM "{csv_path}" (IGNORE_ERRORS FALSE);')
    assert ERROR_STRINGS[ERR_BAD_ENCODING] in str(excinfo.value)
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
@pytest.mark.skip(
    reason="Inconsistent data type, expect string, but got int64. However, no error handling for type conversion yet"
)
def test_import_type_conversion1(tmp_path):
    db_dir = tmp_path / "import_type_conversion1"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "type.csv"
    with open(csv_path, "w") as f:
        f.write("id|name\n1|111\n2|222\n")
    with pytest.raises(Exception) as excinfo:
        conn.execute(f'COPY person FROM "{csv_path}"')
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(
    reason="Inconsistent data type, expect int32, but got int64. However, no error handling for type conversion yet"
)
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
    with pytest.raises(Exception) as excinfo:
        conn.execute(f'COPY person2 FROM "{csv_path2}";')
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(
    reason="Inconsistent data type, expect int64, but got timestamp[ns]. However, no error handling for type conversion yet"
)
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
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    conn.close()
    db.close()


@pytest.mark.skip(
    reason="BUG:  mutable_property_fragment.h:286] Inconsistent data type, expect large_string, but got string"
)
def test_import_string_pk(tmp_path):
    db_dir = tmp_path / "import_type"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id STRING, PRIMARY KEY(id));")
    csv_path = tmp_path / "type.csv"
    with open(csv_path, "w") as f:
        f.write("id\nAlice\n")
    with pytest.raises(Exception) as excinfo:
        conn.execute(f'COPY person FROM "{csv_path}"')
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)


@pytest.mark.skip(
    reason="BUG: mutable_property_fragment.h:286] Inconsistent data type, expect int32, but got int64"
)
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


@pytest.mark.skip(reason="Unsupported primary key type: uint32")
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


@pytest.mark.skip(reason="Unsupported primary key type: uint64")
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
@pytest.mark.skip(reason="unsupported yet")
def test_export_config(tmp_path):
    db_dir = tmp_path / "export_config"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE (u:person {id: 1}), (u2:person {id: 2});")
    out_path = tmp_path / "out.csv"
    # delimiter, header, quotechar, encoding
    conn.execute(f'COPY person TO "{out_path}" (DELIMITER=",", HEADER TRUE)')
    assert out_path.exists()
    conn.close()
    db.close()


# DB-005-07
# TODO: no error code is returned, need to fix it in the compiler
def test_import_file_not_found(tmp_path):
    db_dir = tmp_path / "import_file_not_found"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE person(id INT64, PRIMARY KEY(id));")
    with pytest.raises(Exception) as excinfo:
        conn.execute('COPY person FROM "/not/exist.csv";')
    assert ERROR_STRINGS[ERR_INVALID_FILE] in str(excinfo.value)
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
        assert ERROR_STRINGS[ERR_PERMISSION] in str(excinfo.value)
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
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.close()
    db.close()


# DB-005-10
@pytest.mark.skip(reason="Not supported yet")
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
        conn.execute(f'COPY person FROM "{csv_path}" (IGNORE_ERRORS FALSE);')
    assert ERROR_STRINGS[ERR_BAD_ENCODING] in str(excinfo.value)
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
