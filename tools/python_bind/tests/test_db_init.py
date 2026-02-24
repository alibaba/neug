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
from neug.proto.error_pb2 import ERR_CONFIG_INVALID
from neug.proto.error_pb2 import ERR_CORRUPTION_DETECTED
from neug.proto.error_pb2 import ERR_DATABASE_LOCKED
from neug.proto.error_pb2 import ERR_DISK_SPACE_EXHAUSTED
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_INVALID_PATH
from neug.proto.error_pb2 import ERR_PERMISSION
from neug.proto.error_pb2 import ERR_VERSION_MISMATCHED


# DB-001-01 & DB-001-02
def test_memory_mode_open_and_close():
    db = Database(db_path="", mode="r")
    assert db is not None
    db.close()
    db2 = Database(db_path="", mode="w")
    assert db2 is not None
    db2.close()


def test_memory_mode_open_and_close_none():
    with pytest.raises(Exception) as excinfo:
        # In memory database should not be read-only
        Database(db_path=None, mode="r")
        assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    db2 = Database(db_path=None, mode="w")
    assert db2 is not None
    db2.close()


# DB-001-03
def test_local_db_open_exists_and_close(tmp_path):
    db_dir = tmp_path / "existdb"
    if not db_dir.exists():
        db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="r")
    assert db is not None
    db.close()
    db2 = Database(db_path=str(db_dir), mode="rw")
    assert db2 is not None
    db2.close()


def test_local_ldbc_open_and_close():
    db_dir = "/tmp/ldbc"
    db = Database(db_path=str(db_dir), mode="r")
    assert db is not None
    db.close()
    db2 = Database(db_path=str(db_dir), mode="rw")
    assert db2 is not None
    db2.close()


# DB-001-04
def test_local_db_open_not_exists_and_close(tmp_path):
    db_dir = tmp_path / "not_existdb"
    if db_dir.exists():
        os.system("rm -rf %s" % db_dir)
    assert not db_dir.exists()
    db = Database(db_path=str(db_dir), mode="r")
    assert db is not None
    db.close()
    if not db_dir.exists():
        db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    assert db is not None
    db.close()


# DB-001-05
def test_local_db_close(tmp_path):
    db_dir = tmp_path / "closedb"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    db.close()


# DB-001-06
def test_readonly_mode_multi_instance(tmp_path):
    db_dir = tmp_path / "multi_db"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    db.close()
    db1 = Database(db_path=str(db_dir), mode="r")
    db2 = Database(db_path=str(db_dir), mode="r")
    assert db1 is not None and db2 is not None
    db1.close()
    db2.close()


# DB-001-07
def test_rw_mode_exclusive(tmp_path):
    db_dir = tmp_path / "exclusive_db"
    db1 = Database(db_path=str(db_dir), mode="w")
    try:
        with pytest.raises(Exception) as excinfo:
            Database(db_path=str(db_dir), mode="w")
        assert str(ERR_DATABASE_LOCKED) in str(excinfo.value)
    finally:
        db1.close()


# DB-001-08
def test_rw_ro_conflict(tmp_path):
    db_dir = tmp_path / "conflict_db"
    db1 = Database(db_path=str(db_dir), mode="w")
    try:
        with pytest.raises(Exception) as excinfo:
            Database(db_path=str(db_dir), mode="r")
        assert str(ERR_DATABASE_LOCKED) in str(excinfo.value)
    finally:
        db1.close()


# DB-001-09
def test_readonly_write_operation(tmp_path):
    db_dir = tmp_path / "readonly_db"
    db_ro = Database(db_path=str(db_dir), mode="r")
    with pytest.raises(Exception) as excinfo:
        conn = db_ro.connect()
        conn.execute("CREATE NODE TABLE person(id INT32, PRIMARY KEY(id));")
        conn.close()
    db_ro.close()
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)


# DB-001-10
def test_invalid_path():
    with pytest.raises(Exception) as excinfo:
        Database(db_path="??/illegal", mode="r")
    assert str(ERR_INVALID_PATH) in str(excinfo.value)
    # remove the invalid path after the test
    if os.path.exists("??/illegal"):
        os.system("rm -rf ??/")


# DB-001-11
def test_config_param(tmp_path):
    db_dir = tmp_path / "config_db"
    # mode: 'r', 'read', 'readwrite', 'w', 'rw', 'write'
    db1 = Database(db_path=str(db_dir), mode="r", max_thread_num=0)
    assert db1 is not None
    db1.close()
    db2 = Database(db_path=str(db_dir), mode="read", max_thread_num=0)
    assert db2 is not None
    db2.close()
    db3 = Database(db_path=str(db_dir), mode="readwrite", max_thread_num=0)
    assert db3 is not None
    db3.close()
    db4 = Database(db_path=str(db_dir), mode="w", max_thread_num=0)
    assert db4 is not None
    db4.close()
    db5 = Database(db_path=str(db_dir), mode="rw", max_thread_num=0)
    assert db5 is not None
    db5.close()
    db6 = Database(db_path=str(db_dir), mode="write", max_thread_num=0)
    assert db6 is not None
    db6.close()
    # max_thread_num: 0 means no limit
    db7 = Database(db_path=str(db_dir), mode="r", max_thread_num=0)
    assert db7 is not None
    db7.close()
    max_thread_num = os.cpu_count() or 1
    db8 = Database(db_path=str(db_dir), mode="r", max_thread_num=max_thread_num)
    assert db8 is not None
    db8.close()
    db9 = Database(db_path=str(db_dir), mode="r", max_thread_num=0)
    assert db9 is not None
    db9.close()


def test_config_param_exception(tmp_path):
    db_dir = tmp_path / "config_db_exception"
    with pytest.raises(Exception) as excinfo:
        Database(db_path=str(db_dir), mode="rw", max_thread_num=-1)
    assert str(ERR_CONFIG_INVALID) in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        Database(db_path=str(db_dir), mode="red")
        assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    with pytest.raises(TypeError) as excinfo:
        Database(db_path=str(db_dir), mode="write", planner="gopt123")
        assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)


def test_config_param_boundary(tmp_path):
    db_dir = tmp_path / "conn_param_boundary_db"
    # test with more than maximum cores
    with pytest.raises(Exception) as excinfo:
        max_cores = os.cpu_count() or 1
        # max_thread_num should not exceed the number of cores
        Database(str(db_dir), "w", max_thread_num=max_cores + 1)
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)


# DB-001-12
def test_open_no_permission(tmp_path):
    db_dir = tmp_path / "no_permission_db"
    if db_dir.exists():
        os.system("rm -rf %s" % db_dir)
    db = Database(db_path=str(db_dir), mode="w")
    db.close()
    os.chmod(db_dir, 0o400)
    try:
        with pytest.raises(Exception) as excinfo:
            Database(db_path=str(db_dir), mode="w")
        assert str(ERR_PERMISSION) in str(excinfo.value)
    finally:
        os.chmod(db_dir, 0o700)


# DB-001-13
@pytest.mark.skip(reason="https://github.com/GraphScope/neug/issues/788")
def test_open_version_mismatch(tmp_path):
    db_dir = tmp_path / "ver_db"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    db.close()

    # Simulate version mismatch by modifying the version metadata file
    # Assuming version metadata is stored in version.txt
    version_file = db_dir / "version.txt"
    with open(version_file, "w") as f:
        f.write("mismatched_version")

    # Attempt to open the database
    with pytest.raises(Exception) as excinfo:
        Database(db_path=str(db_dir), mode="r")
    assert str(ERR_VERSION_MISMATCHED) in str(excinfo.value)


# DB-001-14
def test_open_dir_not_exist(tmp_path):
    db_dir = tmp_path / "not_exist_dir"
    if db_dir.exists():
        os.system("rm -rf %s" % db_dir)
    # mock the os.chmod to simulate no permission
    os.makedirs(db_dir, exist_ok=True)
    os.chmod(db_dir, 0o400)
    try:
        with pytest.raises(Exception) as excinfo:
            Database(db_path=str(db_dir), mode="w")
        assert str(ERR_PERMISSION) in str(excinfo.value)
    finally:
        os.chmod(db_dir, 0o700)


# DB-001-15
@pytest.mark.skip(reason="planned in stress test issues #524")
def test_disk_space_exhausted(monkeypatch, tmp_path):
    db_dir = tmp_path / "no_space_db"
    db_dir.mkdir()

    def mock_open(*args, **kwargs):
        # Simulate a disk space error
        raise OSError("No space left on device")

    # Mock open function to raise a disk space error
    monkeypatch.setattr(os, "open", mock_open)
    with pytest.raises(Exception) as excinfo:
        Database(db_path=str(db_dir), mode="w")
    assert str(ERR_DISK_SPACE_EXHAUSTED) in str(excinfo.value)


# DB-001-16
@pytest.mark.skip(reason="https://github.com/GraphScope/neug/issues/794")
def test_file_header_corruption(tmp_path):
    db_dir = tmp_path / "corrupt_db"
    db_dir.mkdir()
    Database(db_path=str(db_dir), mode="w")
    # db_file such as "wal/thread_0_0.wal" should exist after db creation
    db_file = db_dir / "wal/thread_0_0.wal"
    assert db_file.exists(), "Database file should exist after creation"
    # simulate file corruption by writing a corrupt header
    with open(db_file, "wb") as f:
        f.write(b"corrupt-header")
    try:
        Database(db_path=str(db_dir), mode="w")
    except Exception as exc:
        assert str(ERR_CORRUPTION_DETECTED) in str(exc)
    else:
        pytest.fail("Expected ERR_CORRUPTION_DETECTED but no exception was raised")


# DB-001-17
def test_db_default_mode(tmp_path):
    db_dir = tmp_path / "default_mode_db"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir))
    assert db is not None
    assert db.mode == "read-write"
