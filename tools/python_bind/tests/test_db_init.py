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
from errors import ERR_CONFIG_INVALID
from errors import ERR_CORRUPTION_DETECTED
from errors import ERR_DATABASE_LOCKED
from errors import ERR_DIRECTORY_NOT_EXIST
from errors import ERR_DISK_SPACE_EXHAUSTED
from errors import ERR_INVALID_ARGUMENT
from errors import ERR_INVALID_PATH
from errors import ERR_PERMISSION
from errors import ERR_VERSION_MISMATCHED
from errors import ERROR_STRINGS

from neug.database import Database


# DB-001-01 & DB-001-02
@pytest.mark.skip(reason="memory mode is not supported in the current version of neug")
def test_memory_mode_open_and_close():
    db = Database(db_path="", mode="r", planner="gopt")
    assert db is not None
    db.close()
    db2 = Database(db_path="", mode="w", planner="gopt")
    assert db2 is not None
    db2.close()
    db3 = Database(db_path=None, mode="r", planner="gopt")
    assert db3 is not None
    db3.close()
    db4 = Database(db_path=None, mode="w", planner="gopt")
    assert db4 is not None
    db4.close()


# DB-001-03
def test_local_db_open_exists_and_close(tmp_path):
    db_dir = tmp_path / "existdb"
    if not db_dir.exists():
        db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="r", planner="gopt")
    assert db is not None
    db.close()
    db2 = Database(db_path=str(db_dir), mode="rw", planner="gopt")
    assert db2 is not None
    db2.close()


# DB-001-04
def test_local_db_open_not_exists_and_close(tmp_path):
    db_dir = tmp_path / "not_existdb"
    if db_dir.exists():
        os.system("rm -rf %s" % db_dir)
    assert not db_dir.exists()
    db = Database(db_path=str(db_dir), mode="r", planner="gopt")
    assert db is not None
    db.close()
    if not db_dir.exists():
        db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    assert db is not None
    db.close()


# DB-001-05
def test_local_db_close(tmp_path):
    db_dir = tmp_path / "closedb"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    db.close()


# DB-001-06
def test_readonly_mode_multi_instance(tmp_path):
    db_dir = tmp_path / "multi_db"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    db.close()
    db1 = Database(db_path=str(db_dir), mode="r", planner="gopt")
    db2 = Database(db_path=str(db_dir), mode="r", planner="gopt")
    assert db1 is not None and db2 is not None
    db1.close()
    db2.close()


# DB-001-07
def test_rw_mode_exclusive(tmp_path):
    db_dir = tmp_path / "exclusive_db"
    db1 = Database(db_path=str(db_dir), mode="w", planner="gopt")
    try:
        with pytest.raises(Exception) as excinfo:
            Database(db_path=str(db_dir), mode="w", planner="gopt")
        assert ERROR_STRINGS[ERR_DATABASE_LOCKED] in str(excinfo.value)
    finally:
        db1.close()


# DB-001-08
def test_rw_ro_conflict(tmp_path):
    db_dir = tmp_path / "conflict_db"
    db1 = Database(db_path=str(db_dir), mode="w", planner="gopt")
    try:
        with pytest.raises(Exception) as excinfo:
            Database(db_path=str(db_dir), mode="r", planner="gopt")
        assert ERROR_STRINGS[ERR_DATABASE_LOCKED] in str(excinfo.value)
    finally:
        db1.close()


# DB-001-09
def test_readonly_write_operation(tmp_path):
    db_dir = tmp_path / "readonly_db"
    db_ro = Database(db_path=str(db_dir), mode="r", planner="gopt")
    with pytest.raises(Exception) as excinfo:
        conn = db_ro.connect()
        conn.execute("CREATE NODE TABLE person(id INT32, PRIMARY KEY(id));")
        conn.close()
    db_ro.close()
    # TODO: error code should be ERR_PERMISSION??
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)


# DB-001-10
def test_invalid_path():
    with pytest.raises(Exception) as excinfo:
        Database(db_path="??/illegal", mode="r", planner="gopt")
    assert ERROR_STRINGS[ERR_INVALID_PATH] in str(excinfo.value)
    # remove the invalid path after the test
    if os.path.exists("??/illegal"):
        os.system("rm -rf ??/")


# DB-001-11
def test_config_param_exception(tmp_path):
    db_dir = tmp_path / "config_db"
    db_dir.mkdir()
    # Use the number of CPU cores as max_thread_num
    max_thread_num = os.cpu_count() or 1
    db = Database(
        db_path=str(db_dir), mode="rw", max_thread_num=max_thread_num, planner="gopt"
    )
    assert db is not None
    db.close()
    with pytest.raises(Exception) as excinfo:
        Database(db_path=str(db_dir), mode="rw", max_thread_num=-1, planner="gopt")
    assert ERROR_STRINGS[ERR_CONFIG_INVALID] in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        Database(db_path="/tmp/test", mode="red", planner="gopt")
        assert "Invalid mode: red" in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        Database(db_path="/tmp/test", mode="write", planner="gopt123")
        assert "Invalid planner: gopt123" in str(excinfo.value)


# DB-001-12
def test_open_no_permission(tmp_path):
    db_dir = tmp_path / "no_permission_db"
    if db_dir.exists():
        os.system("rm -rf %s" % db_dir)
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    db.close()
    os.chmod(db_dir, 0o400)
    try:
        with pytest.raises(Exception) as excinfo:
            Database(db_path=str(db_dir), mode="w", planner="gopt")
        assert ERROR_STRINGS[ERR_PERMISSION] in str(excinfo.value)
    finally:
        os.chmod(db_dir, 0o700)


# DB-001-13
@pytest.mark.skip(reason="To mock the version mismatch correctly")
def test_open_version_mismatch(tmp_path):
    db_dir = tmp_path / "ver_db"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    db.close()

    # Simulate version mismatch by modifying the version metadata file
    # Assuming version metadata is stored in version.txt
    version_file = db_dir / "version.txt"
    with open(version_file, "w") as f:
        f.write("mismatched_version")

    # Attempt to open the database
    with pytest.raises(Exception) as excinfo:
        Database(db_path=str(db_dir), mode="r", planner="gopt")
    assert ERROR_STRINGS[ERR_VERSION_MISMATCHED] in str(excinfo.value)


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
            Database(db_path=str(db_dir), mode="w", planner="gopt")
        assert "Permission denied" in str(excinfo.value)
    finally:
        os.chmod(db_dir, 0o700)


# DB-001-15
@pytest.mark.skip(reason="TODO: mock disk space exhausted correctly")
def test_disk_space_exhausted(monkeypatch, tmp_path):
    db_dir = tmp_path / "no_space_db"
    db_dir.mkdir()

    def mock_open(*args, **kwargs):
        # Simulate a disk space error
        raise OSError("No space left on device")

    # Mock open function to raise a disk space error
    monkeypatch.setattr(os, "open", mock_open)
    with pytest.raises(Exception) as excinfo:
        Database(db_path=str(db_dir), mode="w", planner="gopt")
    assert ERROR_STRINGS[ERR_DISK_SPACE_EXHAUSTED] in str(excinfo.value)


# DB-001-16
@pytest.mark.skip(reason="Core dump. No file corruption check implemented yet")
def test_file_header_corruption(tmp_path):
    db_dir = tmp_path / "corrupt_db"
    db_dir.mkdir()
    Database(db_path=str(db_dir), mode="w", planner="gopt")
    # db_file such as "wal/thread_0_0.wal" should exist after db creation
    db_file = db_dir / "wal/thread_0_0.wal"
    assert db_file.exists(), "Database file should exist after creation"
    # simulate file corruption by writing a corrupt header
    with open(db_file, "wb") as f:
        f.write(b"corrupt-header")
    try:
        Database(db_path=str(db_dir), mode="w", planner="gopt")
    except Exception as exc:
        assert ERROR_STRINGS[ERR_CORRUPTION_DETECTED] in str(exc)
    else:
        pytest.fail("Expected ERR_CORRUPTION_DETECTED but no exception was raised")
