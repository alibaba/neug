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
import time

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from errors import ERR_SCHEMA_MISMATCH
from errors import ERR_TX_STATE_CONFLICT
from errors import ERR_TX_TIMEOUT
from errors import ERR_TYPE_CONVERSION
from errors import ERROR_STRINGS

from neug.database import Database


# DB-004-01
def test_ap_read_concurrent():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=str(db_dir), mode="r", planner="gopt")
    conns = [db.connect() for _ in range(4)]
    for conn in conns:
        result = conn.execute("MATCH (n) RETURN n")
        assert len(result) == 6
    for conn in conns:
        conn.close()
    db.close()


# DB-004-02
def test_ap_write_concurrent(tmp_path):
    db_dir = tmp_path / "ap_write_concurrent"
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        # in rw mode, only one connection is allowed
        db.connect()
    assert ERROR_STRINGS[ERR_TX_STATE_CONFLICT] in str(excinfo.value)
    conn.close()
    db.close()


# DB-004-03
def test_ap_read_write_concurrent():
    db_dir = "/tmp/modern_graph"
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        # in rw mode, only one connection is allowed
        db.connect()
    assert ERROR_STRINGS[ERR_TX_STATE_CONFLICT] in str(excinfo.value)
    conn.close()
    db.close()


# DB-004-04
@pytest.mark.skip(reason="Session not supported yet")
def test_tp_read_concurrent(started_server):
    db, port = started_server
    from neug.session import Session

    s1 = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    s2 = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    r1 = s1.execute("MATCH (n) RETURN count(n);")
    r2 = s2.execute("MATCH (n) RETURN count(n);")
    assert r1 == r2
    s1.close()
    s2.close()


# DB-004-05
@pytest.mark.skip(reason="Session not supported yet")
def test_tp_write_concurrent(started_server):
    db, port = started_server
    from neug.session import Session

    s1 = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    s2 = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    s1.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    s1.execute("CREATE (n:T {id: 1});")
    s2.execute("CREATE (n:T {id: 2});")
    r1 = s1.execute("MATCH (n:T) RETURN count(n);")
    r2 = s2.execute("MATCH (n:T) RETURN count(n);")
    assert r1 == r2
    s1.close()
    s2.close()


# DB-004-06
@pytest.mark.skip(reason="Session not supported yet")
def test_tp_read_write_concurrent(started_server):
    db, port = started_server
    from neug.session import Session

    s1 = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    s2 = Session.open(f"neug://user:pass@127.0.0.1:{port}/")
    r1 = s2.execute("MATCH (n) RETURN count(n);")
    assert r1[0][0] == 0
    s1.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    s1.execute("CREATE (n:T {id: 1});")
    r2 = s2.execute("MATCH (n:T) RETURN count(n);")
    assert r2[0][0] == 1
    s1.close()
    s2.close()


# DB-004-07
@pytest.mark.skip(reason="not supported yet")
def test_auto_transaction_management(tmp_path):
    db_dir = tmp_path / "auto_tx_mgmt"
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn = db.connect()
    # create success, commit automatically
    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    conn.execute("CREATE (n:T {id: 1});")
    r = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r) == 1

    # create with errors, rollback automatically
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (n:T {id: 'bad_type'});")
    assert ERROR_STRINGS[ERR_TYPE_CONVERSION] in str(excinfo.value)
    r2 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r2) == 1

    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    r3 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r3) == 1

    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE T DROP COLUMN not_exist;")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    r4 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r4) == 1

    with pytest.raises(Exception) as excinfo:
        conn.execute("DROP TABLE not_exist;")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    r5 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r5) == 1

    with pytest.raises(Exception) as excinfo:
        conn.execute("MATCH (n:T) WHERE n.id = 1 SET n.not_exist = 1;")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    r6 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r6) == 1

    conn.close()
    db.close()


# DB-004-08
@pytest.mark.skip(reason="not supported yet")
def test_manual_transaction_management(tmp_path):
    db_dir = tmp_path / "manual_tx_mgmt"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn = db.connect()
    # BEGIN/COMMIT
    conn.execute("BEGIN TRANSACTION;")
    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    conn.execute("CREATE (n:T {id: 1});")
    conn.execute("COMMIT;")
    r = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r) == 1

    # BEGIN/ROLLBACK: DML
    conn.execute("BEGIN TRANSACTION;")
    conn.execute("CREATE (n:T {id: 2});")
    conn.execute("ROLLBACK;")
    r2 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r2) == 1

    # BEGIN/ROLLBACK: CREATE TABLE
    conn.execute("BEGIN TRANSACTION;")
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")  # 已存在
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.execute("ROLLBACK;")
    r3 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r3) == 1

    # BEGIN/ROLLBACK: ALTER TABLE
    conn.execute("BEGIN TRANSACTION;")
    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE T DROP COLUMN not_exist;")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.execute("ROLLBACK;")
    r4 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r4) == 1

    # BEGIN/ROLLBACK: DROP TABLE
    conn.execute("BEGIN TRANSACTION;")
    with pytest.raises(Exception) as excinfo:
        conn.execute("DROP TABLE not_exist;")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.execute("ROLLBACK;")
    r5 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r5) == 1

    # BEGIN/ROLLBACK: SET properties
    conn.execute("BEGIN TRANSACTION;")
    with pytest.raises(Exception) as excinfo:
        conn.execute("MATCH (n:T) WHERE n.id = 1 SET n.not_exist = 1;")
    assert ERROR_STRINGS[ERR_SCHEMA_MISMATCH] in str(excinfo.value)
    conn.execute("ROLLBACK;")
    r6 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r6) == 1

    conn.close()
    db.close()


# DB-004-09
@pytest.mark.skip(reason="not supported yet")
def test_readonly_transaction_write(tmp_path):
    db_dir = tmp_path / "readonly_tx_write"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn = db.connect()
    conn.execute("BEGIN TRANSACTION READ ONLY;")
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    assert ERROR_STRINGS[ERR_TX_STATE_CONFLICT] in str(excinfo.value)
    conn.execute("ROLLBACK;")
    conn.close()
    db.close()


# DB-004-11
@pytest.mark.skip(reason="not supported yet")
def test_nested_transaction(tmp_path):
    db_dir = tmp_path / "nested_tx"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn = db.connect()
    conn.execute("BEGIN TRANSACTION;")
    with pytest.raises(Exception):
        conn.execute("BEGIN TRANSACTION;")
    conn.execute("ROLLBACK;")
    conn.close()
    db.close()


# DB-004-12
@pytest.mark.skip(reason="not supported yet")
def test_transaction_timeout(tmp_path):
    db_dir = tmp_path / "tx_timeout"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn = db.connect()
    conn.execute("BEGIN TRANSACTION;")
    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    # sleep to trigger timeout, assuming timeout is set to 5 seconds
    time.sleep(5)
    with pytest.raises(Exception) as excinfo:
        conn.execute("COMMIT;")
    assert ERROR_STRINGS[ERR_TX_TIMEOUT] in str(excinfo.value)
    conn.close()
    db.close()


# DB-004-13
@pytest.mark.skip(reason="not supported yet")
def test_commit_after_rollback(tmp_path):
    db_dir = tmp_path / "commit_after_rollback"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn = db.connect()
    conn.execute("BEGIN TRANSACTION;")
    conn.execute("ROLLBACK;")
    with pytest.raises(Exception):
        conn.execute("COMMIT;")
    conn.close()
    db.close()


# DB-004-14
@pytest.mark.skip(reason="not supported yet")
def test_crash_recovery(tmp_path):
    db_dir = tmp_path / "crash_recovery"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn = db.connect()
    conn.execute("BEGIN TRANSACTION;")
    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    conn.execute("CREATE (n:T {id: 1});")
    conn.execute("COMMIT;")
    conn.execute("BEGIN TRANSACTION;")
    conn.execute("CREATE (n:T {id: 2});")
    conn.close()
    db.close()
    db2 = Database(db_path=str(db_dir), mode="w", planner="gopt")
    conn2 = db2.connect()
    # committed transaction should be visible
    r = conn2.execute("MATCH (n:T) WHERE n.id = 1 RETURN n;")
    assert len(r) == 1
    # uncommitted transaction should not be visible
    r2 = conn2.execute("MATCH (n:T) WHERE n.id = 2 RETURN n;")
    assert len(r2) == 0
    conn2.close()
    db2.close()
