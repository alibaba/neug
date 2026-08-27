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
import shutil
import sys
import time

import pytest

from neug.database import Database
from neug.proto.error_pb2 import ERR_COMPILATION
from neug.proto.error_pb2 import ERR_CONNECTION_CLOSED
from neug.proto.error_pb2 import ERR_DATABASE_LOCKED
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_QUERY_SYNTAX
from neug.proto.error_pb2 import ERR_SCHEMA_MISMATCH
from neug.proto.error_pb2 import ERR_TX_STATE_CONFLICT
from neug.proto.error_pb2 import ERR_TYPE_CONVERSION


class ConnectionApiTransactionControl:
    """Explicit transaction control through the embedded Connection API."""

    def __init__(self, conn):
        self._conn = conn

    def begin(self, read_only=False):
        self._conn.begin_transaction(read_only=read_only)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()


@pytest.fixture(
    params=[pytest.param(ConnectionApiTransactionControl, id="connection-api")]
)
def transaction_control(request):
    """Create the currently supported explicit-transaction control surface.

    Add a Cypher control implementation here only after query-level BEGIN,
    COMMIT, and ROLLBACK are supported.
    """
    return request.param


# DB-004-01
def test_ap_read_concurrent(tmp_path):
    db_dir = tmp_path / "modern_graph"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("modern_graph")
    db.close()
    db = Database(db_path=str(db_dir), mode="r")
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
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        # in rw mode, only one connection is allowed
        db.connect()
    assert str(ERR_TX_STATE_CONFLICT) in str(excinfo.value)
    conn.close()
    db.close()


# DB-004-03
def test_ap_read_write_concurrent(tmp_path):
    db_dir = tmp_path / "modern_graph"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        # in rw mode, only one connection is allowed
        db.connect()
    assert str(ERR_TX_STATE_CONFLICT) in str(excinfo.value)
    conn.close()
    db.close()


@pytest.fixture
def unused_tcp_port():
    return 10000


@pytest.fixture
def started_server(tmp_path, unused_tcp_port):
    db_dir = tmp_path / "remote_db"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=str(db_dir), mode="w")
    endpoint = db.serve(port=unused_tcp_port, host="localhost", blocking=False)
    # sleep to ensure server is ready
    time.sleep(1)
    yield db, endpoint
    db.close()


# DB-004-04
def test_tp_read_concurrent(started_server):
    db, endpoint = started_server
    from neug.session import Session

    session = Session.open(endpoint)
    session.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    session.execute("CREATE (n:T {id: 1});")
    session.execute("CREATE (n:T {id: 2});")

    s1 = Session.open(endpoint)
    s2 = Session.open(endpoint)
    r1 = s1.execute("MATCH (n) RETURN count(n);")
    r2 = s2.execute("MATCH (n) RETURN count(n);")
    assert r1.__next__()[0] == 2
    assert r2.__next__()[0] == 2
    s1.close()
    s2.close()


# DB-004-05
def test_tp_write_concurrent(started_server):
    db, endpoint = started_server
    from neug.session import Session

    session = Session.open(endpoint)
    session.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")

    s1 = Session.open(endpoint)
    s2 = Session.open(endpoint)
    s1.execute("CREATE (n:T {id: 1});")
    s2.execute("CREATE (n:T {id: 2});")
    r1 = s1.execute("MATCH (n:T) RETURN count(n);")
    r2 = s2.execute("MATCH (n:T) RETURN count(n);")
    assert r1.__next__()[0] == 2
    assert r2.__next__()[0] == 2
    s1.close()
    s2.close()


# DB-004-06
def test_tp_read_write_concurrent(started_server):
    db, endpoint = started_server
    from neug.session import Session

    session = Session.open(endpoint)
    session.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")

    s1 = Session.open(endpoint)
    s2 = Session.open(endpoint)
    r1 = s1.execute("MATCH (n) RETURN count(n);")
    s2.execute("CREATE (n:T {id: 1});")
    r2 = s2.execute("MATCH (n:T) RETURN count(n);")
    assert r1.__next__()[0] == 0
    assert r2.__next__()[0] == 1
    s1.close()
    s2.close()


# DB-004-07
def test_auto_transaction_management(tmp_path):
    db_dir = tmp_path / "auto_tx_mgmt"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    # create success, commit automatically
    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    conn.execute("CREATE (n:T {id: 1});")
    r = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r) == 1

    # create with errors, rollback automatically
    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE (n:T {id: 'bad_type'});")
    assert str(ERR_TYPE_CONVERSION) in str(excinfo.value)
    r2 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r2) == 1

    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    r3 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r3) == 1

    with pytest.raises(Exception) as excinfo:
        conn.execute("ALTER TABLE T DROP not_exist;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    r4 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r4) == 1

    with pytest.raises(Exception) as excinfo:
        conn.execute("DROP TABLE not_exist;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    r5 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r5) == 1

    with pytest.raises(Exception) as excinfo:
        conn.execute("MATCH (n:T) WHERE n.id = 1 SET n.not_exist = 1;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    r6 = conn.execute("MATCH (n:T) RETURN n;")
    assert len(r6) == 1

    conn.close()
    db.close()


# DB-004-08
def test_embedded_explicit_transaction_lifecycle(tmp_path, transaction_control):
    db_dir = tmp_path / "manual_tx_mgmt"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    tx = transaction_control(conn)

    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    tx.begin()
    assert conn.has_active_transaction
    conn.execute("CREATE (n:T {id: 1});")
    assert len(conn.execute("MATCH (n:T) RETURN n;")) == 1
    tx.commit()
    assert not conn.has_active_transaction
    assert len(conn.execute("MATCH (n:T) RETURN n;")) == 1

    tx.begin()
    conn.execute("CREATE (n:T {id: 2});")
    tx.rollback()
    assert not conn.has_active_transaction
    assert len(conn.execute("MATCH (n:T) RETURN n;")) == 1

    tx.begin()
    conn.execute("CREATE (n:T {id: 3});")
    conn.close()

    conn = db.connect()
    assert len(conn.execute("MATCH (n:T) RETURN n;")) == 1
    conn.close()
    db.close()


def test_embedded_explicit_transaction_state_and_schema(tmp_path, transaction_control):
    db_dir = tmp_path / "manual_tx_state"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    tx = transaction_control(conn)

    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    tx.begin()
    conn.execute("CREATE NODE TABLE PrivateT(id INT32, PRIMARY KEY(id));")
    assert "PrivateT" in conn.get_schema()
    tx.rollback()
    assert "PrivateT" not in conn.get_schema()

    tx.begin(read_only=True)
    with pytest.raises(RuntimeError) as excinfo:
        conn.execute("CREATE (n:T {id: 1});")
    assert str(ERR_TX_STATE_CONFLICT) in str(excinfo.value)
    assert conn.has_active_transaction
    with pytest.raises(RuntimeError) as excinfo:
        tx.commit()
    assert str(ERR_TX_STATE_CONFLICT) in str(excinfo.value)
    tx.rollback()

    tx.begin()
    with pytest.raises(RuntimeError) as excinfo:
        tx.begin()
    assert str(ERR_TX_STATE_CONFLICT) in str(excinfo.value)
    tx.rollback()

    tx.begin()
    with pytest.raises(RuntimeError):
        conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    assert conn.has_active_transaction
    with pytest.raises(RuntimeError) as excinfo:
        tx.commit()
    assert str(ERR_TX_STATE_CONFLICT) in str(excinfo.value)
    tx.rollback()

    with pytest.raises(RuntimeError) as excinfo:
        tx.commit()
    assert str(ERR_TX_STATE_CONFLICT) in str(excinfo.value)

    with pytest.raises(RuntimeError) as excinfo:
        tx.rollback()
    assert str(ERR_TX_STATE_CONFLICT) in str(excinfo.value)

    conn.close()
    db.close()


def test_embedded_explicit_transaction_preserves_python_api_contracts(
    tmp_path, transaction_control
):
    db_dir = tmp_path / "python_tx_api_contracts"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    tx = transaction_control(conn)

    conn.execute("CREATE NODE TABLE T(id INT32, name STRING, PRIMARY KEY(id));")
    tx.begin()
    conn.execute("CREATE (n:T {id: 1, name: 'parameterized'});")
    result = conn.execute(
        "MATCH (n:T) WHERE n.id = $id RETURN n.name;", parameters={"id": 1}
    )
    tx.commit()
    assert list(result) == [["parameterized"]]

    tx.begin()
    with pytest.raises(ValueError, match="Invalid access_mode"):
        conn.execute("MATCH (n:T) RETURN n;", access_mode="invalid")
    assert conn.has_active_transaction
    assert len(conn.execute("MATCH (n:T) RETURN n;")) == 1
    tx.rollback()

    conn.close()
    assert not conn.has_active_transaction
    for operation in (
        conn.begin_transaction,
        conn.commit,
        conn.rollback,
        conn.get_schema,
    ):
        with pytest.raises(RuntimeError) as excinfo:
            operation()
        assert str(ERR_CONNECTION_CLOSED) in str(excinfo.value)
    db.close()


# DB-004-12
@pytest.mark.skip(
    reason=(
        "Embedded AP explicit transactions do not yet expose timeout "
        "configuration or enforce transaction lifetime."
    )
)
def test_embedded_explicit_transaction_timeout():
    """Explicit transaction timeout is outside the current API scope."""


# DB-004-14
def test_embedded_explicit_transaction_crash_recovery(tmp_path, transaction_control):
    db_dir = tmp_path / "tx_crash_recovery"
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    tx = transaction_control(conn)

    conn.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    tx.begin()
    conn.execute("CREATE (n:T {id: 1});")
    tx.commit()

    tx.begin()
    conn.execute("CREATE (n:T {id: 2});")
    conn.close()
    db.close()

    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    assert len(conn.execute("MATCH (n:T) WHERE n.id = 1 RETURN n;")) == 1
    assert len(conn.execute("MATCH (n:T) WHERE n.id = 2 RETURN n;")) == 0
    conn.close()
    db.close()


# DB-004-15
def test_auto_enable_checkpoint(tmp_path):
    db_dir = tmp_path / "test_checkpoint"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()

    # 1. open database and create some data
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.close()
    db.close()

    # 2. reopen database with checkpoint
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    result = conn.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")
    rows = list(result)
    assert rows == [[1, "Alice", 30], [2, "Bob", 25]]
    conn.close()
    db.close()


# DB-004-16
def test_manual_enable_checkpoint(tmp_path):
    db_dir = tmp_path / "test_checkpoint"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()

    # 1. open database with checkpoint_on_close=True
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=True)
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.close()
    db.close()

    # 2. reopen database with checkpoint
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    result = conn.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")
    rows = list(result)
    assert rows == [[1, "Alice", 30], [2, "Bob", 25]]
    conn.close()
    db.close()


# DB-004-17
def test_disable_checkpoint_on_close_recovers_wal(tmp_path):
    db_dir = tmp_path / "test_checkpoint"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()

    # 1. open database with checkpoint_on_close=False
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.close()
    db.close()

    # 2. reopen database and recover committed writes from WAL
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    result = conn.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")
    rows = list(result)
    assert rows == [[1, "Alice", 30], [2, "Bob", 25]]
    conn.close()
    db.close()


# DB-004-18
def test_manual_checkpoint_command(tmp_path):
    db_dir = tmp_path / "test_checkpoint"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()

    # 1. open database with checkpoint_on_close=True
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.execute("CHECKPOINT;")
    conn.close()
    db.close()

    # 2. reopen database with checkpoint
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    result = conn.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")
    rows = list(result)
    assert rows == [[1, "Alice", 30], [2, "Bob", 25]]
    conn.close()
    db.close()


# DB-004-19
def test_pure_memory_without_parameter(tmp_path):
    # 1. open database with pure_memory model
    db = Database(db_path="", mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.close()

    # 2. reopen database with pure_memory model, data is lost
    db = Database(db_path="", mode="w")
    conn = db.connect()
    result = conn.execute("MATCH (p) RETURN p;")
    rows = list(result)
    assert rows == []
    conn.close()

    # 3. open a pure_memory database using :memory
    db = Database(db_path=":memory", mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.close()


def test_pure_memory_with_true_parameter(tmp_path):
    # 1. open database with pure_memory model
    db = Database(db_path="", mode="w", checkpoint_on_close=True)
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.close()

    # 2. reopen database with pure_memory model, data is lost
    db = Database(db_path="", mode="w")
    conn = db.connect()
    result = conn.execute("MATCH (p) RETURN p;")
    rows = list(result)
    assert rows == []
    conn.close()


def test_pure_memory_with_false_parameter(tmp_path):
    # 1. open database with pure_memory model
    db = Database(db_path="", mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.close()

    # 2. reopen database with pure_memory model, data is lost
    db = Database(db_path="", mode="w")
    conn = db.connect()
    result = conn.execute("MATCH (p) RETURN p;")
    rows = list(result)
    assert rows == []
    conn.close()


# DB-004-20
def test_database_concurrent_read(tmp_path):
    db_dir = tmp_path / "test_checkpoint"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()

    # 1. open database and create some data
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.close()
    db.close()

    # 2. read data concurrently
    db1 = Database(db_path=str(db_dir), mode="r")
    conn1 = db1.connect()
    result1 = conn1.execute(
        "MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;"
    )
    rows1 = list(result1)

    db2 = Database(db_path=str(db_dir), mode="r")
    conn2 = db2.connect()
    result2 = conn2.execute(
        "MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;"
    )
    rows2 = list(result2)

    assert rows1 == [[1, "Alice", 30], [2, "Bob", 25]]
    assert rows1 == rows2
    conn1.close()
    db1.close()
    conn2.close()
    db2.close()


# DB-004-21
def test_database_concurrent_lock(tmp_path):
    db_dir = tmp_path / "test_checkpoint"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()

    # 1. open database and create some data
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.close()
    db.close()

    # 2. read-lock
    db1 = Database(db_path=str(db_dir), mode="r")
    conn1 = db1.connect()
    conn1.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")

    with pytest.raises(Exception) as excinfo:
        db2 = Database(db_path=str(db_dir), mode="w")
        conn2 = db2.connect()
        conn2.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")
        conn2.close()
        db2.close()
    assert str(ERR_DATABASE_LOCKED) in str(excinfo.value)

    conn1.close()
    db1.close()

    # 3. write-lock
    db1 = Database(db_path=str(db_dir), mode="w")
    conn1 = db1.connect()
    conn1.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")

    with pytest.raises(Exception) as excinfo:
        db2 = Database(db_path=str(db_dir), mode="r")
        conn2 = db2.connect()
        conn2.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")
        conn2.close()
        db2.close()
    assert str(ERR_DATABASE_LOCKED) in str(excinfo.value)

    with pytest.raises(Exception) as excinfo:
        db3 = Database(db_path=str(db_dir), mode="w")
        conn3 = db3.connect()
        conn3.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")
        conn3.close()
        db3.close()
    assert str(ERR_DATABASE_LOCKED) in str(excinfo.value)

    conn1.close()
    db1.close()


# DB-004-22
def test_checkpoint_alter(tmp_path):
    db_dir = tmp_path / "test_checkpoint"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()

    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=True)
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.execute("ALTER TABLE person ADD creation INT64;")
    conn.close()
    db.close()

    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    result = conn.execute("MATCH (p:person) RETURN p.creation;")
    rows = list(result)
    assert rows == [[0], [0]]
    conn.execute("ALTER TABLE person DROP creation;")
    result = conn.execute("MATCH (p:person) RETURN p.id, p.name, p.age ORDER BY p.id;")
    rows = list(result)
    assert rows == [[1, "Alice", 30], [2, "Bob", 25]]
    conn.close()
    db.close()
