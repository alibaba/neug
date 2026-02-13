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

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from neug.database import Database
from neug.proto.error_pb2 import ERR_CONFIG_INVALID
from neug.proto.error_pb2 import ERR_CONNECTION_CLOSED
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_LOAD_OVERFLOW
from neug.proto.error_pb2 import ERR_NETWORK
from neug.proto.error_pb2 import ERR_POOL_EXHAUSTED
from neug.proto.error_pb2 import ERR_SESSION_CLOSED


# DB-002-01
# DB-002-02
def test_local_connection(tmp_path):
    shutil.rmtree("/tmp/local_conn_db", ignore_errors=True)
    db_dir = tmp_path / "local_conn_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    assert conn is not None
    conn.close()
    db.close()


def test_open_after_close(tmp_path):
    shutil.rmtree("/tmp/test_open_after_close_db", ignore_errors=True)
    db_dir = tmp_path / "test_open_after_close_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    assert conn is not None
    conn.close()
    # try to open a new connection after closing the previous one
    new_conn = db.connect()
    assert new_conn is not None
    new_conn.close()
    db.close()


# DB-002-03
def test_local_connection_params(tmp_path):
    shutil.rmtree("/tmp/local_conn_param_db", ignore_errors=True)
    db_dir = tmp_path / "local_conn_param_db"
    db = Database(db_path=str(db_dir), mode="w", max_thread_num=4)
    conn = db.connect()
    assert conn is not None
    conn.close()
    db.close()


# DB-002-04
def test_local_connection_invalid_param(tmp_path):
    shutil.rmtree("/tmp/local_conn_invalid_db", ignore_errors=True)
    db_dir = tmp_path / "local_conn_invalid_db"
    with pytest.raises(Exception) as excinfo:
        Database(db_path=str(db_dir), mode="w", max_thread_num=-1)
    assert str(ERR_CONFIG_INVALID) in str(excinfo.value)


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


# DB-002-05
# DB-002-06
def test_remote_connection(started_server):
    db, endpoint = started_server
    from neug.session import Session

    session = Session.open(endpoint)
    assert session is not None
    session.close()


# DB-002-07
def test_remote_connection_params(started_server):
    db, endpoint = started_server
    from neug.session import Session

    session = Session.open(
        endpoint,
        timeout=10,
        num_threads=2,
    )
    # query_timeout and num_threads are not supported
    assert session is not None
    session.close()


# DB-002-08
def test_remote_connection_wrong_ip_port(started_server):
    db, endpoint = started_server
    from neug.session import Session

    with pytest.raises(Exception) as excinfo:
        Session("http://256.256.256.256:10000/")
    assert str(ERR_NETWORK) in str(excinfo.value)
    with pytest.raises(Exception) as excinfo:
        Session("http://127.0.0.1:65536/")
    assert str(ERR_NETWORK) in str(excinfo.value)


# DB-002-09
def test_remote_connection_broken(started_server):
    db, endpoint = started_server
    from neug.session import Session

    session = Session.open(endpoint)
    # simulate server disconnect
    db.close()
    time.sleep(5)
    with pytest.raises(Exception) as excinfo:
        session.execute(
            "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
    assert str(ERR_NETWORK) in str(excinfo.value)
    session.close()


# DB-002-10
@pytest.mark.skip(reason="Not Planned Yet")
def test_tx_not_commit_connection_broken(started_server):
    db, endpoint = started_server
    from neug.session import Session

    session = Session.open(endpoint)
    session.begin()
    session.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
    # simulate server disconnect
    db.close()
    time.sleep(5)
    with pytest.raises(Exception) as excinfo:
        session.commit()
    assert str(ERR_SESSION_CLOSED) in str(excinfo.value)
    # reconnect and check if the transaction is rolled back
    session2 = Session.open(endpoint)
    # TODO: do we support to query schema directly?
    session2.execute("MATCH (n:T) RETURN n")
    result = session2.execute("SHOW TABLES;")
    assert len(result) == 0
    # close the session
    # session2.close()


# DB-002-11
@pytest.mark.skip(reason="planned in stress test issues #524")
def test_server_load_overflow(started_server):
    db, endpoint = started_server
    from neug.session import Session

    sessions = []
    try:
        for _ in range(10000):
            try:
                s = Session.open(endpoint)
                sessions.append(s)
            except Exception as exc:
                assert str(ERR_LOAD_OVERFLOW) in str(exc)
                break
        else:
            pytest.fail("Expected ERR_LOAD_OVERFLOW but did not get exception")
    finally:
        for s in sessions:
            try:
                s.close()
            except Exception:
                pass


# DB-002-12
def test_local_connection_after_close(tmp_path):
    # local connection after close
    shutil.rmtree("/tmp/conn_after_close_db", ignore_errors=True)
    db_dir = tmp_path / "conn_after_close_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.close()
    with pytest.raises(Exception) as excinfo:
        conn.execute("MATCH (n) RETURN n")
    assert str(ERR_CONNECTION_CLOSED) in str(excinfo.value)
    db.close()


# DB-002-12
def test_remote_connection_after_close(started_server):
    # remote connection after close
    url, db = started_server
    from neug.session import Session

    db, endpoint = started_server
    session = Session.open(endpoint)
    session.close()
    with pytest.raises(Exception) as excinfo:
        session.execute(
            "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
    assert str(ERR_SESSION_CLOSED) in str(excinfo.value)


# DB-002-13
def test_server_restart(started_server):
    db, endpoint = started_server
    from neug.session import Session

    session = Session.open(endpoint)
    db.close()
    time.sleep(2)
    shutil.rmtree("/tmp/remote_db", ignore_errors=True)
    db2 = Database(db_path="/tmp/remote_db", mode="w")
    # get port from the original db
    port = int(endpoint.split(":")[-1])
    db2.serve(port=port, host="localhost", blocking=False)
    time.sleep(2)
    try:
        try:
            session.execute(
                "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
            )
        except Exception as e:
            assert str(ERR_SESSION_CLOSED) in str(e)
    finally:
        # session.close()
        db.close()


# DB-002-14
@pytest.mark.skip(reason="planned in stress test issues #524")
def test_connection_pool_exhausted(started_server):
    db, endpoint = started_server
    from neug.session import Session

    # suppose the server has a connection pool limit of 8
    s1 = Session.open(endpoint, num_threads=8)
    # try to open more connections than the pool limit
    with pytest.raises(Exception) as excinfo:
        Session.open(endpoint)
    assert str(ERR_POOL_EXHAUSTED) in str(excinfo.value)
    s1.close()


def test_parallel_connections(tmp_path):
    shutil.rmtree("/tmp/parallel_conn_db", ignore_errors=True)
    db_dir = tmp_path / "parallel_conn_db"
    db = Database(db_path=str(db_dir), mode="r")
    connections = []
    for _ in range(5):
        conn = db.connect()
        connections.append(conn)
    for conn in connections:
        conn.execute("MATCH (n) RETURN n")
        conn.close()
    db.close()


def test_parallel_query_executions(tmp_path):
    shutil.rmtree("/tmp/parallel_query_db", ignore_errors=True)
    db_dir = tmp_path / "parallel_query_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    def run_query(thread_id, conn):
        for i in range(10):
            conn.execute(
                f"CREATE (p: person {{id: {thread_id * 10 + i}, name: 'Node{thread_id * 10 + i}'}});"
            )

    import threading

    conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
    threads = []
    for i in range(10):
        t = threading.Thread(target=run_query, args=(i, conn))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    res = conn.execute("MATCH (p) RETURN p.id AS id ORDER BY id;")
    assert len(res) == 100
    conn.close()


def test_access_mode(tmp_path):
    shutil.rmtree("/tmp/access_mode_db", ignore_errors=True)
    db_dir = tmp_path / "access_mode_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn_rw = db.connect()
    supported_access_modes = ["read", "r", "insert", "i", "update", "u"]
    for mode in supported_access_modes:
        conn_rw.execute(
            f"CREATE NODE TABLE test_table_{mode}(id INT64, PRIMARY KEY(id));",
            access_mode=mode,
        )
    unsupported_access_modes = ["delete", "d", "drop", "dr"]
    for mode in unsupported_access_modes:
        with pytest.raises(Exception) as excinfo:
            conn_rw.execute(
                f"CREATE NODE TABLE test_table_{mode}(id INT64, PRIMARY KEY(id));",
                access_mode=mode,
            )
        assert "Invalid access_mode" in str(excinfo.value)
    conn_rw.close()
    db.close()
