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
import subprocess
import sys
import textwrap
import time

import pytest

from neug.database import Database
from neug.proto.error_pb2 import ERR_COMPILATION
from neug.proto.error_pb2 import ERR_CONNECTION_CLOSED
from neug.proto.error_pb2 import ERR_DATABASE_LOCKED
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_NOT_SUPPORTED
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


def test_embedded_explicit_transaction_commits_multiple_copies(
    tmp_path, transaction_control
):
    db_dir = tmp_path / "explicit_copy_transaction"
    people_a = tmp_path / "people_a.csv"
    people_b = tmp_path / "people_b.csv"
    people_a.write_text("id,name\n1,Alice\n", encoding="utf-8")
    people_b.write_text("id,name\n2,Bob\n", encoding="utf-8")

    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    tx = transaction_control(conn)
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));")

    tx.begin()
    conn.execute(
        f'COPY Person FROM "{people_a.as_posix()}" (HEADER=true, DELIMITER=",");'
    )
    assert list(conn.execute("MATCH (n:Person) RETURN n.id ORDER BY n.id;")) == [[1]]
    conn.execute(
        f'COPY Person FROM "{people_b.as_posix()}" (HEADER=true, DELIMITER=",");'
    )
    tx.commit()
    assert list(conn.execute("MATCH (n:Person) RETURN n.id ORDER BY n.id;")) == [
        [1],
        [2],
    ]
    conn.close()
    db.close()

    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    assert list(conn.execute("MATCH (n:Person) RETURN n.id ORDER BY n.id;")) == [
        [1],
        [2],
    ]
    conn.close()
    db.close()


@pytest.mark.parametrize("empty", [True, False])
@pytest.mark.parametrize("target", ["vertex", "edge"])
def test_embedded_copy_transaction_rejects_temporary_targets(tmp_path, empty, target):
    people = tmp_path / "people.csv"
    people.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    edges = tmp_path / "edges.csv"
    edges.write_text("src,dst\n1,2\n", encoding="utf-8")
    added = tmp_path / "added.csv"
    header, row = (
        ("id,name\n", "3,Carol\n") if target == "vertex" else ("src,dst\n", "2,1\n")
    )
    added.write_text(header + ("" if empty else row), encoding="utf-8")
    options = "(HEADER=true, DELIMITER=',')"
    edge_options = "(HEADER=true, DELIMITER=',', FROM='TempPerson', TO='TempPerson')"
    table = "TempPerson" if target == "vertex" else "TempKnows"
    copy_options = options if target == "vertex" else edge_options
    db = Database(db_path=str(tmp_path / "db"), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    try:
        conn.execute(f"COPY TEMP TempPerson FROM '{people.as_posix()}' {options}")
        conn.execute(f"COPY TEMP TempKnows FROM '{edges.as_posix()}' {edge_options}")
        conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))")
        conn.begin_transaction()
        conn.execute(f"COPY Person FROM '{people.as_posix()}' {options}")
        with pytest.raises(RuntimeError, match="Only persistent COPY FROM") as error:
            conn.execute(f"COPY {table} FROM '{added.as_posix()}' {copy_options}")
        assert str(ERR_NOT_SUPPORTED) in str(error.value)
        for operation in (
            lambda: conn.execute("MATCH (p:Person) RETURN count(p)"),
            conn.commit,
        ):
            with pytest.raises(RuntimeError, match="rollback-only"):
                operation()
        conn.rollback()
        assert list(conn.execute("MATCH (p:Person) RETURN count(p)")) == [[0]]
        assert list(conn.execute("MATCH (p:TempPerson) RETURN count(p)")) == [[2]]
        assert list(conn.execute("MATCH ()-[e:TempKnows]->() RETURN count(e)")) == [[1]]

        # The restriction belongs to explicit transactions, not auto-commit COPY.
        conn.execute(f"COPY {table} FROM '{added.as_posix()}' {copy_options}")
        query = (
            "MATCH (p:TempPerson) RETURN count(p)"
            if target == "vertex"
            else "MATCH ()-[e:TempKnows]->() RETURN count(e)"
        )
        expected = (2 if target == "vertex" else 1) + int(not empty)
        assert list(conn.execute(query)) == [[expected]]
    finally:
        conn.close()
        db.close()


@pytest.mark.parametrize("commit", [True, False])
def test_embedded_copy_transaction_inferred_schema(tmp_path, commit):
    db_path = str(tmp_path / "inferred_db")
    people = tmp_path / "people.csv"
    people.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    edges = tmp_path / "edges.csv"
    edges.write_text("src,dst\n1,2\n", encoding="utf-8")
    node_query = "MATCH (p:InferredPerson) RETURN p.id, p.name ORDER BY p.id"
    edge_query = "MATCH (a:InferredPerson)-[:InferredKnows]->(b) RETURN a.id, b.id"
    db = Database(db_path=db_path, mode="w", checkpoint_on_close=False)
    conn = db.connect()
    try:
        conn.begin_transaction()
        conn.execute(
            f"COPY InferredPerson FROM '{people.as_posix()}' (HEADER=true, DELIMITER=',')"
        )
        conn.execute(
            f"COPY InferredKnows FROM '{edges.as_posix()}' "
            "(HEADER=true, DELIMITER=',', FROM='InferredPerson', TO='InferredPerson')"
        )
        assert list(conn.execute(node_query)) == [[1, "Alice"], [2, "Bob"]]
        assert list(conn.execute(edge_query)) == [[1, 2]]
        if commit:
            conn.commit()
        else:
            conn.rollback()
        assert ("InferredPerson" in conn.get_schema()) == commit
        assert ("InferredKnows" in conn.get_schema()) == commit
    finally:
        conn.close()
        db.close()

    db = Database(db_path=db_path, mode="w", checkpoint_on_close=False)
    conn = db.connect()
    try:
        assert ("InferredPerson" in conn.get_schema()) == commit
        assert ("InferredKnows" in conn.get_schema()) == commit
        if commit:
            assert list(conn.execute(node_query)) == [[1, "Alice"], [2, "Bob"]]
            assert list(conn.execute(edge_query)) == [[1, 2]]
    finally:
        conn.close()
        db.close()


@pytest.mark.parametrize("bundled", [True, False], ids=["sorted", "multi-property"])
def test_embedded_copy_transaction_repeated_edges_and_recovery(tmp_path, bundled):
    db_dir = tmp_path / "copy_edges"
    people_a = tmp_path / "people_a.csv"
    people_b = tmp_path / "people_b.csv"
    edges_a = tmp_path / "edges_a.csv"
    edges_b = tmp_path / "edges_b.csv"
    people_a.write_text("id\n1\n2\n", encoding="utf-8")
    # Grow beyond the first COPY's reserved capacity after loading an edge.
    people_b.write_text(
        "id\n" + "".join(f"{i}\n" for i in range(3, 6001)), encoding="utf-8"
    )
    header = "from,to,since\n" if bundled else "from,to,since,name\n"
    edges_a.write_text(
        header + ("1,2,20\n" if bundled else "1,2,20,First\n"), encoding="utf-8"
    )
    edges_b.write_text(
        header + ("1,6000,10\n" if bundled else "1,6000,10,Second\n"), encoding="utf-8"
    )

    def copy(conn, table, path):
        conn.execute(
            f"COPY {table} FROM '{path.as_posix()}' (HEADER=true, DELIMITER=',');"
        )

    edge_query = (
        "MATCH (a:Person)-[e:Knows]->(b:Person) RETURN b.id, e.since"
        + ("" if bundled else ", e.name")
        + " ORDER BY b.id;"
    )
    first = [[2, 20]] if bundled else [[2, 20, "First"]]
    expected = first + ([[6000, 10]] if bundled else [[6000, 10, "Second"]])
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute(
        "CREATE REL TABLE Knows(FROM Person TO Person, since INT64"
        + (") WITH (sort_key_for_nbr='since');" if bundled else ", name STRING);")
    )
    conn.begin_transaction()
    copy(conn, "Person", people_a)
    copy(conn, "Knows", edges_a)
    assert list(conn.execute(edge_query)) == first
    copy(conn, "Person", people_b)
    assert list(conn.execute(edge_query)) == first
    copy(conn, "Knows", edges_b)
    assert list(conn.execute(edge_query)) == expected
    assert list(
        conn.execute(
            "MATCH (a:Person)-[e:Knows]->(b:Person) WHERE e.since < 15 RETURN b.id;"
        )
    ) == [[6000]]
    conn.commit()

    # Exercise logical-WAL commit in the epoch activated by COPY commit.
    conn.begin_transaction()
    conn.execute("CREATE (:Person {id: 7000});")
    conn.commit()
    conn.close()
    db.close()

    for cycle in range(2):
        db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
        conn = db.connect()
        # Check all previously durable rows before the next mutation.
        expected_ids = list(range(1, 6001)) + [7000] + list(range(8000, 8000 + cycle))
        assert list(conn.execute("MATCH (n:Person) RETURN n.id ORDER BY n.id;")) == [
            [i] for i in expected_ids
        ]
        assert list(conn.execute(edge_query)) == expected
        if cycle == 0:
            more_people = tmp_path / "more_people.csv"
            more_people.write_text("id\n8000\n", encoding="utf-8")
            conn.begin_transaction()
            copy(conn, "Person", more_people)
            conn.commit()
        conn.close()
        db.close()


@pytest.mark.parametrize("failure", ["copy", "checkpoint-preparation"])
def test_embedded_copy_transaction_failure_preserves_committed_data(tmp_path, failure):
    db_dir = tmp_path / "copy_failure"
    base = tmp_path / "base.csv"
    added = tmp_path / "added.csv"
    base.write_text("id\n0\n", encoding="utf-8")
    added.write_text("id\n1\n", encoding="utf-8")
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute(f"COPY Person FROM '{base.as_posix()}' (HEADER=true);")
    current = db_dir / "checkpoint" / "CURRENT"
    checkpoint_before = current.read_text()
    conn.begin_transaction()
    conn.execute(f"COPY Person FROM '{added.as_posix()}' (HEADER=true);")
    if failure == "copy":
        with pytest.raises(RuntimeError):
            conn.execute(f"COPY Person FROM '{(tmp_path / 'missing.csv').as_posix()}';")
    else:
        # Force CreateStaging to fail before checkpoint consumes storage.
        # Restore the real manifests even if the assertion fails.
        manifests = db_dir / "checkpoint" / "manifests"
        saved = db_dir / "checkpoint" / "saved_manifests"
        manifests.rename(saved)
        try:
            manifests.write_text(
                "block checkpoint directory creation", encoding="utf-8"
            )
            with pytest.raises(RuntimeError, match="failed to create"):
                conn.commit()
        finally:
            manifests.unlink()
            saved.rename(manifests)
    with pytest.raises(RuntimeError, match="rollback-only"):
        conn.commit()
    conn.rollback()
    assert current.read_text() == checkpoint_before
    assert list(conn.execute("MATCH (n:Person) RETURN n.id;")) == [[0]]
    # The failed commit must not leave a stale staging handle or commit mode.
    conn.begin_transaction()
    conn.execute(f"COPY Person FROM '{added.as_posix()}' (HEADER=true);")
    conn.commit()
    assert list(conn.execute("MATCH (n:Person) RETURN n.id ORDER BY n.id;")) == [
        [0],
        [1],
    ]
    conn.close()
    db.close()
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    assert list(conn.execute("MATCH (n:Person) RETURN n.id ORDER BY n.id;")) == [
        [0],
        [1],
    ]
    conn.close()
    db.close()


@pytest.mark.parametrize("commit", [False, True], ids=["uncommitted", "committed"])
def test_embedded_copy_transaction_survives_process_exit(tmp_path, commit):
    db_dir = tmp_path / "copy_process_exit"
    for name, value in [("base", 0), ("a", 1), ("b", 2)]:
        (tmp_path / f"{name}.csv").write_text(f"id\n{value}\n", encoding="utf-8")
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute(
        f"COPY Person FROM '{(tmp_path / 'base.csv').as_posix()}' (HEADER=true);"
    )
    conn.close()
    db.close()
    script = textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path
        from neug import Database

        root = Path(sys.argv[1])
        db = Database(db_path=str(root / 'copy_process_exit'), mode='w',
                      checkpoint_on_close=False)
        conn = db.connect()
        conn.begin_transaction()
        for name in ['a', 'b']:
            path = (root / (name + '.csv')).as_posix()
            conn.execute(f"COPY Person FROM '{path}' (HEADER=true);")
        if sys.argv[2] == 'commit':
            conn.commit()
        # Do not let Connection/Database destructors roll back or checkpoint.
        os._exit(0)
        """
    )
    subprocess.run(
        [sys.executable, "-c", script, str(tmp_path), "commit" if commit else "abort"],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    assert list(conn.execute("MATCH (n:Person) RETURN n.id ORDER BY n.id;")) == (
        [[0], [1], [2]] if commit else [[0]]
    )
    conn.close()
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
