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

import json
import queue
import threading
import time

import pytest
from conftest import wait_for_server_ready

from neug.database import Database
from neug.session import Session


@pytest.fixture
def tp_endpoint(tmp_path, unused_tcp_port, request):
    explicit_transaction_timeout_ms = getattr(request, "param", 60000)
    db = Database(
        db_path=str(tmp_path / "tp_explicit_transaction"),
        mode="w",
        max_thread_num=2,
    )
    endpoint = db.serve(
        port=unused_tcp_port,
        host="localhost",
        blocking=False,
        thread_num=2,
        auto_compaction=False,
        explicit_transaction_timeout_ms=explicit_transaction_timeout_ms,
    )
    wait_for_server_ready(endpoint)
    yield endpoint
    db.stop_serving()
    db.close()


@pytest.mark.parametrize(
    "response_body",
    [
        ValueError("invalid JSON"),
        {"mode": "read_write"},
        {"transaction_id": "different-transaction-id"},
    ],
    ids=["invalid_json", "missing_transaction_id", "mismatched_transaction_id"],
)
def test_tp_begin_cleans_up_transaction_when_response_is_unusable(response_body):
    class BeginResponse:
        status_code = 201
        headers = {"Location": "/transactions/transaction-id"}

        def json(self):
            if isinstance(response_body, ValueError):
                raise response_body
            return response_body

    class RollbackResponse:
        status_code = 200

    class HttpSession:
        def __init__(self):
            self.requests = []
            self.responses = [BeginResponse(), RollbackResponse()]

        def post(self, endpoint, data, timeout):
            self.requests.append((endpoint, data, timeout))
            return self.responses.pop(0)

    session = object.__new__(Session)
    session._closed = False
    session._transaction_id = None
    session._transactions_endpoint = "http://example.test/transactions"
    session._timeout = "10s"
    session._http_session = HttpSession()

    with pytest.raises(RuntimeError, match="Transaction begin response"):
        session.begin_transaction()

    assert not session.has_active_transaction
    assert session._http_session.requests == [
        ("http://example.test/transactions", '{"mode": "read_write"}', 10),
        (
            "http://example.test/transactions/transaction-id/rollback",
            "",
            10,
        ),
    ]


def test_tp_explicit_transaction_lifecycle_schema_and_close(tp_endpoint):
    setup = Session.open(tp_endpoint, num_threads=1)
    transaction = Session.open(tp_endpoint, num_threads=1)
    observer = Session.open(tp_endpoint, num_threads=1)
    try:
        setup.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));", "schema")

        transaction.begin_transaction()
        assert transaction.has_active_transaction
        transaction.execute("CREATE (:Person {id: 1});", "update")
        assert list(
            transaction.execute("MATCH (p:Person {id: 1}) RETURN p.id;", "read")
        ) == [[1]]
        assert (
            list(observer.execute("MATCH (p:Person {id: 1}) RETURN p.id;", "read"))
            == []
        )

        transaction.execute(
            "CREATE NODE TABLE TransactionOnly(id INT64, PRIMARY KEY(id));",
            "schema",
        )
        assert "TransactionOnly" not in json.dumps(observer.get_schema())

        transaction.commit()
        assert not transaction.has_active_transaction
        assert list(
            observer.execute("MATCH (p:Person {id: 1}) RETURN p.id;", "read")
        ) == [[1]]
        assert "TransactionOnly" in json.dumps(observer.get_schema())

        transaction.begin_transaction()
        transaction.execute("CREATE (:Person {id: 2});", "update")
        transaction.rollback()
        assert (
            list(observer.execute("MATCH (p:Person {id: 2}) RETURN p.id;", "read"))
            == []
        )

        transaction.begin_transaction()
        transaction.execute("CREATE (:Person {id: 3});", "update")
        transaction.close()
        assert not transaction.has_active_transaction
        assert (
            list(observer.execute("MATCH (p:Person {id: 3}) RETURN p.id;", "read"))
            == []
        )
    finally:
        setup.close()
        transaction.close()
        observer.close()


def test_tp_explicit_transaction_read_only_failure_requires_rollback(tp_endpoint):
    setup = Session.open(tp_endpoint, num_threads=1)
    transaction = Session.open(tp_endpoint, num_threads=1)
    try:
        setup.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));", "schema")

        transaction.begin_transaction(read_only=True)
        with pytest.raises(ValueError, match="Invalid access_mode"):
            transaction.execute("MATCH (p:Person) RETURN p.id;", "invalid")
        assert transaction.has_active_transaction
        with pytest.raises(Exception, match="Http code: 409"):
            transaction.execute("CREATE (:Person {id: 1});", "update")
        assert transaction.has_active_transaction
        with pytest.raises(RuntimeError, match="Http code: 409"):
            transaction.commit()
        assert transaction.has_active_transaction

        transaction.rollback()
        assert not transaction.has_active_transaction
    finally:
        setup.close()
        transaction.close()


@pytest.mark.parametrize("tp_endpoint", [20], indirect=True)
def test_tp_session_recovers_after_transaction_expiry(tp_endpoint):
    transaction = Session.open(tp_endpoint, num_threads=1)
    try:
        transaction.begin_transaction(read_only=True)
        time.sleep(0.1)

        with pytest.raises(Exception, match="Http code: 410"):
            transaction.execute("MATCH (n) RETURN n LIMIT 1;", "read")
        assert not transaction.has_active_transaction

        transaction.begin_transaction(read_only=True)
        assert transaction.has_active_transaction
        transaction.rollback()
        assert not transaction.has_active_transaction
    finally:
        transaction.close()


def test_tp_read_only_transactions_are_isolated_across_client_threads(tp_endpoint):
    setup = Session.open(tp_endpoint, num_threads=1)
    try:
        setup.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));", "schema")
        setup.execute("CREATE (:Person {id: 1});", "insert")
    finally:
        setup.close()

    ready = threading.Barrier(3, timeout=10)
    proceed = threading.Event()
    observed_counts = queue.Queue()
    errors = queue.Queue()

    def read_from_pinned_snapshot():
        session = None
        try:
            session = Session.open(tp_endpoint, num_threads=1)
            session.begin_transaction(read_only=True)
            ready.wait()
            assert proceed.wait(timeout=10)
            rows = list(session.execute("MATCH (p:Person) RETURN count(p);", "read"))
            observed_counts.put(rows[0][0])
        except Exception as error:
            errors.put(error)
        finally:
            if session is not None:
                if session.has_active_transaction:
                    try:
                        session.rollback()
                    except Exception as error:
                        errors.put(error)
                session.close()

    threads = [threading.Thread(target=read_from_pinned_snapshot) for _ in range(2)]
    for thread in threads:
        thread.start()

    ready.wait()
    writer = Session.open(tp_endpoint, num_threads=1)
    try:
        writer.execute("CREATE (:Person {id: 2});", "insert")
    finally:
        writer.close()
        proceed.set()

    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()
    assert errors.empty(), list(errors.queue)
    assert sorted(observed_counts.queue) == [1, 1]
