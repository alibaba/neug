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

import concurrent.futures
import os
import random
import shutil
import socket
import sys
import threading
import time
from contextlib import closing

import pytest

from neug.database import Database
from neug.session import Session


def execute_1(endpoint, id):
    from neug.session import Session

    session = Session.open(endpoint)

    if id == 1:
        session.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
        session.execute("CREATE (n:T {id: 1});")
    else:
        time.sleep(1)
        session.execute("CREATE (n:T {id: 2});")

    session.close()


def test_1(tmp_path):
    db_dir = tmp_path / "test_db"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=str(db_dir), mode="w")
    endpoint = db.serve(port=10010, host="localhost", blocking=False)

    import multiprocessing as mp

    from neug.session import Session

    proc1 = mp.Process(
        target=execute_1,
        args=(
            endpoint,
            1,
        ),
    )
    proc2 = mp.Process(
        target=execute_1,
        args=(
            endpoint,
            2,
        ),
    )
    proc1.start()
    proc2.start()
    proc1.join()
    proc2.join()

    session = Session.open(endpoint)
    result = session.execute("MATCH (n) RETURN count(n);")
    assert result.__next__()[0] == 2
    session.close()

    db.close()


def execute_2(endpoint, id):
    from neug.session import Session

    session = Session.open(endpoint)

    for i in range(100):
        session.execute("MATCH (n: T {id: 1}) SET n.value = n.value + CAST(1, 'INT32')")

    session.close()


def test_2(tmp_path):
    db_dir = tmp_path / "test_db"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=str(db_dir), mode="w")
    endpoint = db.serve(port=10010, host="localhost", blocking=False)

    import multiprocessing as mp

    from neug.session import Session

    session = Session.open(endpoint)
    session.execute("CREATE NODE TABLE T(id INT32, value INT32, PRIMARY KEY(id));")
    session.execute("CREATE (n:T {id: 1, value: 0});")

    proc1 = mp.Process(
        target=execute_2,
        args=(
            endpoint,
            1,
        ),
    )
    proc2 = mp.Process(
        target=execute_2,
        args=(
            endpoint,
            2,
        ),
    )
    proc1.start()
    proc2.start()
    proc1.join()
    proc2.join()

    session = Session.open(endpoint)
    result = session.execute("MATCH (n: T {id: 1}) RETURN n.value;")
    assert result.__next__()[0] == 200
    session.close()

    db.close()


# test db concurrent query via multiple threads to one single instance
def test_tp_service_concurrent_query():
    db_dir = "/tmp/test_tp_service_concurrent_query"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    conn.execute("CREATE (p:person {id: 1, name: 'marko', age: 21});")
    conn.execute("CREATE (p:person {id: 2, name: 'vadas', age: 22});")
    conn.execute("CREATE (p:person {id: 3, name: 'josh', age: 23});")
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:knows {weight: 0.1}]->(b);"
    )
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:knows {weight: 0.2}]->(b);"
    )
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:knows {weight: 0.3}]->(b);"
    )
    conn.close()
    uri = db.serve(10002, "localhost", False)
    time.sleep(1)

    sess = Session(uri, timeout="10s")
    res = sess.execute("MATCH (n) return count(n);")
    assert len(res) == 1 and res[0][0] == 3
    res = sess.execute("MATCH (a:person)-[e:knows]->(b:person) RETURN count(e);")
    assert len(res) == 1 and res[0][0] == 3

    # 1. Test concurrent insert
    def insert_task(id_start, id_end):
        session = Session(uri, timeout="10s")
        for i in range(id_start, id_end):
            session.execute(
                query=f"CREATE (p:person {{id: {i}, name: 'name_{i}', age: {20 + i}}});",
                access_mode="insert",
            )
        session.close()

    threads = []
    for i in range(5):
        t = threading.Thread(target=insert_task, args=(4 + i * 10, 4 + (i + 1) * 10))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    res = sess.execute("MATCH (n) return count(n);")
    assert len(res) == 1 and res[0][0] == 53

    # 2. Test concurrent update
    # For each thread, pick a random id to update
    def update_task(id_start, id_end, times):
        session = Session(uri, timeout="10s")
        for _ in range(times):
            id = random.randint(id_start, id_end - 1)
            session.execute(
                query=f"MATCH (p:person {{id: {id}}}) SET p.age = p.age + 1;",
                access_mode="update",
            )
        session.close()

    threads = []
    for i in range(5):
        t = threading.Thread(target=update_task, args=(1, 53, 10))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    # Verify the update results
    total_age = 0
    res = sess.execute("MATCH (p:person) RETURN p.age;")
    for row in res:
        total_age += row[0]
    initial_age_sum = 20 * 53 + sum(range(1, 54))  # initial ages from 21 to 72
    assert total_age == initial_age_sum + 50  # 5 threads * 10 updates

    # 3. Test concurrent create relationship
    def create_rel_task(src_node_name, dst_node_name, edge_name):
        session = Session(uri, timeout="10s")
        session.execute(
            query=f"CREATE NODE TABLE {src_node_name}(id INT64, name STRING, PRIMARY KEY(id));",
            access_mode="schema",
        )
        session.execute(
            query=f"CREATE NODE TABLE {dst_node_name}(id INT64, name STRING, PRIMARY KEY(id));",
            access_mode="schema",
        )
        session.execute(
            query=f"CREATE REL TABLE {edge_name}(FROM {src_node_name} TO {dst_node_name}, weight DOUBLE);",
            access_mode="schema",
        )
        session.execute(
            query=f"CREATE (p:{src_node_name} {{id: 1, name: 'src'}});",
            access_mode="insert",
        )
        session.execute(
            query=f"CREATE (p:{dst_node_name} {{id: 1, name: 'dst'}});",
            access_mode="insert",
        )
        session.execute(
            query=f"MATCH (a:{src_node_name}), (b:{dst_node_name})"
            f" WHERE a.id = 1 AND b.id = 1 CREATE (a)-[:{edge_name} {{weight: 0.5}}]->(b);",
            access_mode="insert",
        )
        session.close()

    threads = []
    for i in range(5):
        t = threading.Thread(
            target=create_rel_task, args=(f"src_node_{i}", f"dst_node_{i}", f"edge_{i}")
        )
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    # Verify the relationship creation
    for i in range(5):
        res = sess.execute(
            f"MATCH (a:src_node_{i})-[e:edge_{i}]->(b:dst_node_{i}) RETURN e;"
        )
        assert len(res) == 1
    sess.close()
    db.stop_serving()
    db.close()


# Insert capacity growth, publication, and WAL recovery regression tests.
def _start_capacity_growth_service(db):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    return db.serve(port, "127.0.0.1", False, thread_num=4, auto_compaction=False)


def test_batch_growth_and_wal_recovery(tmp_path):
    path = str(tmp_path / "capacity")
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id INT64, value STRING, PRIMARY KEY(id))"))
    c.close()
    endpoint = _start_capacity_growth_service(db)
    s = Session(endpoint, timeout="60s")
    try:
        payload = "x" * 200
        # Both row capacity and the independent string append region grow.
        list(
            s.execute(
                "UNWIND CAST($ids, 'INT64[]') AS i CREATE (:Item {id:i,value:$v})",
                access_mode="insert",
                parameters={"v": payload, "ids": list(range(5000))},
            )
        )
        assert list(s.execute("MATCH (n:Item) RETURN count(n)")) == [[5000]]
        assert list(s.execute("MATCH (n:Item {id:4999}) RETURN n.value")) == [[payload]]
    finally:
        s.close()
        db.stop_serving()
        db.close()
    # No checkpoint-on-close: the successful batch must replay against the
    # original small checkpoint without replaying the discarded attempt.
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    try:
        assert list(c.execute("MATCH (n:Item) RETURN count(n)")) == [[5000]]
        assert list(c.execute("MATCH (n:Item {id:4999}) RETURN n.value")) == [[payload]]
    finally:
        c.close()
        db.close()


def test_growth_waits_for_read_transaction(tmp_path):
    db = Database(str(tmp_path / "drain"), max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id INT64, value INT64, PRIMARY KEY(id))"))
    list(c.execute("CREATE (:Item {id:-1,value:42})"))
    c.close()
    endpoint = _start_capacity_growth_service(db)
    reader, writer = Session(endpoint, timeout="60s"), Session(endpoint, timeout="60s")
    try:
        reader.begin_transaction(read_only=True)
        assert list(reader.execute("MATCH (n:Item {id:-1}) RETURN n.value")) == [[42]]
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(
                lambda: list(
                    writer.execute(
                        "UNWIND CAST($ids, 'INT64[]') AS i CREATE (:Item {id:i,value:i})",
                        access_mode="insert",
                        parameters={"ids": list(range(5000))},
                    )
                )
            )
            time.sleep(0.3)
            assert not pending.done()
            reader.commit()
            pending.result(timeout=60)
        assert list(reader.execute("MATCH (n:Item) RETURN count(n)")) == [[5001]]
    finally:
        reader.close()
        writer.close()
        db.stop_serving()
        db.close()


def test_string_payload_growth_without_row_growth(tmp_path):
    path = str(tmp_path / "string_payload")
    db = Database(path, max_thread_num=4, checkpoint_on_close=True)
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id INT64, value STRING, PRIMARY KEY(id))"))
    list(c.execute("CREATE (:Item {id:-1,value:'seed'})"))
    c.close()
    db.close()
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    c.close()
    endpoint = _start_capacity_growth_service(db)
    s = Session(endpoint, timeout="60s")
    try:
        payload = "z" * 200
        list(
            s.execute(
                "UNWIND CAST($ids, 'INT64[]') AS i CREATE (:Item {id:i,value:$v})",
                access_mode="insert",
                parameters={"ids": list(range(1000)), "v": payload},
            )
        )
        assert list(s.execute("MATCH (n:Item) RETURN count(n)")) == [[1001]]
        assert list(s.execute("MATCH (n:Item {id:999}) RETURN n.value")) == [[payload]]
        # The existing update path remains able to grow string storage.
        list(
            s.execute(
                "MATCH (n:Item) SET n.value=$v",
                access_mode="update",
                parameters={"v": "q" * 250},
            )
        )
        assert list(s.execute("MATCH (n:Item {id:999}) RETURN n.value")) == [
            ["q" * 250]
        ]
    finally:
        s.close()
        db.stop_serving()
        db.close()


def test_concurrent_batches_with_string_primary_keys(tmp_path):
    db = Database(
        str(tmp_path / "string_keys"), max_thread_num=4, checkpoint_on_close=False
    )
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id STRING, value STRING, PRIMARY KEY(id))"))
    c.close()
    endpoint = _start_capacity_growth_service(db)

    def insert_batch(worker):
        s = Session(endpoint, timeout="60s")
        keys = [f"{worker}-{i}-" + "k" * 100 for i in range(1500)]
        try:
            list(
                s.execute(
                    "UNWIND CAST($ids, 'STRING[]') AS id CREATE (:Item {id:id,value:$v})",
                    access_mode="insert",
                    parameters={"ids": keys, "v": "x" * 200},
                )
            )
        finally:
            s.close()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(insert_batch, range(4)))
        s = Session(endpoint, timeout="60s")
        try:
            assert list(s.execute("MATCH (n:Item) RETURN count(n)")) == [[6000]]
            for worker in range(4):
                assert list(
                    s.execute(
                        "MATCH (n:Item {id:$id}) RETURN n.value",
                        parameters={"id": f"{worker}-1499-" + "k" * 100},
                    )
                ) == [["x" * 200]]
        finally:
            s.close()
    finally:
        db.stop_serving()
        db.close()


def test_fixed_vector_array_growth(tmp_path):
    db = Database(str(tmp_path / "arrays"), max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    list(
        c.execute(
            "CREATE NODE TABLE Item(id INT64, embedding FLOAT[4], PRIMARY KEY(id))"
        )
    )
    c.close()
    endpoint = _start_capacity_growth_service(db)
    s = Session(endpoint, timeout="60s")
    try:
        query = (
            "UNWIND CAST($ids, 'INT64[]') AS id "
            "CREATE (:Item {id:id,embedding:CAST($v, 'FLOAT[4]')})"
        )
        params = {"ids": list(range(5000)), "v": [1, 2, 3, 4]}
        list(s.execute(query, access_mode="insert", parameters=params))
        assert list(s.execute("MATCH (n:Item) RETURN count(n)")) == [[5000]]
        assert list(s.execute("MATCH (n:Item {id:4999}) RETURN n.embedding")) == [
            [[1, 2, 3, 4]]
        ]
    finally:
        s.close()
        db.stop_serving()
        db.close()


def test_atomic_growth_four_readers_four_writers(tmp_path):
    db = Database(
        str(tmp_path / "atomic_mixed"), max_thread_num=8, checkpoint_on_close=False
    )
    c = db.connect()
    list(c.execute("CREATE NODE TABLE Item(id INT64, value STRING, PRIMARY KEY(id))"))
    list(c.execute("CREATE REL TABLE Link(FROM Item TO Item, note STRING)"))
    list(c.execute("CREATE (:Item {id:-1,value:'anchor'})"))
    c.close()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = db.serve(port, "127.0.0.1", False, thread_num=8, auto_compaction=False)
    barrier = threading.Barrier(8)

    def worker(index):
        session = Session(endpoint, timeout="60s")
        try:
            barrier.wait(timeout=30)
            if index < 4:
                for _ in range(40):
                    assert list(
                        session.execute("MATCH (n:Item {id:-1}) RETURN n.value")
                    ) == [["anchor"]]
                    count = list(session.execute("MATCH (n:Item) RETURN count(n)"))[0][
                        0
                    ]
                    assert 1 <= count <= 10001
            else:
                base = (index - 4) * 2500
                for batch in range(10):
                    list(
                        session.execute(
                            "UNWIND CAST($ids, 'INT64[]') AS id CREATE (:Item {id:id,value:$v})",
                            access_mode="insert",
                            parameters={
                                "ids": list(
                                    range(base + batch * 250, base + (batch + 1) * 250)
                                ),
                                "v": "x" * 200,
                            },
                        )
                    )
        finally:
            session.close()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(8)))
        session = Session(endpoint, timeout="60s")
        try:
            assert list(session.execute("MATCH (n:Item) RETURN count(n)")) == [[10001]]
            list(
                session.execute(
                    "MATCH (n:Item {id:9999}) SET n.value=$v",
                    access_mode="update",
                    parameters={"v": "y" * 250},
                )
            )
            assert list(session.execute("MATCH (n:Item {id:9999}) RETURN n.value")) == [
                ["y" * 250]
            ]
            list(
                session.execute(
                    "MATCH (a:Item {id:-1}), (b:Item {id:9999}) CREATE (a)-[:Link {note:'after growth'}]->(b)",
                    access_mode="insert",
                )
            )
            assert list(
                session.execute(
                    "MATCH (:Item {id:-1})-[e:Link]->(:Item {id:9999}) RETURN e.note"
                )
            ) == [["after growth"]]
        finally:
            session.close()
    finally:
        db.stop_serving()
        db.close()


@pytest.mark.parametrize(
    "property_type,expression,expected",
    [
        ("STRING[]", "CAST([$v, $v], 'STRING[]')", ["x" * 200] * 2),
        ("STRING[2]", "CAST([$v, $v], 'STRING[2]')", ["x" * 200] * 2),
        ("INT64[]", "CAST([1, 2, 3], 'INT64[]')", [1, 2, 3]),
        (
            "STRUCT(name STRING, tags STRING[])",
            "CAST({name:$v, tags:CAST([$v, $v], 'STRING[]')}, "
            "'STRUCT(name STRING, tags STRING[])')",
            {"name": "x" * 200, "tags": ["x" * 200] * 2},
        ),
        (
            "STRING[][2][]",
            "CAST([CAST([CAST([$v], 'STRING[]'), CAST([$v], 'STRING[]')], "
            "'STRING[][2]')], 'STRING[][2][]')",
            [[["x" * 200], ["x" * 200]]],
        ),
    ],
)
def test_nested_insert_capacity_and_recovery(
    tmp_path, property_type, expression, expected
):
    expression = expression.replace("$v", "'" + "x" * 200 + "'")
    path = str(tmp_path / "nested_capacity")
    # Reopening a checkpoint leaves LIST elements and string payloads without
    # spare capacity, while the table still has ample row slots for this batch.
    db = Database(path, max_thread_num=4, checkpoint_on_close=True)
    c = db.connect()
    list(
        c.execute(
            f"CREATE NODE TABLE Item(id INT64, value {property_type}, PRIMARY KEY(id))"
        )
    )
    list(c.execute(f"CREATE (:Item {{id:-1,value:{expression}}})", parameters={}))
    c.close()
    db.close()
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    endpoint = _start_capacity_growth_service(db)

    def write(worker):
        with closing(Session(endpoint, timeout="60s")) as session:
            for batch in range(4):
                ids = list(
                    range(worker * 80 + batch * 20, worker * 80 + (batch + 1) * 20)
                )
                list(
                    session.execute(
                        f"UNWIND CAST($ids, 'INT64[]') AS id CREATE (:Item {{id:id,value:{expression}}})",
                        access_mode="insert",
                        parameters={"ids": ids},
                    )
                )

    try:
        # Exercise capacity growth independently of LIST append synchronization.
        for worker in range(4):
            write(worker)
        with closing(Session(endpoint, timeout="60s")) as session:
            assert list(session.execute("MATCH (n:Item) RETURN count(n)")) == [[321]]
            for row in session.execute("MATCH (n:Item) RETURN n.value"):
                assert row[0] == expected, repr(row[0])
    finally:
        db.stop_serving()
        db.close()
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    c = db.connect()
    try:
        values = list(c.execute("MATCH (n:Item) RETURN n.value"))
        assert len(values) == 321
        assert all(row[0] == expected for row in values)
    finally:
        c.close()
        db.close()


@pytest.mark.parametrize("property_type", ["INT64[]", "STRING[]", "STRING[][]"])
def test_concurrent_list_distinct_payload_and_recovery(tmp_path, property_type):
    import json

    def payload(row):
        count = row % 3 + 1
        if property_type == "INT64[]":
            return [row * 10 + i for i in range(count)]
        if property_type == "STRING[]":
            return [f"row-{row}-element-{i}" for i in range(count)]
        return [
            [f"row-{row}-list-{i}-element-{j}" for j in range(i + 1)]
            for i in range(count)
        ]

    def expression(values, data_type):
        child_type = data_type[:-2]
        children = [
            expression(v, child_type) if child_type.endswith("[]") else json.dumps(v)
            for v in values
        ]
        return f"CAST([{','.join(children)}], '{data_type}')"

    path = str(tmp_path / "list_cas")
    db = Database(path, max_thread_num=4, checkpoint_on_close=True)
    conn = db.connect()
    list(
        conn.execute(
            f"CREATE NODE TABLE Item(id INT64, value {property_type}, PRIMARY KEY(id))"
        )
    )
    list(
        conn.execute(
            f"CREATE (:Item {{id:-1,value:{expression(payload(-1), property_type)}}})"
        )
    )
    conn.close()
    db.close()
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    endpoint = _start_capacity_growth_service(db)
    barrier = threading.Barrier(4)
    count = 128

    def write(worker):
        with closing(Session(endpoint, timeout="60s")) as session:
            session._http_session.trust_env = False
            barrier.wait(timeout=30)
            for row in range(worker, count * 4, 4):
                list(
                    session.execute(
                        f"CREATE (:Item {{id:{row},value:{expression(payload(row), property_type)}}})",
                        access_mode="insert",
                    )
                )

    expected = {row: payload(row) for row in range(-1, count * 4)}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(write, range(4)))
        with closing(Session(endpoint, timeout="60s")) as session:
            session._http_session.trust_env = False
            before = list(
                session.execute("MATCH (n:Item) RETURN n.id, n.value ORDER BY n.id")
            )
            assert len(before) == len(expected)
            assert dict(before) == expected
    finally:
        db.stop_serving()
        db.close()
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    conn = db.connect()
    try:
        after = list(conn.execute("MATCH (n:Item) RETURN n.id, n.value ORDER BY n.id"))
        assert after == before
        assert dict(after) == expected
    finally:
        conn.close()
        db.close()


def test_edge_insert_capacity_growth_and_recovery(tmp_path):
    path = str(tmp_path / "edge_capacity")
    db = Database(path, max_thread_num=4, checkpoint_on_close=True)
    conn = db.connect()
    list(conn.execute("CREATE NODE TABLE Item(id INT64, PRIMARY KEY(id))"))
    list(
        conn.execute("CREATE REL TABLE Link(FROM Item TO Item, seq INT64, note STRING)")
    )
    list(conn.execute("CREATE (:Item {id:0}), (:Item {id:1})"))
    conn.close()
    db.close()
    db = Database(path, max_thread_num=4, checkpoint_on_close=False)
    endpoint = _start_capacity_growth_service(db)
    expected = [
        [i, f"batch-{i // 1000}-" + "x" * (50 if i < 10000 else 200)]
        for i in range(20000)
    ]
    try:
        with closing(Session(endpoint, timeout="60s")) as session:
            session._http_session.trust_env = False
            for start in range(0, 20000, 1000):
                list(
                    session.execute(
                        "MATCH (a:Item {id:0}), (b:Item {id:1}) "
                        "UNWIND CAST($ids, 'INT64[]') AS id "
                        "CREATE (a)-[:Link {seq:id,note:$note}]->(b)",
                        access_mode="insert",
                        parameters={
                            "ids": list(range(start, start + 1000)),
                            "note": f"batch-{start // 1000}-"
                            + "x" * (50 if start < 10000 else 200),
                        },
                    )
                )
            before = list(
                session.execute(
                    "MATCH ()-[r:Link]->() RETURN r.seq,r.note ORDER BY r.seq"
                )
            )
            assert before == expected
    finally:
        db.stop_serving()
        db.close()
    db = Database(path, checkpoint_on_close=False)
    conn = db.connect()
    try:
        assert (
            list(
                conn.execute("MATCH ()-[r:Link]->() RETURN r.seq,r.note ORDER BY r.seq")
            )
            == expected
        )
    finally:
        conn.close()
        db.close()
