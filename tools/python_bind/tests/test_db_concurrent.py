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
import random
import shutil
import sys
import time

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
import threading

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
