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

import logging
import shutil

import pytest

from neug.database import Database
from neug.session import Session

logger = logging.getLogger(__name__)


def test_checkpoint(tmp_path):
    db_dir = str(tmp_path / "test_checkpoint")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT32, name STRING, PRIMARY KEY(id))")
    conn.execute("CREATE (p:Person {id: 1, name: 'Alice'});")
    conn.execute("CREATE (p:Person {id: 2, name: 'Bob'});")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)")
    conn.execute("CREATE REL TABLE Likes(FROM Person TO Person, weight DOUBLE)")
    conn.execute(
        "CREATE REL TABLE Visits(FROM Person TO Person, time STRING, location STRING)"
    )
    conn.execute(
        "MATCH (p1:Person), (p2:Person)  WHERE p1.id = 1 AND p2.id = 2 CREATE (p1)-[:Knows]->(p2);"
    )
    conn.execute(
        "MATCH (p1:Person), (p2:Person)  WHERE p1.id = 1 AND p2.id = 2 CREATE (p1)-[:Likes {weight: 0.5}]->(p2);"
    )
    conn.execute(
        "MATCH (p1:Person), (p2:Person)  WHERE p1.id = 2 AND p2.id = 1 "
        " CREATE (p1)-[:Visits {time: '2024-01-01', location: 'NYC'}]->(p2);"
    )
    res = conn.execute("MATCH (p1:Person)-[k:Knows]->(p2:Person) RETURN p1.id, p2.id;")
    records = list(res)
    assert records == [[1, 2]]
    res = conn.execute(
        "MATCH (p1:Person)-[k:Likes]->(p2:Person) RETURN p1.id, p2.id, k.weight;"
    )
    records = list(res)
    assert records == [[1, 2, 0.5]]
    res = conn.execute(
        "MATCH (p1:Person)-[k:Visits]->(p2:Person) RETURN p1.id, p2.id, k.time, k.location;"
    )
    records = list(res)
    assert records == [[2, 1, "2024-01-01", "NYC"]]
    conn.execute("CHECKPOINT;")
    res = conn.execute("MATCH (p:Person) RETURN p.id, p.name;")
    records = list(res)
    assert records == [[1, "Alice"], [2, "Bob"]]
    res = conn.execute("MATCH (p1:Person)-[k:Knows]->(p2:Person) RETURN p1.id, p2.id;")
    records = list(res)
    assert records == [[1, 2]]
    conn.close()
    db.close()


def test_recreate_vertex(tmp_path):
    db_dir = str(tmp_path / "test_recreate_vertex")
    logger.info("Starting test_recreate_vertex")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE (p: Person {id: 111});")
    conn.execute("CREATE (p: Person {id: 222});")
    conn.execute("DROP TABLE IF EXISTS Person;")

    conn.execute("CREATE NODE TABLE Person(id INT64, age INT32, PRIMARY KEY(id));")
    val = conn.execute("MATCH (n) return count(n);")
    records = list(val)
    assert records == [[0]], f"Expected value [[0]], got {records}"
    conn.execute("CREATE (p: Person {id: 111, age: 20});")
    res = conn.execute("MATCH (p: Person) RETURN p.id, p.age;")
    records = list(res)
    assert records == [[111, 20]]
    logger.info("test_recreate_vertex passed first part")

    conn.execute("DROP TABLE IF EXISTS Person;")
    conn.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute("CREATE (p: Person {id: 111, name: 'Alice'});")
    res = conn.execute("MATCH (p: Person) RETURN p.id, p.name;")
    records = list(res)
    assert records == [[111, "Alice"]]
    logger.info("test_recreate_vertex passed second part")
    conn.close()
    db.close()


def test_recreate_edge(tmp_path):
    db_dir = str(tmp_path / "test_recreate_edge")
    logger.info("Starting test_recreate_edge")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, id INT64);")
    conn.execute("CREATE (p: Person {id: 111});")
    conn.execute("CREATE (p: Person {id: 222});")
    conn.execute(
        "MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 333}]->(p2);"
    )
    conn.execute("DROP TABLE IF EXISTS Knows;")

    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weight DOUBLE);")
    val = conn.execute("MATCH ()-[e:Knows]->() return count(e);")
    records = list(val)
    assert records == [[0]], f"Expected value [[0]], got {records}"
    conn.execute(
        "MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {weight: 1.5}]->(p2);"
    )
    res = conn.execute("MATCH (p1: Person)-[k: Knows]->(p2:Person) RETURN k.weight;")
    records = list(res)
    assert records == [[1.5]]
    logger.info("test_recreate_edge passed first part")

    conn.execute("DROP TABLE IF EXISTS Knows;")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, id INT64);")
    conn.execute(
        "MATCH (p1: Person {id: 111}), (p2: Person {id: 222}) CREATE (p1)-[k:Knows {id: 444}]->(p2);"
    )
    res = conn.execute("MATCH (p1: Person)-[k: Knows]->(p2:Person) RETURN k.id;")
    records = list(res)
    assert records == [[444]]
    logger.info("test_recreate_edge passed second part")
    conn.close()
    db.close()


def test_insert_string_column_exhaustion(tmp_path):
    logging.disable(logging.CRITICAL)
    try:
        db_dir = str(tmp_path / "test_insert_string_column_exhaustion")
        shutil.rmtree(db_dir, ignore_errors=True)
        db = Database(db_path=db_dir, mode="w")
        conn = db.connect()
        conn.execute(
            "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));"
        )
        # by default the string column has maximum length 256
        conn.execute("CREATE (p: Person {id: 1, name: 'a'});")
        conn.execute("CREATE (p: Person {id: 2, name: 'b'});")
        conn.execute("CHECKPOINT;")
        conn.close()
        db.close()

        db2 = Database(db_path=db_dir, mode="w")
        conn2 = db2.connect()
        str_prop = "a" * 255
        for i in range(10000):
            conn2.execute(f"CREATE (p: Person {{id: {i+3}, name: '{str_prop}'}});")
        res = conn2.execute("MATCH (p: Person) RETURN count(p);")
        records = list(res)
        assert records == [[10002]], f"Expected value [[10002]], got {records}"
        conn2.close()
        db2.close()

        db3 = Database(db_path=db_dir, mode="w")
        conn3 = db3.connect()
        conn3.execute("CREATE REL TABLE Knows(FROM Person TO Person, note STRING);")
        conn3.execute(
            "MATCH (a: Person), (b: Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:Knows {note: '12'}]->(b);"
        )
        conn3.execute(
            "MATCH (a: Person), (b: Person) WHERE a.id = 3 AND b.id = 4 CREATE (a)-[:Knows {note: '34'}]->(b);"
        )
        conn3.execute("CHECKPOINT;")
        db3.close()

        db4 = Database(db_path=db_dir, mode="w")
        conn4 = db4.connect()
        res4 = conn4.execute(
            "MATCH (a: Person)-[k: Knows]->(b: Person) RETURN k.note ORDER BY k.note;"
        )
        records = list(res4)
        assert records == [
            ["12"],
            ["34"],
        ], f"Expected value [['12'], ['34']], got {records}"
        str_prop = "a" * 255
        for i in range(100):
            conn4.execute(
                f"MATCH (a: Person {{id: 1}}), (b: Person {{id: 2}}) CREATE (a)-[:Knows {{note: '{str_prop}'}}]->(b);"
            )
        conn4.close()
        db4.close()
    except Exception as e:
        raise AssertionError(f"Test failed with exception: {e}")
    finally:
        logging.disable(logging.NOTSET)


def test_sort_csr_compact(tmp_path):
    db_dir = tmp_path / "test_sort_csr_compact"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="rw")
    conn = db.connect()
    conn.execute("""CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id))""")
    conn.execute(
        """CREATE REL TABLE Knows(FROM Person TO Person, since INT64) WITH (sort_key_for_nbr='since')"""
    )

    for i in range(100):
        conn.execute(f"CREATE (:Person {{id: {i}}});")
    for i in range(100):
        if i % 2 == 0:
            conn.execute(
                f"MATCH (a:Person {{id: 1}}), (b:Person {{id: {i}}}) CREATE (a)-[:Knows {{since: {i}}}]->(b:Person);"
            )
        else:
            conn.execute(
                f"MATCH (a:Person {{id: 0}}), (b:Person {{id: {i}}}) CREATE (a)-[:Knows {{since: {i}}}]->(b);"
            )
    conn.close()
    db.close()

    db = Database(db_path=str(db_dir), mode="rw")
    endpoint = db.serve(host="127.0.0.1", port=10010, blocking=False)
    sess = Session.open(endpoint=endpoint, timeout="30s", num_threads=5)
    sess.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 98}) CREATE (a)-[:Knows {since: 1}]->(b);"
    )
    sess.execute(
        "MATCH (a:Person {id: 0}), (b:Person {id: 1}) CREATE (a)-[:Knows {since: 100}]->(b);"
    )
    res = sess.execute(
        query="MATCH (a: Person {id: 1})-[r:Knows]-> (b: Person) WHERE r.since < $since RETURN b.id, r.since",
        parameters={"since": 2},
    )
    assert list(res) == [[0, 0], [98, 1]]
    res = sess.execute(
        query="MATCH (a: Person {id: 0})-[r:Knows]-> (b: Person) WHERE r.since > $since RETURN b.id, r.since",
        parameters={"since": 99},
    )
    assert list(res) == [[1, 100]]
    sess.close()
    db.close()


@pytest.mark.skip(reason="TODO(zhanglei,lexiao): get view from invalid vid")
def test_join_queries(modern_graph):
    conn = modern_graph
    res = conn.execute(
        """
        MATCH (a:person), (b:person) WHERE a.ID = b.ID AND a.ID = 1 RETURN a.id, b.id, a.age, b.age;
        """
    )
    assert res.__next__() == [1, 1, 29, 29]

    res = conn.execute(
        """
        MATCH (a:person) WHERE a.name = 'marko' OPTIONAL MATCH (b:person) WHERE b.name = 'm' RETURN a.ID, b.ID;
        """
    )
    assert res.__next__() == [1, None]
