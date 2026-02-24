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

import datetime
import logging
import os
import shutil
import sys
import time

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database
from neug.session import Session

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def setup_database():
    db_dir = "/tmp/test_batch_loading"
    if os.path.exists(db_dir):
        os.system("rm -rf %s" % db_dir)
    os.makedirs(db_dir)
    db = Database(db_dir, "w")

    uri = db.serve(10000, "localhost", False)
    logger.info(f"Database server started at {uri}")

    yield db, uri
    db.stop_serving()


@pytest.mark.skip(reason="batch loading is not supported under tp mode")
def test_batch_loading(setup_database):
    db, uri = setup_database
    time.sleep(1)  # Wait for the server to start
    logger.info("[test_batch_loading]")
    flex_data_dir = os.environ.get("FLEX_DATA_DIR")
    if not flex_data_dir:
        raise Exception("FLEX_DATA_DIR is not set")
    person_csv = os.path.join(flex_data_dir, "person.csv")
    person_knows_person_csv = os.path.join(
        flex_data_dir, "person_knows_person.part*.csv"
    )
    sess = Session(uri, timeout="10s")
    sess.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    sess.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    sess.execute(f'COPY person from "{person_csv}"')
    sess.execute(
        f'COPY knows from "{person_knows_person_csv}" (from="person", to="person")'
    )

    res = sess.execute("MATCH (n) WHERE n.id = 1 RETURN n.name;")
    assert len(res) == 1
    assert res[0][0] == "marko"

    res = sess.execute("MATCH (n:person)-[e:knows]->(:person) WHERE n.id = 1 RETURN e;")
    assert len(res) == 2

    # get service status
    status = sess.service_status()
    logger.info(f"Service status: {status}")
    assert status["status"] == "OK"


def test_start_service_on_pure_memory_db():
    db = Database("", "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    conn.execute("CREATE (p:person {id: 1, name: 'marko', age: 29});")
    conn.execute("CREATE (p:person {id: 2, name: 'vadas', age: 27});")
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:knows {weight: 0.5}]->(b);"
    )
    conn.close()
    uri = db.serve(10010, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    res = session.execute("MATCH (n) WHERE n.id = 1 RETURN n.name;")
    assert len(res) == 1
    assert res[0][0] == "marko"

    db.stop_serving()
    db.close()


def test_start_serving_and_dump():
    db_dir = "/tmp/test_start_serving_and_dump"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    conn.execute("CREATE (p:person {id: 1, name: 'marko', age: 29});")
    conn.execute("CREATE (p:person {id: 2, name: 'vadas', age: 27});")
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:knows {weight: 0.5}]->(b);"
    )
    conn.close()
    db.close()

    db2 = Database(db_dir, "r")
    uri = db2.serve(10002, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    res = session.execute("MATCH (n) WHERE n.id = 1 RETURN n.name;")
    assert len(res) == 1
    assert res[0][0] == "marko"
    db2.stop_serving()
    db2.close()


def test_start_service_and_stop():
    db_dir = "/tmp/test_start_service_and_stop"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    conn.execute("CREATE (p:person {id: 1, name: 'marko', age: 29});")
    conn.execute("CREATE (p:person {id: 2, name: 'vadas', age: 27});")
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:knows {weight: 0.5}]->(b);"
    )
    conn.close()
    uri = db.serve(10002, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    res = session.execute("MATCH (n) WHERE n.id = 1 RETURN n.name;")
    assert len(res) == 1
    assert res[0][0] == "marko"
    db.stop_serving()

    # Now do something with new connection
    conn = db.connect()
    res = conn.execute("CREATE (p:person {id: 3, name: 'josh', age: 32});")
    res = conn.execute("MATCH (n) WHERE n.id = 3 RETURN n.name;")
    assert len(res) == 1
    assert res[0][0] == "josh"
    conn.close()

    db.serve(10002, "localhost", False)
    time.sleep(1)
    session = Session(uri, timeout="10s")
    res = session.execute("MATCH (n) WHERE n.id = 3 RETURN n.name;")
    assert len(res) == 1
    assert res[0][0] == "josh"
    db.stop_serving()
    db.close()


def test_access_mode_validation():
    from neug.utils import is_access_mode_valid
    from neug.utils import valid_access_modes

    for mode in valid_access_modes:
        assert is_access_mode_valid(mode) is True

    invalid_modes = ["readwrite", "write", "delete", "modify", "execute", "rwx", ""]
    for mode in invalid_modes:
        assert is_access_mode_valid(mode) is False


def test_readable_function():
    from neug.utils import readable

    assert readable("r") == "read-only"
    assert readable("read") == "read-only"
    assert readable("read-only") == "read-only"
    assert readable("read_only") == "read-only"

    assert readable("w") == "read-write"
    assert readable("rw") == "read-write"
    assert readable("write") == "read-write"
    assert readable("readwrite") == "read-write"
    assert readable("read-write") == "read-write"
    assert readable("read_write") == "read-write"

    invalid_modes = ["delete", "modify", "execute", "rwx", ""]
    for mode in invalid_modes:
        try:
            readable(mode)
            assert False, f"Expected ValueError for mode: {mode}"
        except ValueError:
            pass


def test_invalid_access_mode_in_session():
    db_dir = "/tmp/test_invalid_access_mode_in_session"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.close()
    uri = db.serve(10003, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    with pytest.raises(ValueError):
        session.execute("MATCH (n) RETURN n;", access_mode="readwrite")
    with pytest.raises(ValueError):
        session.execute("MATCH (n) RETURN n;", access_mode="delete")
    # correct access mode
    session.execute("MATCH (n) RETURN n;", access_mode="r")
    with pytest.raises(ValueError):
        session.execute(
            "CREATE (p:person {id: 1, name: 'marko', age: 29});",
            access_mode="read-only",
        )
    session.execute(
        "CREATE (p:person {id: 1, name: 'marko', age: 29});", access_mode="insert"
    )
    # TODO(xiaolei,zhanglei): Support insert access mode.
    # with pytest.raises(ValueError):
    #    session.execute("MATCH (n: person) WHERE n.id = 1 SET n.age = 30;", access_mode="insert")
    # with pytest.raises(ValueError):
    # session.execute("MATCH (n: person) WHERE n.id = 1 SET n.age = 30;", access_mode="read")
    session.execute(
        "MATCH (n: person) WHERE n.id = 1 SET n.age = 30;", access_mode="update"
    )
    session.close()

    db.stop_serving()
    db.close()


def test_delete_vertices():
    db_dir = "/tmp/test_delete_vertices"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    uri = db.serve(10004, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    session.execute("CREATE NODE TABLE Person(id INT64, name INT64, PRIMARY KEY (id))")
    session.execute("MATCH (n:Person) DELETE n;")

    session.execute("Create (n:Person {id: 111111, name: 6666})")
    session.execute("MATCH (n:Person {id: 111111}) DELETE n;")

    db.stop_serving()
    db.close()


def test_delete_edges():
    db_dir = "/tmp/test_delete_edges"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    uri = db.serve(10005, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    session.execute("CREATE NODE TABLE Person(id INT64, name INT64, PRIMARY KEY (id))")
    session.execute("CREATE REL TABLE Knows(FROM Person TO Person)")

    session.execute("Create (a:Person {id: 1, name: 1111})")
    session.execute("Create (b:Person {id: 2, name: 2222})")
    session.execute("Create (c:Person {id: 3, name: 3333})")
    session.execute("Create (d:Person {id: 4, name: 4444})")
    session.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) CREATE (a)-[:Knows]->(b)"
    )
    session.execute(
        "MATCH (a:Person {id: 1}), (c:Person {id: 3}) CREATE (a)-[:Knows]->(c)"
    )
    session.execute(
        "MATCH (a:Person {id: 2}), (d:Person {id: 4}) CREATE (a)-[:Knows]->(d)"
    )
    session.execute("MATCH (a:Person {id: 1})-[e:Knows]->(b : Person{id: 2}) DELETE e;")
    session.execute("MATCH (a:Person {id: 1})-[e:Knows]->(c : Person{id: 3}) DELETE e;")
    session.execute("MATCH (a:Person {id: 1}) DELETE a;")

    result = session.execute("MATCH (n:Person) RETURN n;")
    assert len(result) == 3
    result = session.execute("MATCH ()-[e:Knows]->() RETURN e;")
    assert len(result) == 1
    session.execute("MATCH (n:Person {id: 4}) DELETE n;")
    result = session.execute("MATCH ()-[e:Knows]->() RETURN e;")
    assert len(result) == 0

    db.stop_serving()
    db.close()


def test_parameterized_query():
    db_dir = "/tmp/test_parameterized_query"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE param_values("
        "id INT32, bool_prop BOOL, date_prop Date, timestamp_prop Timestamp, "
        "int32_prop INT32, int64_prop INT64, uint32_prop UINT32, uint64_prop UINT64, "
        "float_prop FLOAT, double_prop DOUBLE, string_prop STRING, "
        "PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE (p:param_values {"
        "id: 1, bool_prop: true, date_prop: date('2024-01-01'), "
        "timestamp_prop: Timestamp('2024-01-02 03:04:05'), "
        "int32_prop: 42, int64_prop: 1234567890123, "
        "uint32_prop: 123, uint64_prop: 456, float_prop: 3.14, "
        "double_prop: 6.28, string_prop: 'parameterized'"
        "});"
    )

    res = conn.execute("MATCH (n:param_values) RETURN n;")
    print(list(res))
    conn.close()
    uri = db.serve(10004, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    cases = [
        (
            "MATCH (n:param_values) WHERE n.bool_prop = $value RETURN n.bool_prop;",
            {"value": True},
            True,
        ),
        (
            "MATCH (n:param_values) WHERE n.int32_prop = $value RETURN n.int32_prop;",
            {"value": 42},
            42,
        ),
        (
            "MATCH (n:param_values) WHERE n.int64_prop = $value RETURN n.int64_prop;",
            {"value": 1234567890123},
            1234567890123,
        ),
        (
            "MATCH (n:param_values) WHERE n.uint32_prop = $value RETURN n.uint32_prop;",
            {"value": 123},
            123,
        ),
        (
            "MATCH (n:param_values) WHERE n.uint64_prop = $value RETURN n.uint64_prop;",
            {"value": 456},
            456,
        ),
        (
            "MATCH (n:param_values) WHERE n.float_prop = $value RETURN n.float_prop;",
            {"value": 3.14},
            pytest.approx(3.14),
        ),
        (
            "MATCH (n:param_values) WHERE n.double_prop = $value RETURN n.double_prop;",
            {"value": 6.28},
            pytest.approx(6.28),
        ),
        (
            "MATCH (n:param_values) WHERE n.string_prop = $value RETURN n.string_prop;",
            {"value": "parameterized"},
            "parameterized",
        ),
        (
            "MATCH (n:param_values) WHERE n.date_prop = $value RETURN n.date_prop;",
            {"value": datetime.date(2024, 1, 1)},
            "2024-01-01",
        ),
        (
            "MATCH (n:param_values) WHERE n.timestamp_prop <> $value RETURN n.timestamp_prop;",
            {"value": "2024-01-02 03:04:05"},
            "2024-01-02 03:04:05.000",
        ),
    ]

    for query, params, expected in cases:
        res = session.execute(query, parameters=params)
        assert len(res) == 1, f"Failed for query: {query} with params: {params}"
        assert res[0][0] == expected

    session.execute(
        "CREATE (p:param_values {"
        "id: 2, bool_prop: true, date_prop: date('2024-01-01'), "
        "timestamp_prop: Timestamp('2024-01-02 03:04:05'), "
        "int32_prop: 42, int64_prop: 1234567890123, "
        "uint32_prop: 123, uint64_prop: 456, float_prop: 3.14, "
        "double_prop: 7.28, string_prop: 'parameterized'"
        "});"
    )

    list_values = [1, 2]
    list_res = list(
        session.execute(
            "MATCH(a: param_values) WHERE a.id IN CAST($ids, 'INT32[]') return a.double_prop;",
            parameters={"ids": list_values},
        )
    )
    assert list_res == [[6.28], [7.28]]

    session.close()
    db.stop_serving()
    db.close()
