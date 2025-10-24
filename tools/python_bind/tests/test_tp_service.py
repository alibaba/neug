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


# @pytest.mark.skip(reason="https://github.com/GraphScope/neug/issues/846")
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
    db.stop_serving()
    db.close()
    # session = Session(uri, timeout="10s")
    # res = session.execute("MATCH (n) WHERE n.id = 3 RETURN n.name;")
    # assert len(res) == 1
    # assert res[0][0] == "josh"
    # db.stop_serving()
    # db.close()
