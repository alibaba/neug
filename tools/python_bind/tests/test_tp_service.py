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

    uri = db.serve(10000, "localhost")
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
    person_knows_person_csv_part1 = os.path.join(
        flex_data_dir, "person_knows_person.part1.csv"
    )
    person_knows_person_csv_part2 = os.path.join(
        flex_data_dir, "person_knows_person.part2.csv"
    )
    sess = Session(uri, timeout="10s")
    sess.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    sess.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    sess.execute(f'COPY person from "{person_csv}"')
    sess.execute(
        f'COPY knows from "{person_knows_person_csv_part1}" (from="person", to="person")'
    )
    sess.execute(
        f'COPY knows from "{person_knows_person_csv_part2}" (from="person", to="person")'
    )

    res = sess.execute("MATCH (n) WHERE n.id = 1 RETURN n.name;")
    assert len(res) == 1
    assert res[0][0] == "marko"

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
    uri = db.serve(10001, "localhost")
    time.sleep(1)

    session = Session(uri, timeout="10s")
    res = session.execute("MATCH (n) WHERE n.id = 1 RETURN n.name;")
    assert len(res) == 1
    assert res[0][0] == "marko"

    db.stop_serving()
    db.close()
