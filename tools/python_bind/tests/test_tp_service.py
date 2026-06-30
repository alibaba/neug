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
from conftest import wait_for_server_ready

from neug.database import Database
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_SCHEMA_MISMATCH
from neug.session import Session

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def setup_database(tmp_path_factory):
    db_dir = str(tmp_path_factory.mkdtemp("test_batch_loading"))
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
        flex_data_dir, "test_data/person_knows_person.part*.csv"
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


def test_start_serving_and_dump(tmp_path):
    db_dir = str(tmp_path / "test_start_serving_and_dump")
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


def test_start_service_and_stop(tmp_path):
    db_dir = str(tmp_path / "test_start_service_and_stop")
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


def test_invalid_access_mode_in_session(tmp_path):
    db_dir = str(tmp_path / "test_invalid_access_mode_in_session")
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


def test_delete_vertices(tmp_path):
    db_dir = str(tmp_path / "test_delete_vertices")
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


def test_delete_edges(tmp_path):
    db_dir = str(tmp_path / "test_delete_edges")
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


def test_merge_vertex(tmp_path):
    db_dir = str(tmp_path / "test_merge_vertex")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    uri = db.serve(10013, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    session.execute(
        "CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY(name));"
    )
    session.execute("CREATE (:User {name: 'Adam', age: 29});")

    existing = list(session.execute("MERGE (u:User {name: 'Adam'}) RETURN u.age;"))
    assert existing == [[29]]

    created = list(
        session.execute("MERGE (u:User {name: 'Bob', age: 45}) RETURN u.name, u.age;")
    )
    assert created == [["Bob", 45]]

    names = sorted(r[0] for r in session.execute("MATCH (u:User) RETURN u.name;"))
    assert names == ["Adam", "Bob"]

    session.close()
    db.stop_serving()
    db.close()


def test_merge_edge(tmp_path):
    db_dir = str(tmp_path / "test_merge_edge")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    uri = db.serve(10014, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    session.execute(
        "CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY(name));"
    )
    session.execute("CREATE REL TABLE follows(FROM User TO User, date INT64);")
    session.execute("CREATE (:User {name: 'Adam', age: 29});")
    session.execute("CREATE (:User {name: 'marko', age: 32});")
    session.execute("CREATE (:User {name: 'Bob', age: 40});")
    session.execute(
        "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'marko'}) "
        "CREATE (u1)-[:follows {date: 2012}]->(u2);"
    )

    matched = list(
        session.execute(
            "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'marko'}) "
            "MERGE (u1)-[e:follows {date: 2012}]->(u2) "
            "RETURN u1.name, e.date, u2.name;"
        )
    )
    assert matched == [["Adam", 2012, "marko"]]

    created = list(
        session.execute(
            "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'Bob'}) "
            "MERGE (u1)-[e:follows {date: 2012}]->(u2) "
            "RETURN u1.name, e.date, u2.name;"
        )
    )
    assert created == [["Adam", 2012, "Bob"]]

    edge_count = list(session.execute("MATCH ()-[e:follows]->() RETURN count(e);"))
    assert edge_count == [[2]]

    session.close()
    db.stop_serving()
    db.close()


def test_query_cache(tmp_path):
    db_dir = str(tmp_path / "test_query_cache")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.close()
    uri = db.serve(10004, "localhost", False)
    time.sleep(1)
    session = Session(uri, timeout="10s")
    session.execute("CREATE (p:person {id: 1, name: 'marko', age: 29});")
    for _ in range(10):
        session.execute("MATCH (n) return n;")
    # expect the query has been cached for most sessions
    session.execute("ALTER TABLE person ADD email STRING;")
    session.execute("ALTER TABLE person DROP age;")
    for _ in range(10):
        session.execute("MATCH (n) return n;")
    with pytest.raises(Exception):
        session.execute("MATCH (n) RETURN n.age;")
    session.close()
    db.stop_serving()
    db.close()


def test_parameterized_query(tmp_path):
    db_dir = str(tmp_path / "test_parameterized_query")
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
            datetime.date(2024, 1, 1),
        ),
        (
            "MATCH (n:param_values) WHERE n.timestamp_prop = $value RETURN n.timestamp_prop;",
            {"value": "2024-01-02 03:04:05"},
            datetime.datetime(2024, 1, 2, 3, 4, 5),
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

    conn = db.connect()
    for query, params, expected in cases:
        res = conn.execute(query, parameters=params)
        assert len(res) == 1, f"Failed for query: {query} with params: {params}"
        assert res[0][0] == expected
    conn.close()
    db.close()


def test_iu_1(tmp_path):
    """
    Test IU_1: Create PERSON with connections to PLACE, TAG, ORGANISATION
    Corresponds to FlagTest.IU_1 in flag_test.cpp
    """
    db_dir = str(tmp_path / "test_iu_1")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()

    # Create schema
    conn.execute(
        "CREATE NODE TABLE PLACE(id INT64, name STRING, url STRING, type STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, firstName STRING, lastName STRING, gender STRING, "
        "birthday DATE, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, "
        "language STRING, email STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE TAG(id INT64, name STRING, url STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE ORGANISATION(id INT64, type STRING, name STRING, url STRING, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE ISLOCATEDIN(FROM PERSON TO PLACE);")
    conn.execute("CREATE REL TABLE HASINTEREST(FROM PERSON TO TAG);")
    conn.execute(
        "CREATE REL TABLE STUDYAT(FROM PERSON TO ORGANISATION, classYear INT32);"
    )
    conn.execute(
        "CREATE REL TABLE WORKAT(FROM PERSON TO ORGANISATION, workFrom INT32);"
    )

    # Create prerequisite data
    conn.execute("CREATE (c:PLACE {id: 1, name: 'City1', url: 'url1', type: 'City'});")
    conn.execute("CREATE (t1:TAG {id: 10, name: 'Tag1', url: 'tag1'});")
    conn.execute("CREATE (t2:TAG {id: 11, name: 'Tag2', url: 'tag2'});")
    conn.execute(
        "CREATE (o1:ORGANISATION {id: 100, type: 'University', name: 'Univ1', url: 'univ1'});"
    )
    conn.execute(
        "CREATE (o2:ORGANISATION {id: 101, type: 'Company', name: 'Comp1', url: 'comp1'});"
    )
    conn.close()

    uri = db.serve(10009, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")

    # Execute IU_1 query with parameters
    query = """
    MATCH (c:PLACE {id: $cityId})
    CREATE (p:PERSON {
      id: $personId,
      firstName: $personFirstName,
      lastName: $personLastName,
      gender: $gender,
      birthday: $birthday,
      creationDate: $creationDate,
      locationIP: $locationIP,
      browserUsed: $browserUsed,
      language: $languages,
      email: $emails
    })-[:ISLOCATEDIN]->(c)
    WITH distinct p
    UNWIND CAST($tagIds, 'INT64[]') AS tagId
    MATCH (t:TAG {id: tagId})
    CREATE (p)-[:HASINTEREST]->(t)
    WITH distinct p
    UNWIND CAST($studyAt, 'INT64[][]') AS studyAt
    WITH p, studyAt[0] as studyAt_0, CAST(studyAt[1], 'INT32') as studyAt_1
    MATCH(u:ORGANISATION {id: studyAt_0})
    CREATE (p)-[:STUDYAT {classYear:studyAt_1}]->(u)
    WITH distinct p
    UNWIND CAST($workAt, 'INT64[][]') AS workAt
    WITH p, workAt[0] as workAt_0, CAST(workAt[1], 'INT32') as workAt_1
    MATCH(comp:ORGANISATION {id: workAt_0})
    CREATE (p)-[:WORKAT {workFrom: workAt_1}]->(comp)
    """

    import datetime

    session.execute(
        query,
        parameters={
            "cityId": 1,
            "personId": 1000,
            "personFirstName": "John",
            "personLastName": "Doe",
            "gender": "male",
            "birthday": datetime.date(1990, 1, 1),
            "creationDate": "2024-01-01 00:00:00",
            "locationIP": "192.168.1.1",
            "browserUsed": "Chrome",
            "languages": "en",
            "emails": "john@example.com",
            "tagIds": [10, 11],
            "studyAt": [[100, 2020]],
            "workAt": [[101, 2022]],
        },
        access_mode="insert",  # ensure the query is executed in insert mode
    )

    # Verify the person was created
    result = session.execute(
        "MATCH (p:PERSON {id: 1000}) RETURN p.firstName, p.lastName;"
    )
    assert len(result) == 1
    assert result[0][0] == "John"
    assert result[0][1] == "Doe"

    # Verify relationships
    result = session.execute(
        "MATCH (p:PERSON {id: 1000})-[:ISLOCATEDIN]->(c:PLACE) RETURN c.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 1

    result = session.execute(
        "MATCH (p:PERSON {id: 1000})-[:HASINTEREST]->(t:TAG) RETURN count(t) as cnt;"
    )
    assert len(result) == 1
    assert result[0][0] == 2

    result = session.execute(
        "MATCH (p:PERSON {id: 1000})-[:STUDYAT]->(o:ORGANISATION) RETURN o.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 100

    result = session.execute(
        "MATCH (p:PERSON {id: 1000})-[:WORKAT]->(o:ORGANISATION) RETURN o.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 101

    session.close()
    db.stop_serving()
    db.close()


def test_iu_4(tmp_path):
    """
    Test IU_4: Create FORUM with connections to PERSON and TAG
    Corresponds to FlagTest.IU_4 in flag_test.cpp
    """
    db_dir = str(tmp_path / "test_iu_4")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()

    # Create schema
    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, firstName STRING, lastName STRING, gender STRING, "
        "birthday DATE, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, "
        "language STRING, email STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE FORUM(id INT64, title STRING, creationDate TIMESTAMP, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE TAG(id INT64, name STRING, url STRING, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE HASMODERATOR(FROM FORUM TO PERSON);")
    conn.execute("CREATE REL TABLE HASTAG(FROM FORUM TO TAG);")

    # Create prerequisite data
    conn.execute(
        "CREATE (p:PERSON {id: 1, firstName: 'Moderator', lastName: 'User', gender: 'male', "
        "birthday: date('1990-01-01'), creationDate: timestamp('2024-01-01 00:00:00'), "
        "locationIP: '192.168.1.1', browserUsed: 'Chrome', language: 'en', email: 'mod@example.com'});"
    )
    conn.execute("CREATE (t1:TAG {id: 10, name: 'Tag1', url: 'tag1'});")
    conn.execute("CREATE (t2:TAG {id: 11, name: 'Tag2', url: 'tag2'});")
    conn.close()

    uri = db.serve(10010, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")

    # Execute IU_4 query with parameters
    query = """
    MATCH (p:PERSON {id: $moderatorPersonId})
    CREATE (f:FORUM {id: $forumId, title: $forumTitle, creationDate: $creationDate})-[:HASMODERATOR]->(p)
    WITH f
    UNWIND CAST($tagIds, 'INT64[]') AS tagId
    MATCH (t:TAG {id: tagId} )
    CREATE (f)-[:HASTAG]->(t)
    """

    session.execute(
        query,
        parameters={
            "moderatorPersonId": 1,
            "forumId": 100,
            "forumTitle": "Test Forum",
            "creationDate": "2024-01-01 00:00:00",
            "tagIds": [10, 11],
        },
        access_mode="insert",  # ensure the query is executed in insert mode
    )

    # Verify the forum was created
    result = session.execute("MATCH (f:FORUM {id: 100}) RETURN f.title;")
    assert len(result) == 1
    assert result[0][0] == "Test Forum"

    # Verify relationships
    result = session.execute(
        "MATCH (f:FORUM {id: 100})-[:HASMODERATOR]->(p:PERSON) RETURN p.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 1

    result = session.execute(
        "MATCH (f:FORUM {id: 100})-[:HASTAG]->(t:TAG) RETURN count(t) as cnt;"
    )
    assert len(result) == 1
    assert result[0][0] == 2

    session.close()
    db.stop_serving()
    db.close()


def test_iu_6(tmp_path):
    """
    Test IU_6: Create POST with connections to PERSON, PLACE, FORUM, TAG
    Corresponds to FlagTest.IU_6 in flag_test.cpp
    """
    db_dir = str(tmp_path / "test_iu_6")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()

    # Create schema
    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, firstName STRING, lastName STRING, gender STRING, "
        "birthday DATE, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, "
        "language STRING, email STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE PLACE(id INT64, name STRING, url STRING, type STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE FORUM(id INT64, title STRING, creationDate TIMESTAMP, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE POST(id INT64, imageFile STRING, creationDate TIMESTAMP, locationIP STRING, "
        "browserUsed STRING, language STRING, content STRING, length INT32, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE TAG(id INT64, name STRING, url STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE REL TABLE HASCREATOR(FROM POST TO PERSON, creationDate TIMESTAMP);"
    )
    conn.execute("CREATE REL TABLE CONTAINEROF(FROM FORUM TO POST);")
    conn.execute("CREATE REL TABLE ISLOCATEDIN(FROM POST TO PLACE);")
    conn.execute("CREATE REL TABLE HASTAG(FROM POST TO TAG);")

    # Create prerequisite data
    conn.execute(
        "CREATE (p:PERSON {id: 1, firstName: 'Author', lastName: 'User', gender: 'male', "
        "birthday: date('1990-01-01'), creationDate: timestamp('2024-01-01 00:00:00'), "
        "locationIP: '192.168.1.1', browserUsed: 'Chrome', language: 'en', email: 'author@example.com'});"
    )
    conn.execute(
        "CREATE (country:PLACE {id: 1, name: 'Country1', url: 'country1', type: 'Country'});"
    )
    conn.execute(
        "CREATE (f:FORUM {id: 1, title: 'Forum1', creationDate: timestamp('2024-01-01 00:00:00')});"
    )
    conn.execute("CREATE (t1:TAG {id: 10, name: 'Tag1', url: 'tag1'});")
    conn.execute("CREATE (t2:TAG {id: 11, name: 'Tag2', url: 'tag2'});")
    conn.close()

    uri = db.serve(10011, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")

    # Execute IU_6 query with parameters
    query = """
    MATCH (author:PERSON {id: $authorPersonId}), (country:PLACE {id: $countryId}), (forum:FORUM {id: $forumId})
    CREATE (author)<-[:HASCREATOR {creationDate: $creationDate}]-(p:POST {
        id: $postId,
        creationDate: $creationDate,
        locationIP: $locationIP,
        browserUsed: $browserUsed,
        language: $language,
        content: $content,
        imageFile: $imageFile,
        length: $length
      })<-[:CONTAINEROF]-(forum), (p)-[:ISLOCATEDIN]->(country)
    WITH p
    UNWIND CAST($tagIds, 'INT64[]') AS tagId
    MATCH (t:TAG {id: tagId})
    CREATE (p)-[:HASTAG]->(t)
    """

    session.execute(
        query,
        parameters={
            "authorPersonId": 1,
            "countryId": 1,
            "forumId": 1,
            "creationDate": "2024-01-01 00:00:00",
            "postId": 1000,
            "locationIP": "192.168.1.1",
            "browserUsed": "Chrome",
            "language": "en",
            "content": "Test post content",
            "imageFile": "image.jpg",
            "length": 100,
            "tagIds": [10, 11],
        },
        access_mode="insert",  # ensure the query is executed in insert mode
    )

    # Verify the post was created
    result = session.execute("MATCH (p:POST {id: 1000}) RETURN p.content, p.length;")
    assert len(result) == 1
    assert result[0][0] == "Test post content"
    assert result[0][1] == 100

    # Verify relationships
    result = session.execute(
        "MATCH (p:POST {id: 1000})-[:HASCREATOR]->(author:PERSON) RETURN author.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 1

    result = session.execute(
        "MATCH (f:FORUM)-[:CONTAINEROF]->(p:POST {id: 1000}) RETURN f.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 1

    result = session.execute(
        "MATCH (p:POST {id: 1000})-[:ISLOCATEDIN]->(country:PLACE) RETURN country.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 1

    result = session.execute(
        "MATCH (p:POST {id: 1000})-[:HASTAG]->(t:TAG) RETURN count(t) as cnt;"
    )
    assert len(result) == 1
    assert result[0][0] == 2

    session.close()
    db.stop_serving()
    db.close()


def test_iu_7(tmp_path):
    """
    Test IU_7: Create COMMENT with connections to PERSON, PLACE, COMMENT, POST, TAG
    Corresponds to FlagTest.IU_7 in flag_test.cpp
    Note: The CALL syntax in the original query may not be supported, so we'll simplify it
    """
    db_dir = str(tmp_path / "test_iu_7")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()

    # Create schema
    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, firstName STRING, lastName STRING, gender STRING, "
        "birthday DATE, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, "
        "language STRING, email STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE PLACE(id INT64, name STRING, url STRING, type STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE COMMENT(id INT64, creationDate TIMESTAMP, locationIP STRING, "
        "browserUsed STRING, content STRING, length INT32, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE POST(id INT64, imageFile STRING, creationDate TIMESTAMP, locationIP STRING, "
        "browserUsed STRING, language STRING, content STRING, length INT32, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE TAG(id INT64, name STRING, url STRING, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE REL TABLE HASCREATOR(FROM COMMENT TO PERSON, creationDate TIMESTAMP);"
    )
    conn.execute("CREATE REL TABLE ISLOCATEDIN(FROM COMMENT TO PLACE);")
    conn.execute(
        "CREATE REL TABLE REPLYOF(FROM COMMENT TO COMMENT, FROM COMMENT TO POST);"
    )
    conn.execute("CREATE REL TABLE HASTAG(FROM COMMENT TO TAG);")

    # Create prerequisite data
    conn.execute(
        "CREATE (p:PERSON {id: 1, firstName: 'Author', lastName: 'User', gender: 'male', "
        "birthday: date('1990-01-01'), creationDate: timestamp('2024-01-01 00:00:00'), "
        "locationIP: '192.168.1.1', browserUsed: 'Chrome', language: 'en', email: 'author@example.com'});"
    )
    conn.execute(
        "CREATE (country:PLACE {id: 1, name: 'Country1', url: 'country1', type: 'Country'});"
    )
    conn.execute(
        "CREATE (post:POST {id: 100, imageFile: 'img.jpg', creationDate: timestamp('2024-01-01 00:00:00'), "
        "locationIP: '192.168.1.1', browserUsed: 'Chrome', language: 'en', content: 'Post content', length: 50});"
    )
    conn.execute(
        "CREATE (comment:COMMENT {id: 200, creationDate: timestamp('2024-01-01 00:00:00'), "
        "locationIP: '192.168.1.1', browserUsed: 'Chrome', content: 'Comment content', length: 30});"
    )
    conn.execute("CREATE (t1:TAG {id: 10, name: 'Tag1', url: 'tag1'});")
    conn.execute("CREATE (t2:TAG {id: 11, name: 'Tag2', url: 'tag2'});")
    conn.close()

    uri = db.serve(10012, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")

    # Execute IU_7 query with parameters - using the complete union query from flag_test.cpp
    query = """
    MATCH (author:PERSON {id: $authorPersonId}),
          (country:PLACE {id: $countryId})
    CREATE (author)<-[:HASCREATOR {creationDate: $creationDate}]- (c:COMMENT {
        id: $commentId,
        creationDate: $creationDate,
        locationIP: $locationIP,
        browserUsed: $browserUsed,
        content: $content,
        length: $length
    })-[:ISLOCATEDIN]->(country)
    WITH c
    OPTIONAL MATCH(comment :COMMENT {id: $replyToCommentId})
    OPTIONAL MATCH(post:POST {id: $replyToPostId})
    WITH c, comment, post
    CALL (c, comment, post)  {
    WITH c, comment, post
    WHERE comment IS NOT NULL
    CREATE (c)-[:REPLYOF]->(comment: COMMENT)
    RETURN c

    UNION ALL

    WITH c, comment, post
    WHERE post IS NOT NULL
    CREATE (c)-[:REPLYOF]->(post: POST)
    RETURN c
    }
    WITH c
    UNWIND CAST($tagIds, 'INT64[]') AS tagId
    MATCH (t:TAG {id: tagId})
    CREATE (c)-[:HASTAG]->(t)
    """

    # Test case 1: Reply to a comment
    session.execute(
        query,
        parameters={
            "authorPersonId": 1,
            "countryId": 1,
            "creationDate": "2024-01-02 00:00:00",
            "commentId": 1000,
            "locationIP": "192.168.1.1",
            "browserUsed": "Chrome",
            "content": "Reply to comment",
            "length": 20,
            "replyToCommentId": 200,
            "replyToPostId": 0,  # Not used in this case
            "tagIds": [10, 11],
        },
        access_mode="insert",  # ensure the query is executed in insert mode
    )

    # Verify the comment was created
    result = session.execute("MATCH (c:COMMENT {id: 1000}) RETURN c.content;")
    assert len(result) == 1
    assert result[0][0] == "Reply to comment"

    # Verify relationships
    result = session.execute(
        "MATCH (c:COMMENT {id: 1000})-[:HASCREATOR]->(author:PERSON) RETURN author.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 1

    result = session.execute(
        "MATCH (c:COMMENT {id: 1000})-[:ISLOCATEDIN]->(country:PLACE) RETURN country.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 1

    result = session.execute(
        "MATCH (c:COMMENT {id: 1000})-[:REPLYOF]->(comment:COMMENT) RETURN comment.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 200

    result = session.execute(
        "MATCH (c:COMMENT {id: 1000})-[:HASTAG]->(t:TAG) RETURN count(t) as cnt;"
    )
    assert len(result) == 1
    assert result[0][0] == 2

    # Test case 2: Reply to a post
    session.execute(
        query,
        parameters={
            "authorPersonId": 1,
            "countryId": 1,
            "creationDate": "2024-01-03 00:00:00",
            "commentId": 1001,
            "locationIP": "192.168.1.1",
            "browserUsed": "Chrome",
            "content": "Reply to post",
            "length": 25,
            "replyToCommentId": 0,  # Not used in this case
            "replyToPostId": 100,
            "tagIds": [10, 11],
        },
        access_mode="insert",  # ensure the query is executed in insert mode
    )

    # Verify the comment was created and connected to post
    result = session.execute("MATCH (c:COMMENT {id: 1001}) RETURN c.content;")
    assert len(result) == 1
    assert result[0][0] == "Reply to post"

    result = session.execute(
        "MATCH (c:COMMENT {id: 1001})-[:REPLYOF]->(post:POST {id: 100}) RETURN post.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 100

    result = session.execute(
        "MATCH (c:COMMENT {id: 1001})-[:HASTAG]->(t:TAG) RETURN count(t) as cnt;"
    )
    assert len(result) == 1
    assert result[0][0] == 2

    session.close()
    db.stop_serving()
    db.close()


def test_insert_string_column_exaustion(tmp_path):
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
        endpoint = db2.serve(10005, "localhost", False)
        sess = Session.open(endpoint, timeout="10s")
        str_prop = "a" * 256
        for i in range(7000):
            sess.execute(f"CREATE (p: Person {{id: {i+3}, name: '{str_prop}'}});")
    except Exception as e:
        assert "not enough space" in e.__str__(), f"Unexpected exception: {e}"
    finally:
        try:
            sess.close()
            db2.stop_serving()
            db2.close()
        except Exception as e:
            logger.exception(
                "Error while cleaning up resources in test_insert_string_column_exaustion: %s",
                e,
            )
        logging.disable(logging.NOTSET)


def test_readonly_db_rejects_write_queries_via_session(tmp_path):
    """A database opened in read-only mode must reject INSERT/UPDATE queries
    submitted through a Session, while still serving read queries correctly."""
    db_dir = str(tmp_path / "test_readonly_db_rejects_write_queries_via_session")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)

    # Step 1: create the database and populate it in write mode.
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    conn.execute("CREATE (p:person {id: 1, name: 'marko', age: 29});")
    conn.execute("CREATE (p:person {id: 2, name: 'vadas', age: 27});")
    conn.execute(
        "MATCH (a:person), (b:person) WHERE a.id = 1 AND b.id = 2"
        " CREATE (a)-[:knows {weight: 0.5}]->(b);"
    )
    conn.close()
    db.close()

    # Step 2: reopen in read-only mode and start serving.
    db_ro = Database(db_dir, "r")
    uri = db_ro.serve(10006, "localhost", False)
    wait_for_server_ready(uri)

    session = Session(uri, timeout="10s")

    # Read queries must succeed.
    res = session.execute("MATCH (n:person) WHERE n.id = 1 RETURN n.name;")
    assert len(res) == 1
    assert res[0][0] == "marko"

    # INSERT must be rejected by the server.
    with pytest.raises(Exception):
        session.execute("CREATE (p:person {id: 3, name: 'josh', age: 32});")

    # UPDATE must be rejected by the server.
    with pytest.raises(Exception):
        session.execute("MATCH (n:person) WHERE n.id = 1 SET n.age = 99;")

    # The read-only data must be unchanged after the failed writes.
    res = session.execute("MATCH (n:person) RETURN n.id ORDER BY n.id;")
    assert len(res) == 2
    assert res[0][0] == 1
    assert res[1][0] == 2

    session.close()
    db_ro.stop_serving()
    db_ro.close()


def test_in_memory_service_start_and_stop():
    db = Database("", "w")
    db.serve(19001, "127.0.0.1", False)
    db.stop_serving()
    db.close()


def test_checkpoint(tmp_path):
    db_dir = str(tmp_path / "test_checkpoint")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'marko', age: 29});")
    conn.execute("CREATE (p:person {id: 2, name: 'vadas', age: 27});")
    conn.close()

    uri = db.serve(10007, "localhost", False)
    wait_for_server_ready(uri)
    session = Session(uri, timeout="10s")
    session.execute("CHECKPOINT;")
    session.execute("MATCH (n:person) RETURN n.id ORDER BY n.id;")
    session.close()
    db.stop_serving()
    db.close()


# ---------------------------------------------------------------------------
# Tests merged from test_service.py (embedded + service mode integration)
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


def test_session_create_schema_basic_types(tmp_path):
    db_dir = tmp_path / "schema_basic_types"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    endpoint = db.serve(host="127.0.0.1", port=10010, blocking=False)
    sess = Session.open(endpoint=endpoint, timeout="30s", num_threads=5)

    sess.execute(
        "CREATE NODE TABLE PERSON(int32_prop INT32, uint32_prop UINT32, "
        "int64_prop INT64, uint64_prop UINT64, string_prop STRING, "
        "bool_prop BOOL, float_prop FLOAT, double_prop DOUBLE, "
        "PRIMARY KEY(int32_prop));"
    )

    sess.execute(
        "CREATE (n:PERSON {int32_prop: 1, uint32_prop: 2, "
        "int64_prop: 3, uint64_prop: 4, string_prop: 'test', "
        "bool_prop: true, float_prop: 1.23, double_prop: 2.34});"
    )

    result = sess.execute(
        "MATCH (n:PERSON) RETURN n.int32_prop, n.uint32_prop, "
        "n.int64_prop, n.uint64_prop, n.string_prop, "
        "n.bool_prop, n.float_prop, n.double_prop;"
    )
    record = result.__next__()
    logger.info(f"Record: {record}")
    assert record[0] == 1
    assert record[1] == 2
    assert record[2] == 3
    assert record[3] == 4
    assert record[4] == "test"
    assert record[5] is True
    assert (record[6] == 1.23) or (abs(record[6] - 1.23) < 1e-6)  # float comparison
    assert (record[7] == 2.34) or (abs(record[7] - 2.34) < 1e-6)  # double comparison

    sess.close()
    db.stop_serving()
    db.close()


def test_session_alter_vertex_table(tmp_path):
    db_dir = tmp_path / "alter_table"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    endpoint = db.serve(host="localhost", port=10010, blocking=False)
    sess = Session.open(endpoint=endpoint, timeout="30s", num_threads=5)
    sess.execute("CREATE NODE TABLE person(name STRING, age INT64, PRIMARY KEY(name));")
    # 1. add property
    # correctly add a new property
    sess.execute("ALTER TABLE person ADD grade INT64;")
    # incorrectly add a property that already exists
    with pytest.raises(Exception) as excinfo:
        sess.execute("ALTER TABLE person ADD age INT64;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    # 2. rename property
    # correctly rename a property
    sess.execute("ALTER TABLE person RENAME age TO newAge;")
    # incorrectly rename a property that does not exist
    with pytest.raises(Exception) as excinfo:
        sess.execute("ALTER TABLE person RENAME age1 TO newAge1;")
    assert str(ERR_SCHEMA_MISMATCH) in str(excinfo.value)
    # 3. drop property
    # correctly drop a property
    sess.execute("ALTER TABLE person DROP newAge;")
    # incorrectly drop a property that does not exist
    with pytest.raises(Exception) as excinfo:
        sess.execute("ALTER TABLE person DROP age1;")
    assert str(ERR_INVALID_ARGUMENT) in str(excinfo.value)
    sess.close()
    db.close()


# DB-003-24
def test_complex_example(tmp_path):
    db_dir = tmp_path / "complex_example"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    # Create schema
    conn.execute(
        """
        CREATE NODE TABLE Person(
            id INT64 PRIMARY KEY,
            name STRING,
            age INT32,
            email STRING
        )
    """
    )

    conn.execute(
        """
        CREATE NODE TABLE Company(
            id INT64 PRIMARY KEY,
            name STRING,
            industry STRING,
            founded_year INT32
        )
    """
    )

    # Create edge tables
    conn.execute(
        """
        CREATE REL TABLE WORKS_FOR(
            FROM Person TO Company,
            position STRING,
            start_date DATE,
            salary DOUBLE
        )
    """
    )

    conn.execute(
        """
        CREATE REL TABLE KNOWS(
            FROM Person TO Person,
            since_year INT32,
            relationship_type STRING
        )
    """
    )

    conn.execute(
        """
    CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})
    """
    )

    conn.execute(
        """
        CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})
    """
    )

    conn.execute(
        """
        CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})
    """
    )

    # Insert relationships
    conn.execute(
        """
        MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 1
        CREATE (p)-[:WORKS_FOR {position: 'Software Engineer', start_date: date('2020-01-15'), salary: 75000.0}]->(c)
    """
    )

    conn.execute(
        """
        MATCH (p1:Person {id: 1}), (p2:Person {id: 2})
        CREATE (p1)-[:KNOWS {since_year: 2018, relationship_type: 'colleague'}]->(p2)
    """
    )

    conn.close()

    service_endpoint = db.serve(host="localhost", port=10010, blocking=False)
    print(f"Serving database at {service_endpoint}")

    session = Session("http://localhost:10010/")

    session.execute(
        """
        CREATE NODE TABLE User(
            id INT64 PRIMARY KEY,
            username STRING,
            created_at TIMESTAMP
        )
    """
    )

    session.execute(
        """
        CREATE (u:User {id: 1, username: 'user1', created_at: timestamp('2024-01-01 10:00:00')})
    """
    )

    result = session.execute("MATCH (u:User) RETURN u.username, u.created_at")
    for record in result:
        print(f"User: {record[0]}, Created: {record[1]}")

    session.close()
    db.stop_serving()
    db.close()


def test_tp_array_node_create_query():
    """Array property node CREATE/QUERY via session (Bolt/SQL protocol)."""
    db_dir = "/tmp/test_tp_array_node"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE Sensor("
        "  id INT64,"
        "  readings INT32[3],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.close()

    uri = db.serve(10015, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    session.execute("CREATE (s:Sensor {id: 1, readings: [10, 20, 30]});")
    session.execute("CREATE (s:Sensor {id: 2, readings: [40, 50, 60]});")

    result = session.execute("MATCH (s:Sensor) WHERE s.id = 1 RETURN s.readings;")
    assert len(result) == 1
    assert list(result[0][0]) == [10, 20, 30]

    result = session.execute(
        "MATCH (s:Sensor) WHERE s.readings = [10, 20, 30] RETURN s.id;"
    )
    assert len(result) == 1
    assert result[0][0] == 1

    session.close()
    db.stop_serving()
    db.close()


def test_tp_array_edge_create_query():
    """Array property edge CREATE/QUERY via session."""
    db_dir = "/tmp/test_tp_array_edge"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, weights INT64[2]);")
    conn.execute("CREATE (p:Person {id: 1});")
    conn.execute("CREATE (p:Person {id: 2});")
    conn.close()

    uri = db.serve(10016, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    session.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {weights: [7, 9]}]->(b);"
    )

    result = session.execute(
        "MATCH (a:Person)-[e:Knows]->(b:Person) " "RETURN a.id, b.id, e.weights;"
    )
    assert len(result) == 1
    assert result[0][0] == 1
    assert result[0][1] == 2
    assert list(result[0][2]) == [7, 9]

    session.close()
    db.stop_serving()
    db.close()


def test_tp_array_update_set():
    """SET array property on node via session."""
    db_dir = "/tmp/test_tp_array_update_set"
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE Device("
        "  id INT64,"
        "  values DOUBLE[2],"
        "  PRIMARY KEY(id)"
        ");"
    )
    conn.execute("CREATE (d:Device {id: 1, values: [1.0, 2.0]});")
    conn.close()

    uri = db.serve(10017, "localhost", False)
    time.sleep(1)

    session = Session(uri, timeout="10s")
    session.execute("MATCH (d:Device {id: 1}) SET d.values = [9.9, 8.8];")

    result = session.execute("MATCH (d:Device {id: 1}) RETURN d.values;")
    assert len(result) == 1
    values = list(result[0][0])
    assert abs(values[0] - 9.9) < 0.01
    assert abs(values[1] - 8.8) < 0.01

    session.close()
    db.stop_serving()
    db.close()


# ---------------------------------------------------------------------------
# COW transaction correctness tests
#
# These tests verify the copy-on-write update transaction model by
# sending concurrent HTTP requests from multiple threads. Python's GIL
# is released during socket I/O, so threads achieve real concurrency
# for HTTP requests — multiple requests are in-flight simultaneously
# on the server side.
#
# What we can deterministically test:
#   - Concurrent updates are serialized (both succeed, no corruption)
#   - Data remains consistent after concurrent read + update
#
# What we cannot test from the HTTP client side:
#   - Whether reads see old vs new data during update (requires guaranteed
#     timing overlap, which is non-deterministic)
#   - Whether reads are "blocked" vs "not blocked" (update is too fast
#     to distinguish brief blocking from no blocking)
# ---------------------------------------------------------------------------


def test_cow_concurrent_updates_same_vertex(tmp_path):
    """Two concurrent updates to the SAME vertex must not corrupt data.

    Both threads update person 1's age concurrently. Without proper
    serialization (e.g., broken COW or no locking), the concurrent
    writes could corrupt the vertex or produce an unexpected value.

    With serialization: one update commits fully before the other
    starts. Final age is either 40 or 35 — the value from whichever
    update committed last. It must NOT be 30 (original), 0 (default),
    or any other corrupted value.
    """
    import threading

    db_dir = str(tmp_path / "cow_concurrent_updates")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.close()

    uri = db.serve(10060, "localhost", False)
    wait_for_server_ready(uri)

    # Create sessions before starting threads
    sess1 = Session(uri, timeout="15s")
    sess2 = Session(uri, timeout="15s")

    errors = {}

    def do_update(sess, thread_id, new_age):
        try:
            sess.execute(
                "MATCH (p:person {id: 1}) SET p.age = %d;" % new_age,
                access_mode="update",
            )
        except Exception as e:
            errors[thread_id] = str(e)

    # Both threads update the SAME vertex — tests serialization
    t1 = threading.Thread(target=do_update, args=(sess1, 1, 40))
    t2 = threading.Thread(target=do_update, args=(sess2, 2, 35))
    t1.start()
    t2.start()
    t1.join(timeout=30)
    t2.join(timeout=30)

    sess1.close()
    sess2.close()

    assert len(errors) == 0, f"Update errors: {errors}"

    # Final age must be one of the two values (whichever committed last),
    # not the original (30) and not corrupted
    verify_sess = Session(uri, timeout="10s")
    result = verify_sess.execute(
        "MATCH (p:person {id: 1}) RETURN p.age;", access_mode="read"
    )
    final_age = list(result)[0][0]
    verify_sess.close()

    assert final_age in (
        40,
        35,
    ), f"Corrupted or unexpected age: {final_age}. Expected 40 or 35."

    db.stop_serving()
    db.close()


def test_cow_data_consistency_after_concurrent_read_update(tmp_path):
    """After concurrent reads and an update, all snapshots must be consistent.

    A reader session is created before the thread starts and reused for
    15 read queries. Meanwhile, the main thread updates all ages on a
    separate session. After both complete, sessions are closed.

    Validates: each read snapshot is internally consistent — either all
    ages are old (0..99) or all are new (1000..1099), never a mix. This
    would catch a broken COW publish that exposes partial updates.
    """
    import threading

    db_dir = str(tmp_path / "cow_consistency")
    shutil.rmtree(db_dir, ignore_errors=True)
    os.makedirs(db_dir, exist_ok=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    for i in range(100):
        conn.execute(f"CREATE (p:person {{id: {i}, name: 'p{i}', age: {i}}});")
    conn.close()

    uri = db.serve(10061, "localhost", False)
    wait_for_server_ready(uri)

    # Create sessions before starting threads
    reader_sess = Session(uri, timeout="10s")
    updater_sess = Session(uri, timeout="15s")

    read_snapshots = []
    read_errors = []

    def reader(sess):
        try:
            for _ in range(15):
                result = sess.execute(
                    "MATCH (p:person) RETURN p.id, p.age ORDER BY p.id;",
                    access_mode="read",
                )
                ages = [row[1] for row in result]
                read_snapshots.append(ages)
        except Exception as e:
            read_errors.append(str(e))

    reader_thread = threading.Thread(target=reader, args=(reader_sess,))
    reader_thread.start()
    time.sleep(0.05)

    updater_sess.execute(
        "MATCH (p:person) SET p.age = p.age + 1000;", access_mode="update"
    )

    reader_thread.join(timeout=30)

    # Close sessions after all operations complete
    reader_sess.close()
    updater_sess.close()

    assert len(read_errors) == 0, f"Read errors: {read_errors}"
    assert len(read_snapshots) > 0, "No read snapshots collected"

    # Each snapshot must be all-old or all-new, never mixed
    for ages in read_snapshots:
        all_old = all(0 <= a < 1000 for a in ages)
        all_new = all(1000 <= a < 1100 for a in ages)
        assert (
            all_old or all_new
        ), f"Inconsistent snapshot: ages span old and new values: {ages[:5]}..."

    # Final state must be all updated
    verify_sess = Session(uri, timeout="10s")
    result = verify_sess.execute(
        "MATCH (p:person) RETURN p.id, p.age ORDER BY p.id;", access_mode="read"
    )
    final_ages = [row[1] for row in result]
    for i, age in enumerate(final_ages):
        assert age == i + 1000, f"Person {i}: expected {i + 1000}, got {age}"
    verify_sess.close()

    db.stop_serving()
    db.close()
