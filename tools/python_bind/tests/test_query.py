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
from conftest import HAS_LDBC
from conftest import LDBC_DIR
from conftest import ensure_result_cnt_gt_zero
from conftest import submit_cypher_query

from neug.database import Database
from neug.proto.error_pb2 import ERR_QUERY_SYNTAX

logger = logging.getLogger(__name__)


# DB-003-12
def test_query_sync(modern_graph):
    conn = modern_graph
    result = conn.execute("MATCH (n) RETURN n;")
    assert len(result) == 6


@pytest.mark.asyncio
async def test_query_async(tmp_path):
    db = Database(db_path=str(tmp_path / "modern_graph"), mode="w")
    db.load_builtin_dataset("modern_graph")
    conn = db.async_connect()
    result = await conn.execute("MATCH (n) RETURN n;")
    assert len(result) == 6
    conn.close()
    db.close()


# DB-003-24
def test_query_on_empty_graph(empty_db):
    db, conn = empty_db
    res = conn.execute("MATCH (n) RETURN n;")
    assert res is not None and len(res) == 0


def test_result_getitem(modern_graph):
    conn = modern_graph
    res = conn.execute("MATCH (n) RETURN count(n);")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 6  # Assuming there are 6 nodes
    assert res[-1][0] == 6  # Testing negative indexing


def test_count(tinysnb):
    conn = tinysnb
    res = conn.execute("MATCH (n) RETURN count(n)")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 14

    # test count edges
    res = conn.execute("MATCH ()-[e]->() RETURN count(e)")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 30

    res = conn.execute("MATCH ()-[e]-() RETURN count(e)")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 60

    res = conn.execute("""MATCH ()-[e]-()-[]-()-[]-() RETURN COUNT(*)""")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 4120

    res = conn.execute("""MATCH (a)-[]->(b) return count(*)""")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 30

    res = conn.execute("""MATCH (a)<-[]-(b)-[]->() return count(*)""")
    assert res is not None
    assert len(res) == 1
    assert res[0][0] == 144


def test_distinct(tinysnb):
    conn = tinysnb
    result = conn.execute(
        "MATCH (a:person)-[:knows]->(c:person) Return distinct a.fName;"
    )
    records = list(result)
    print(records)
    assert records == [["Alice"], ["Bob"], ["Carol"], ["Dan"], ["Elizabeth"]]


def test_filtering(tinysnb):
    conn = tinysnb
    result = conn.execute(
        "MATCH (a:person)-[e1:knows]->(b:person) WHERE a.age > 35 RETURN b.fName"
    )
    records = list(result)
    assert records == [["Alice"], ["Bob"], ["Dan"]]


# DB-003-03
def test_return_expression(modern_graph):
    conn = modern_graph
    result = conn.execute(
        "Match (n) RETURN 1+2, date('2023-01-01'), interval('1 year 2 days') limit 1;"
    )
    assert result is not None
    print(result)
    assert len(result) == 1
    row = result.__next__()
    assert row[0] == 3  # 1 + 2
    assert str(row[1]) == "2023-01-01"  # Date
    assert row[2] == "1 year 2 days"  # Interval


def test_return_literal(tinysnb):
    conn = tinysnb
    res = conn.execute("MATCH (a:person) RETURN 1 + 1, label(a) LIMIT 2")
    assert res is not None
    assert len(res) == 2
    assert res[0] == [2, "person"]
    assert res[1] == [2, "person"]  # Assuming there are at


def test_no_existing_property(tinysnb):
    conn = tinysnb
    res = conn.execute(
        """
        MATCH (a:person)-[e1:knows|:studyAt|:workAt]->(b:person:organisation) WHERE a.age > 35 RETURN b.fName, b.name;
        """
    )
    for record in res:
        print(record)


def test_return_date(tinysnb):
    conn = tinysnb
    query = "MATCH (n) return n.birthdate limit 1"
    import datetime

    expected = [[datetime.date(1900, 1, 1)]]
    result = conn.execute(query)
    records = list(result)
    assert records == expected, f"Expected {expected}, got {records}"


def test_query_cyclic(modern_graph):
    conn = modern_graph
    res = conn.execute(
        """Match (a:person)-[:created]->(b:software), (c:person)-[:created]->(b:software),
           (a:person)-[:knows]->(c:person) Where a.name <> b.name AND b.name <> c.name
           Return count(*);
        """
    )
    assert res.__next__()[0] == 1


# DB-003-20
def test_query_syntax_error(tmp_path):
    db_dir = tmp_path / "syntax_error"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    with pytest.raises(Exception) as excinfo:
        conn.execute("MATCH (n RETURN n;")
    assert str(ERR_QUERY_SYNTAX) in str(excinfo.value)
    conn.close()
    db.close()


def test_result(modern_graph):
    conn = modern_graph
    result = conn.execute("Match (n: person) return n")
    logger.info(list(result))
    logger.info(result.column_names())


def test_list_return_basic(tmp_path):
    """Test basic list return functionality: RETURN [p.name, p.value]"""
    db_dir = tmp_path / "list_return_basic"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    # Create schema with list property
    conn.execute(
        "CREATE NODE TABLE Person ("
        "id INT32 PRIMARY KEY, "
        "name STRING, "
        "value FLOAT"
        ");"
    )

    # Insert test data
    conn.execute("CREATE (p:Person {id: 1, name: 'Alice', value: 1.11});")
    conn.execute("CREATE (p:Person {id: 2, name: 'Bob', value: 2.22});")
    conn.execute("CREATE (p:Person {id: 3, name: 'Charlie', value: 3.33});")

    # Test basic list return
    result = conn.execute("MATCH (p:Person) RETURN [p.name, p.value] ORDER BY p.id;")

    records = list(result)
    assert len(records) == 3
    assert records[0][0][0] == "Alice"
    assert records[1][0][0] == "Bob"
    assert records[2][0][0] == "Charlie"
    assert abs(records[0][0][1] - 1.11) < 1e-5
    assert abs(records[1][0][1] - 2.22) < 1e-5
    assert abs(records[2][0][1] - 3.33) < 1e-5
    conn.close()
    db.close()


def test_nested_tuple(modern_graph):
    conn = modern_graph
    result = conn.execute("Match (n {name: 'marko'}) Return [[n.name, n.age], n.id]")
    for record in result:
        assert record[0] == [
            ["marko", 29],
            1,
        ], f"Expected value '[['marko', 29], 1]', got {record[0]}"


def test_null_value_tuple(modern_graph):
    conn = modern_graph
    result = conn.execute("Match (n {name: 'lop'}) Return [n.name, n.age]")
    for record in result:
        assert record[0] == [
            "lop",
            None,
        ], f"Expected value '['lop', None]', got {record[0]}"


# test dummy scan before projection
@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_dummy_scan():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    result = conn.execute("Return 1002")
    for record in result:
        assert record[0] == 1002, f"Expected value 1002, got {record[0]}"
    conn.close()
    db.close()


@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_case_expression():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    result = conn.execute(
        "Match (n:PERSON {id: 933}) Return CASE WHEN n.id > 0 THEN n.id ELSE 0 END"
    )
    for record in result:
        assert record[0] == 933, f"Expected value 933, got {record[0]}"
    conn.close()
    db.close()


# test to_tuple function
# todo(engine): VariableKeys is deprecated by ToTuple in PB.
@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_to_tuple():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    submit_cypher_query(
        conn=conn,
        query="Match (n:PERSON {id: 933})"
        " Return [n.firstName, n.gender, n.birthday] as n2 LIMIT 1;",
        lambda_func=ensure_result_cnt_gt_zero,
    )
    conn.close()
    db.close()


@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_date_time_to_string():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    result = conn.execute(
        """
    MATCH (m:POST:COMMENT {id: 1030792332314})
    RETURN
        CASE
            WHEN m.content = ""
                THEN m.imageFile
            ELSE m.content END as messageContent,
        m.creationDate as messageCreationDate
    """
    )
    result = list(result)
    from datetime import datetime

    datetime_obj = datetime.strptime("2012-07-23 02:25:02.068", "%Y-%m-%d %H:%M:%S.%f")
    assert result == [["photo1030792332314.jpg", datetime_obj]]


def test_parameterized_query(modern_graph):
    conn = modern_graph
    params = {"person_id": 1}
    res = conn.execute(
        """
        MATCH (n:PERSON {id: $person_id})-[:KNOWS]->(m:PERSON)
        RETURN m.name;
        """,
        parameters=params,
    )
    records = list(res)
    assert records == [
        ["vadas"],
        ["josh"],
    ], f"Expected value [['vadas'], ['josh']], got {records}"


def test_parameterized_where_on_edge_string_property():
    """Test that parameterized WHERE on edge STRING property works (not just literals)."""
    db = Database(db_path=":memory", mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE IF NOT EXISTS A(id STRING PRIMARY KEY)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS R(FROM A TO A, tag STRING)")

    conn.execute("CREATE (a:A {id: 'n1'})")
    conn.execute("CREATE (a:A {id: 'n2'})")
    conn.execute(
        "MATCH (a:A), (b:A) WHERE a.id = 'n1' AND b.id = 'n2' CREATE (a)-[:R {tag: 'hello'}]->(b)"
    )

    # Literal string should work
    res_literal = conn.execute(
        "MATCH (a:A)-[e:R]->(b:A) WHERE e.tag = 'hello' RETURN e.tag"
    )
    assert list(res_literal) == [["hello"]]

    # Parameterized string should also work
    res_param = conn.execute(
        "MATCH (a:A)-[e:R]->(b:A) WHERE e.tag = $t RETURN e.tag",
        parameters={"t": "hello"},
    )
    assert list(res_param) == [["hello"]]

    conn.close()
    db.close()


def test_duplicate_project_column(tmp_path):
    """Duplicate `RETURN` of the same property, including ORDER BY output alias cases."""

    # ORDER BY output alias with duplicate project columns and parameters
    db_dir_l0 = tmp_path / "order_alias_dup_project"
    shutil.rmtree(db_dir_l0, ignore_errors=True)
    db_dir_l0.mkdir()
    db_l0 = Database(db_path=str(db_dir_l0), mode="w")
    conn_l0 = db_l0.connect()
    conn_l0.execute("CREATE NODE TABLE L0(id INT64, p0_2 INT64, PRIMARY KEY(id))")
    conn_l0.execute("CREATE (:L0 {id: 1, p0_2: 643})")
    parameters = {"v": 643}
    failing_query = (
        "MATCH (n:L0) "
        "WHERE n.p0_2 = $v "
        "RETURN n.id AS node_id, n.id AS selected_id "
        "ORDER BY node_id LIMIT 100"
    )
    assert list(conn_l0.execute(failing_query, parameters=parameters)) == [[1, 1]]


def test_not_list_contains(tmp_path):
    db_dir = tmp_path / "test_not_list_contains"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE L1(id STRING, p0 STRING, PRIMARY KEY(id));")
    conn.execute("CREATE (:L1 {id: 'n1', p0: 's3836'});")
    conn.execute("CREATE (:L1 {id: 'n2', p0: 'x'});")
    conn.execute("CREATE (:L1 {id: 'n3', p0: 'y'});")

    result = conn.execute("MATCH (n:L1) RETURN count(n) AS pair_count;")
    records = list(result)
    assert records == [[3]]
    result = conn.execute(
        "MATCH (n:L1) WHERE (n.p0 IN ['s3836', 'L1']) RETURN count(n) AS pair_count;"
    )
    records = list(result)
    assert records == [[1]]
    result = conn.execute(
        "MATCH (n:L1) WHERE NOT (n.p0 IN ['s3836', 'L1']) RETURN count(n) AS pair_count;"
    )
    records = list(result)
    assert records == [[2]]
    result = conn.execute(
        "MATCH (n:L1) WHERE ((n.p0 IN ['s3836', 'L1'])) IS NULL RETURN count(n) AS pair_count;"
    )
    records = list(result)
    assert records == [[0]]
    conn.close()
    db.close()


def test_unsupported_operator_error_message(modern_graph):
    """Test that unsupported operators produce readable error messages."""
    conn = modern_graph
    query = "CREATE MACRO f(x) AS x + 1"
    with pytest.raises(Exception, match="Unsupported operator type: CREATE_MACRO"):
        conn.execute(query)


def test_aggregate_dependent_key_1(tinysnb):
    conn = tinysnb

    result = conn.execute(
        """
        MATCH (a:person)-[:knows]->(b:person)
        RETURN a.ID, a.gender, b.gender, sum(b.age)
        ORDER BY a.ID, a.gender, b.gender
    """
    )

    records = list(result)
    assert records == [
        [0, 1, 1, 45],
        [0, 1, 2, 50],
        [2, 2, 1, 80],
        [2, 2, 2, 20],
        [3, 1, 1, 35],
        [3, 1, 2, 50],
        [5, 2, 1, 80],
        [5, 2, 2, 30],
        [7, 1, 2, 65],
    ]


def test_aggregate_dependent_key_2(tinysnb):
    conn = tinysnb

    result = conn.execute(
        """
        MATCH (a:person)
        WHERE a.ID > 4 WITH a, a.age AS foo
        MATCH (a)-[:knows]->(b:person)
        RETURN a.ID, foo, COUNT(*)
    """
    )

    records = list(result)
    assert records == [[5, 20, 3], [7, 20, 2]]


def test_list_extract_function(modern_graph):
    conn = modern_graph
    res = conn.execute(
        """
        Match (a)
        WITH a ORDER BY a.name
        RETURN labels(a) as label, collect(a.name)[0];
    """
    )
    records = list(res)
    assert records == [["person", "josh"], ["software", "lop"]]


def test_unwind_t1_l3_l4_read_from_explicit_schema(tmp_path):
    """Minimal graph for the read query taken from tools/python_bind/batch_log (lines 49, 125, 136, 272).

    Schema and values are hand-written. ``(n2 :L3 :L4)`` is modelled with ``id = 120`` in both
    ``L3`` and ``L4`` (same property map as line 125). T1(120->44) matches line 136.

    With the fixture data, ``UNWIND`` expands ``a1`` to ``{n2.k20, -986093799, n1.k20}``; with
    ``n2.k20=512128668`` and ``n1.k20=1400705806`` (n1 id 44) and ``r1.k43`` true, ``DISTINCT a1, a2``
    yields three rows (order not guaranteed), all with ``a2 == true``.
    """
    _cols = (
        "id INT64, k19 BOOL, k18 BOOL, k20 INT64, k22 BOOL, k21 INT64, k24 STRING, k23 BOOL, "
        "k30 BOOL, k25 STRING, k26 BOOL, k28 INT64, k27 INT64, k29 INT64, PRIMARY KEY (id)"
    )
    ddl = (
        f"CREATE NODE TABLE L3 ({_cols});",
        f"CREATE NODE TABLE L4 ({_cols});",
        "CREATE REL TABLE T1 ("
        "FROM L3 TO L3, k39 STRING, k38 BOOL, k40 BOOL, k42 INT64, k41 STRING, id INT64, k43 BOOL"
        ");",
    )
    # batch_log: line 49 (L3 id 44), line 125 (L3:L4 id 120) — duplicated into L3 and L4 for 120
    dml = (
        "CREATE (n0 :L3 {k19 : false, k18 : true, k20 : 1400705806, k22 : true, id : 44, k21 : 685854768, k23 : false});",
        'CREATE (n0 :L3 {k20 : 512128668, k30 : true, k22 : true, k21 : -1607710882, k24 : "ct", '
        'k23 : false, k26 : false, k25 : "0", k28 : -1022812775, k27 : 1963567328, k19 : false, '
        "k29 : 787123989, k18 : true, id : 120});",
        'CREATE (n0 :L4 {k20 : 512128668, k30 : true, k22 : true, k21 : -1607710882, k24 : "ct", '
        'k23 : false, k26 : false, k25 : "0", k28 : -1022812775, k27 : 1963567328, k19 : false, '
        "k29 : 787123989, k18 : true, id : 120});",
        "MATCH (n0 :L3 {id : 120}), (n1 :L3 {id : 44}) "
        'CREATE (n0)-[r :T1{k39 : "Q", k38 : false, k40 : false, k42 : 1062135372, k41 : "g", '
        "id : 131, k43 : true}]->(n1);",
    )
    read_q = (
        "MATCH (n1 :L3)<-[r1 :T1]-(n2 :L3 :L4) "
        "WHERE ((r1.id) > -1) "
        "UNWIND [(n2.k28), -1206557154, (n2.k28)] AS a0 "
        "UNWIND [(n2.k20), -986093799, (n1.k20)] AS a1 "
        "RETURN DISTINCT a1, (r1.k43) AS a2;"
    )

    db_dir = tmp_path / "unwind_t1_l3_l4"
    db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    try:
        for s in ddl:
            conn.execute(s, access_mode="schema")
        for s in dml:
            conn.execute(s, access_mode="update")
        res = conn.execute(read_q, access_mode="read", parameters=None)
        rows = list(res)
        # n2=120: k20=512128668; n1=44: k20=1400705806; r1.k43 from edge = true; literal -986093799
        expected_a1 = {-986093799, 512128668, 1400705806}
        assert len(rows) == 3
        assert {r[0] for r in rows} == expected_a1
        for r in rows:
            assert r[1] is True or r[1] == 1
    finally:
        conn.close()
        db.close()


# ---------------------------------------------------------------------------
# String Functions tests (merged from test_string_functions.py)
# ---------------------------------------------------------------------------


def test_upper(modern_graph):
    conn = modern_graph
    result = conn.execute("MATCH (n:person) RETURN UPPER(n.name)")
    expected = {"MARKO", "VADAS", "JOSH", "PETER"}
    actual = {record[0] for record in result}
    assert actual == expected, f"Expected {expected}, got {actual}"


def test_lower(modern_graph):
    """Test the LOWER() function on constant strings."""
    conn = modern_graph
    result = conn.execute(
        "RETURN LOWER('MARKO'), LOWER('VaDaS'), LOWER('Josh'), LOWER('PETER')"
    )
    row = next(iter(result))
    expected = ("marko", "vadas", "josh", "peter")
    assert tuple(row) == expected, f"Expected {expected}, got {row}"


def test_reverse(modern_graph):
    """Test the REVERSE() function on Person names."""
    conn = modern_graph
    result = conn.execute("MATCH (n:person) RETURN n.name, REVERSE(n.name)")
    expected_map = {
        "marko": "okram",
        "vadas": "sadav",
        "josh": "hsoj",
        "peter": "retep",
    }
    for record in result:
        original, reversed_str = record
        expected = expected_map[original]
        assert (
            reversed_str == expected
        ), f"Expected {expected} for {original}, got {reversed_str}"


def test_starts_with(modern_graph):
    conn = modern_graph
    # todo: property value of `age` is null, engine will fail if the tuple contains null value
    result = conn.execute("Match (n) Where n.name starts with 'mar' Return n.name")
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result[0][0] == "marko", f"Expected value 'marko', got {result[0][0]}"


def test_ends_with(modern_graph):
    conn = modern_graph
    # todo: property value of `age` is null, engine will fail if the tuple contains null value
    result = conn.execute("Match (n) Where n.name ends with 'rko' Return n.name")
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result[0][0] == "marko", f"Expected value 'marko', got {result[0][0]}"


def test_contains(modern_graph):
    conn = modern_graph
    # todo: property value of `age` is null, engine will fail if the tuple contains null value
    result = conn.execute("Match (n) Where n.name contains 'ark' Return n.name")
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result[0][0] == "marko", f"Expected value 'marko', got {result[0][0]}"


def test_ends_with_and_contains_with_slash_in_string(tmp_path):
    """Test that ends with and contains work correctly with strings containing '/'."""
    db_dir = str(tmp_path / "ends_with_contains_slash_db")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE path_node(path STRING, PRIMARY KEY(path));")
    conn.execute("CREATE (n:path_node {path: 'path/to/file'});")
    conn.execute("CREATE (n:path_node {path: 'a/b/c'});")
    conn.execute("CREATE (n:path_node {path: 'no_slash_here'});")
    conn.execute("CREATE (n:path_node {path: 'trailing/'});")

    # Test ends with: should match only 'path/to/file'
    result = conn.execute(
        "MATCH (n:path_node) WHERE n.path ends with '/file' RETURN n.path ORDER BY n.path"
    )
    rows = list(result)
    assert len(rows) == 1, f"Expected 1 row for ends with '/file', got {len(rows)}"
    assert rows[0][0] == "path/to/file", f"Expected 'path/to/file', got {rows[0][0]}"

    result = conn.execute(
        "MATCH (n:path_node) WHERE n.path ends with '/' RETURN n.path ORDER BY n.path"
    )
    rows = list(result)
    assert len(rows) == 1, f"Expected 1 row for ends with '/', got {len(rows)}"
    assert rows[0][0] == "trailing/", f"Expected 'trailing/', got {rows[0][0]}"

    # Test contains: should match all paths that have '/' in them
    result = conn.execute(
        "MATCH (n:path_node) WHERE n.path contains '/' RETURN n.path ORDER BY n.path"
    )
    rows = list(result)
    assert len(rows) == 3, f"Expected 3 rows for contains '/', got {len(rows)}: {rows}"
    assert rows[0][0] == "a/b/c"
    assert rows[1][0] == "path/to/file"
    assert rows[2][0] == "trailing/"

    # contains '/to/' should match only 'path/to/file'
    result = conn.execute(
        "MATCH (n:path_node) WHERE n.path contains '/to/' RETURN n.path"
    )
    rows = list(result)
    assert len(rows) == 1 and rows[0][0] == "path/to/file"

    conn.close()
    db.close()
    shutil.rmtree(db_dir, ignore_errors=True)


def test_not_starts_with(tmp_path):
    db_dir = tmp_path / "test_not_starts_with"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, id STRING);")
    conn.execute("CREATE (:Person {id: 'n4'});")
    conn.execute("CREATE (:Person {id: 'n8'});")
    conn.execute(
        "MATCH (a:Person {id: 'n4'}), (b:Person {id: 'n8'}) CREATE (a)-[:Knows {id: 'e19'}]->(b);"
    )

    result = conn.execute(
        """
        MATCH (a:Person {id: 'n4'})-[r0:Knows {id: 'e19'}]->(b:Person {id: 'n8'})
        WHERE NOT ('a' STARTS WITH 'a') OR (r0.id IN [a.id])
        RETURN a.id AS source_id, b.id AS target_id;
    """
    )

    records = list(result)
    assert records == []
    conn.close()
    db.close()


def test_create_interval(modern_graph):
    conn = modern_graph
    res = conn.execute("RETURN INTERVAL('5 DAY')")
    for record in res:
        assert record[0] == "5 days", f"Expected value '5 days', got {record[0]}"


# ---------------------------------------------------------------------------
# Intersect tests (merged from test_intersect.py)
# ---------------------------------------------------------------------------


def test_intersect_predicate(tmp_path):
    db_dir = tmp_path / "test_intersect_predicate"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE address(id INT32, name STRING, PRIMARY KEY(id))")
    conn.execute("CREATE REL TABLE structure(FROM address TO address, weight DOUBLE)")
    conn.execute("CREATE REL TABLE belong(FROM address TO address, weight DOUBLE)")

    conn.execute("CREATE (u: address {id: 1, name: 'address1' } )")
    conn.execute("CREATE (v: address {id: 2, name: 'address2' } )")
    conn.execute("CREATE (w: address {id: 3, name: 'address3' } )")
    conn.execute("CREATE (x: address {id: 4, name: 'address4' } )")
    conn.execute("CREATE (y: address {id: 5, name: 'address5' } )")
    conn.execute("CREATE (z: address {id: 6, name: 'address6' } )")

    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:structure {weight: 1.0}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:structure {weight: 2.2}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 1 AND b.id = 6 CREATE (a)-[:structure {weight: 2.3}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 2 AND b.id = 4 CREATE (a)-[:structure {weight: 1.3}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 2 AND b.id = 5 CREATE (a)-[:structure {weight: 1.4}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 3 AND b.id = 6 CREATE (a)-[:structure {weight: 1.5}]->(b)"
    )

    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 3 AND b.id = 6 CREATE (a)-[:belong {weight: 2.0}]->(b)"
    )
    conn.execute(
        "MATCH (a: address), (b: address) WHERE a.id = 4 AND b.id = 5 CREATE (a)-[:belong {weight: 2.1}]->(b)"
    )

    res = conn.execute(
        """
        MATCH(n1: address)-[e1: structure]->(m1: address),
              (n1: address)-[e2: structure]->(m2: address),
              (m1)-[e3: belong]->(m2)
        WHERE n1.id = 1 AND e1.weight > 2.0 AND e2.weight > 2.0 AND e3.weight > 1.9
        RETURN e1.weight, e2.weight, e3.weight
    """
    )
    assert res.__next__() == [2.2, 2.3, 2.0]


def test_intersect_predicate_ml(tinysnb):
    conn = tinysnb
    res = conn.execute(
        """
            MATCH(p1)<-[e1:studyAt]-(t2), (p1)<-[e2:studyAt]-(t1),  (t1)-[e3]-(t2)
            WHERE e1.year > 2020
            RETURN e1.year,e2.year
                       """
    )
    assert list(res) == [[2021, 2020], [2021, 2020], [2021, 2020], [2021, 2020]]


def test_multi_intersect_preserves_all_edge_aliases(tmp_path):
    db_dir = tmp_path / "multi_intersect_edge_aliases"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE FORUM(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE NODE TABLE PERSON(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE NODE TABLE POST(id INT64, PRIMARY KEY(id));")
    conn.execute(
        "CREATE REL TABLE Forum_hasMember_Person("
        "FROM FORUM TO PERSON, edge_id INT64);"
    )
    conn.execute(
        "CREATE REL TABLE Forum_containerOf_Post(" "FROM FORUM TO POST, edge_id INT64);"
    )
    conn.execute(
        "CREATE REL TABLE Person_knows_Person(" "FROM PERSON TO PERSON, edge_id INT64);"
    )
    conn.execute(
        "CREATE REL TABLE Person_likes_Post(" "FROM PERSON TO POST, edge_id INT64);"
    )

    conn.execute("CREATE (:FORUM {id: 1});")
    conn.execute("CREATE (:PERSON {id: 10});")
    conn.execute("CREATE (:PERSON {id: 20});")
    conn.execute("CREATE (:POST {id: 100});")
    conn.execute(
        """
        MATCH (f:FORUM {id: 1}), (p1:PERSON {id: 10}),
              (p2:PERSON {id: 20}), (post:POST {id: 100})
        CREATE (f)-[:Forum_hasMember_Person {edge_id: 1}]->(p1),
               (f)-[:Forum_hasMember_Person {edge_id: 2}]->(p2),
               (f)-[:Forum_containerOf_Post {edge_id: 3}]->(post),
               (p1)-[:Person_knows_Person {edge_id: 4}]->(p2),
               (p1)-[:Person_likes_Post {edge_id: 5}]->(post),
               (p2)-[:Person_likes_Post {edge_id: 6}]->(post);
        """
    )

    result = conn.execute(
        """
        MATCH (f:FORUM)-[e1:Forum_hasMember_Person]->(p1:PERSON),
              (f)-[e2:Forum_hasMember_Person]->(p2:PERSON),
              (f)-[e3:Forum_containerOf_Post]->(post:POST),
              (p1)-[e4:Person_knows_Person]->(p2),
              (p1)-[e5:Person_likes_Post]->(post),
              (p2)-[e6:Person_likes_Post]->(post)
        RETURN e1.edge_id, e2.edge_id, e3.edge_id,
               e4.edge_id, e5.edge_id, e6.edge_id;
        """
    )
    assert list(result) == [[1, 2, 3, 4, 5, 6]]

    conn.close()
    db.close()


def test_where_not_subquery(modern_graph):
    conn = modern_graph
    res = conn.execute(
        """
        Match (a:person)-[:created]->(b)<-[:created]-(c:person)
        Where NOT (a)-[:knows]->(c) AND a <> c
        Return count(a);
    """
    )
    records = list(res)
    assert records == [[5]]


def test_where_subquery(modern_graph):
    conn = modern_graph
    res = conn.execute(
        """
        Match (a:person)-[:created]->(b)<-[:created]-(c:person)
        Where (a)-[:knows]->(c) AND a <> c
        Return count(a);
    """
    )
    records = list(res)
    assert records == [[1]]


def test_exists_correlated_pattern_order(tmp_path):
    """Correlated EXISTS: two comma-separated patterns, same semantics, different order.

    Both queries must compile and return the same start_id; covers NODE_LABEL_FILTER
    folded into GetV via FilterPushDownPattern.
    """
    db_dir = tmp_path / "exists_pattern_order"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE L0 (id STRING PRIMARY KEY);")
    conn.execute("CREATE NODE TABLE L2 (id STRING PRIMARY KEY);")
    conn.execute("CREATE REL TABLE T2 (FROM L2 TO L0);")
    conn.execute("CREATE REL TABLE T0 (FROM L0 TO L2);")

    conn.execute("CREATE (n:L2 {id: 'a'});")
    conn.execute("CREATE (n:L0 {id: 'b'});")
    conn.execute("CREATE (n:L2 {id: 'c'});")
    conn.execute(
        "MATCH (n1:L2), (n2:L0) WHERE n1.id = 'a' AND n2.id = 'b' "
        "CREATE (n1)-[:T2]->(n2);"
    )
    conn.execute(
        "MATCH (n2:L0), (n3:L2) WHERE n2.id = 'b' AND n3.id = 'c' "
        "CREATE (n2)-[:T0]->(n3);"
    )

    q10 = (
        "MATCH (n1) WHERE EXISTS { MATCH (n1:L2)-[r1:T2]->(n2:L0), "
        "(n2:L0)-[r2:T0]->(n3:L2) } RETURN n1.id AS start_id"
    )
    q11 = (
        "MATCH (n1) WHERE EXISTS { MATCH (n2)-[r2:T0]->(n3:L2), "
        "(n1:L2)-[r1:T2]->(n2:L0) } RETURN n1.id AS start_id"
    )

    rows10 = list(conn.execute(q10))
    rows11 = list(conn.execute(q11))
    assert rows10 == [["a"]], f"STEP10 expected [['a']], got {rows10!r}"
    assert rows11 == [["a"]], f"STEP11 expected [['a']], got {rows11!r}"

    conn.close()
    db.close()


# ---------------------------------------------------------------------------
# Optional Match tests (merged from test_optional_match.py)
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


@pytest.mark.skip(
    reason="TODO(zhanglei,lexiao): get prop from invalid vertex: column.h:570]"
    "Check failed: index < basic_size Index out of range: 4294967295 >= 4096"
)
def test_optional_match(modern_graph):
    conn = modern_graph

    conn.execute("CREATE NODE TABLE Person(id INT32, PRIMARY KEY(id));")
    conn.execute("CREATE (p: Person {id: 1});")
    conn.execute("CREATE (p: Person {id: 2});")
    conn.execute("CREATE NODE TABLE Company(id INT32, PRIMARY KEY(id));")
    conn.execute("CREATE (c: Company {id: 1001});")
    conn.execute("CREATE REL TABLE WorkAt(FROM Person TO Company);")
    conn.execute(
        "MATCH (p:Person) WHERE p.id = 1"
        "MATCH (c:Company) WHERE c.id = 1001"
        "CREATE (p)-[:WorkAt]->(c);"
    )

    # ok
    res = conn.execute(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WorkAt]->(c:Company) RETURN p.id, c.id;"
    )
    res = list(res)
    assert len(res) == 2
    assert res[0] == [1, 1001]
    assert res[1] == [2, None]

    # return 1
    res = conn.execute(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WorkAt]->(c:Company) RETURN COUNT(c);"
    )
    res = list(res)
    assert res[0] == [1]

    res = conn.execute(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WorkAt]->(c:Company) RETURN COUNT(*);"
    )
    res = list(res)
    assert res[0] == [2]

    res = conn.execute(
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:WorkAt]->(c:Company) RETURN COUNT(p);"
    )
    res = list(res)
    assert res[0] == [2]


def test_optional_match_on_edge(tmp_path):
    db_dir = str(tmp_path / "test_optional_match_on_edge")
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE SRC_INFRA(id STRING PRIMARY KEY, finder STRING);")
    conn.execute("CREATE NODE TABLE SRC_LOGGING(id STRING PRIMARY KEY, finder STRING);")
    conn.execute("CREATE REL TABLE CALLS_NEW (FROM SRC_INFRA TO SRC_INFRA);")

    conn.execute("CREATE (u: SRC_INFRA {id: '1', finder: 'finder'});")
    conn.execute("CREATE (u: SRC_INFRA {id: '2', finder: 'finder'});")
    conn.execute("CREATE (u: SRC_LOGGING {id: '1', finder: 'finder'});")

    result = conn.execute(
        """
    MATCH (u) WHERE u.finder = 'finder'
    OPTIONAL MATCH (u)-[e:CALLS_NEW]-(v)
    RETURN u, e, v;
    """
    )
    length = len(list(result))
    assert length == 3, f"Expected value 3, got {length}"
    conn.close()
    db.close()


def test_optional_match_person_software(modern_graph):
    """Test OPTIONAL MATCH with multi-label pattern on modern graph.

    Query: MATCH (p: PERSON) WHERE p.id=1
           OPTIONAL MATCH (p)-[]-(other:PERSON:SOFTWARE)
           WHERE other.id>0
           RETURN other;
    """
    conn = modern_graph
    res = conn.execute(
        """
        MATCH (p: PERSON) WHERE p.id=1
        OPTIONAL MATCH (p)-[]-(other:PERSON:SOFTWARE)
        WHERE other.id>0
        RETURN other;
        """
    )
    records = list(res)
    print(records)
    # TODO(zhanglei): fix the output format
    assert records == [
        [{"_ID": 1, "id": 2, "name": "vadas", "age": 27, "_LABEL": "person"}],
        [{"_ID": 2, "id": 4, "name": "josh", "age": 32, "_LABEL": "person"}],
        [
            {
                "_ID": 72057594037927936,
                "id": 3,
                "name": "lop",
                "lang": "java",
                "_LABEL": "software",
            }
        ],
    ]


def test_optional_match_person_software_with_edge_weight(modern_graph):
    """Test OPTIONAL MATCH with multi-label pattern and edge weight condition on modern graph.

    Query: MATCH (p: PERSON) WHERE p.id=1
           OPTIONAL MATCH (p)-[e]->(other:PERSON:Software)
           WHERE e.weight>10 and other.id>10
           RETURN other;
    """
    conn = modern_graph
    res = conn.execute(
        """
        MATCH (p: PERSON) WHERE p.id=1
        OPTIONAL MATCH (p)-[e]->(other:PERSON:Software)
        WHERE e.weight>10 and other.id>10
        RETURN other;
        """
    )
    records = list(res)
    assert records == [[None]]


def test_is_not_null_on_node_variable(tmp_path):
    db_dir = tmp_path / "is_not_null_node"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Node(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE (a:Node {id: 1});")

    result = conn.execute(
        "MATCH (a:Node) WHERE a IS NOT NULL RETURN 1;",
        access_mode="read",
    )
    records = list(result)
    assert len(records) == 1
    assert records[0][0] == 1

    conn.execute("CREATE NODE TABLE person(id INT64 PRIMARY KEY);")
    conn.execute("CREATE REL TABLE knows(FROM person TO person);")
    conn.execute("CREATE (a:person {id: 1});")
    conn.execute("CREATE (a:person {id: 2});")
    conn.execute(
        "MATCH (a:person {id: 1}), (b:person {id: 2}) CREATE (a)-[:knows]->(b);"
    )
    result = conn.execute(
        "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) "
        "WHERE b IS NULL RETURN a, b;",
        access_mode="read",
    )
    records = list(result)
    ids = sorted(row[0]["id"] for row in records)
    assert ids == [1, 2]

    conn.close()
    db.close()


def test_mixed_match(tinysnb):
    conn = tinysnb
    result = conn.execute(
        "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) MATCH (b)-[:knows]->(c:person) RETURN a.id,b.id,c.id;"
    )
    records = list(result)
    assert len(records) == 36


def test_undir_multi_label(tinysnb):
    conn = tinysnb
    result = conn.execute(
        "MATCH (a:person:organisation)-[:meets|:marries|:workAt]-(b:person:organisation) RETURN COUNT(*);"
    )
    records = list(result)
    assert records == [[26]]


def test_multi_label2(tinysnb):
    conn = tinysnb
    result = conn.execute(
        "MATCH (a:person:organisation) OPTIONAL MATCH (a)-[:studyAt|:workAt]->(b:person:organisation) RETURN a.id,b.id;"
    )
    records = list(result)
    logger.info(f"records: {records}, len: {len(records)}")
    assert len(records) == 11
