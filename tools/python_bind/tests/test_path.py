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

import pytest
from conftest import HAS_LDBC
from conftest import LDBC_DIR
from conftest import ensure_result_cnt_gt_zero
from conftest import submit_cypher_query

from neug.database import Database


def test_path_expand(modern_graph):
    conn = modern_graph
    # Test path expansion with a simple query
    result = conn.execute("MATCH (p:person)-[k*1..2]->(f) RETURN k;")
    assert result is not None
    expected_result = [
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 1, "_LABEL": "person", "id": 2, "name": "vadas", "age": 27},
                ],
                "rels": [
                    {
                        "_ID": 1,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 1,
                        "weight": 0.5,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                ],
                "rels": [
                    {
                        "_ID": 2,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 2,
                        "weight": 1.0,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {
                        "_ID": 72057594037927936,
                        "_LABEL": "software",
                        "id": 3,
                        "name": "lop",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 1103806595072,
                        "_LABEL": "created",
                        "_SRC_ID": 0,
                        "_DST_ID": 72057594037927936,
                        "weight": 0.4,
                        "since": 2020,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                    {
                        "_ID": 72057594037927936,
                        "_LABEL": "software",
                        "id": 3,
                        "name": "lop",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 1103808692224,
                        "_LABEL": "created",
                        "_SRC_ID": 2,
                        "_DST_ID": 72057594037927936,
                        "weight": 0.4,
                        "since": 2022,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                    {
                        "_ID": 72057594037927937,
                        "_LABEL": "software",
                        "id": 5,
                        "name": "ripple",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 1103808692225,
                        "_LABEL": "created",
                        "_SRC_ID": 2,
                        "_DST_ID": 72057594037927937,
                        "weight": 1.0,
                        "since": 2021,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 3, "_LABEL": "person", "id": 6, "name": "peter", "age": 35},
                    {
                        "_ID": 72057594037927936,
                        "_LABEL": "software",
                        "id": 3,
                        "name": "lop",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 1103809740800,
                        "_LABEL": "created",
                        "_SRC_ID": 3,
                        "_DST_ID": 72057594037927936,
                        "weight": 0.2,
                        "since": 2023,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                    {
                        "_ID": 72057594037927936,
                        "_LABEL": "software",
                        "id": 3,
                        "name": "lop",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 2,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 2,
                        "weight": 1.0,
                    },
                    {
                        "_ID": 1103808692224,
                        "_LABEL": "created",
                        "_SRC_ID": 2,
                        "_DST_ID": 72057594037927936,
                        "weight": 0.4,
                        "since": 2022,
                    },
                ],
                "length": 2,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                    {
                        "_ID": 72057594037927937,
                        "_LABEL": "software",
                        "id": 5,
                        "name": "ripple",
                        "lang": "java",
                    },
                ],
                "rels": [
                    {
                        "_ID": 2,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 2,
                        "weight": 1.0,
                    },
                    {
                        "_ID": 1103808692225,
                        "_LABEL": "created",
                        "_SRC_ID": 2,
                        "_DST_ID": 72057594037927937,
                        "weight": 1.0,
                        "since": 2021,
                    },
                ],
                "length": 2,
            }
        ],
    ]
    for i, record in enumerate(result):
        assert (
            record == expected_result[i]
        ), f"Record {i} does not match expected result"

    result = conn.execute(
        """MATCH (p:person {name: 'marko'})-[k:knows*1..2 (r, _ | WHERE r.weight <= 1.0)]->(f:person) Return k;"""
    )
    assert result is not None
    expected_result = [
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 1, "_LABEL": "person", "id": 2, "name": "vadas", "age": 27},
                ],
                "rels": [
                    {
                        "_ID": 1,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 1,
                        "weight": 0.5,
                    }
                ],
                "length": 1,
            }
        ],
        [
            {
                "nodes": [
                    {"_ID": 0, "_LABEL": "person", "id": 1, "name": "marko", "age": 29},
                    {"_ID": 2, "_LABEL": "person", "id": 4, "name": "josh", "age": 32},
                ],
                "rels": [
                    {
                        "_ID": 2,
                        "_LABEL": "knows",
                        "_SRC_ID": 0,
                        "_DST_ID": 2,
                        "weight": 1.0,
                    }
                ],
                "length": 1,
            }
        ],
    ]
    for i, record in enumerate(result):
        assert (
            record == expected_result[i]
        ), f"Record {i} does not match expected result"


def test_tinysnb_path_expand(tinysnb):
    conn = tinysnb
    result = conn.execute(
        """
        MATCH (n:Person)-[:Meets*1..2]->(m:Person) return count(*);
        """
    )
    records = list(result)
    assert len(records) == 1
    assert records[0][0] == 13


def test_path_expand_count_on_typed_rel_table(tmp_path):
    db_dir = tmp_path / "path_expand_typed_rel"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    setup_queries = [
        ("CREATE NODE TABLE A(id STRING, p INT32, PRIMARY KEY(id));", "schema"),
        ("CREATE NODE TABLE B(id STRING, q INT32, PRIMARY KEY(id));", "schema"),
        ("CREATE REL TABLE R(FROM A TO B, w INT32);", "schema"),
        ("CREATE (a:A {id:'a1', p:1});", "update"),
        ("CREATE (a:A {id:'a2', p:2});", "update"),
        ("CREATE (b:B {id:'b1', q:1});", "update"),
        (
            "MATCH (a:A {id:'a1'}), (b:B {id:'b1'}) CREATE (a)-[:R {w:1}]->(b);",
            "update",
        ),
        (
            "MATCH (a:A {id:'a2'}), (b:B {id:'b1'}) CREATE (a)-[:R {w:2}]->(b);",
            "update",
        ),
    ]

    for query, access_mode in setup_queries:
        conn.execute(query, access_mode=access_mode)

    result = conn.execute(
        "MATCH (a:A)-[:R*1..2]->(b:B) RETURN count(*) AS c",
        access_mode="read",
    )
    records = list(result)

    assert len(records) == 1
    assert records[0][0] == 2

    conn.close()
    db.close()


def test_path_expand_with_filter(tinysnb):
    conn = tinysnb
    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt*2..2]->(b) WHERE (a.ID = 0) RETURN a.ID, b.ID"
    )
    records = list(result)
    assert records == [[0, 1], [0, 5], [0, 1], [0, 5]]

    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt*2..2]->(b) WHERE ((b.ID < 5)) AND (a.ID = 0) RETURN a.ID, b.ID"
    )
    records = list(result)
    assert records == [[0, 1], [0, 1]]

    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt*2..2]->(b) WHERE (b.ID < 5) RETURN a.ID, b.ID"
    )
    records = list(result)
    assert records == [[3, 3], [7, 3], [0, 1], [10, 1], [0, 1], [7, 1]]


def test_edge_expand_with_filter(tinysnb):
    conn = tinysnb
    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt]->(b) WHERE (a.ID = 0) RETURN a.ID, b.ID,label(e)"
    )
    records = list(result)
    sorted_ = sorted(records, key=lambda x: (x[1], x[2]))
    assert sorted_ == [[0, 1, "studyAt"], [0, 2, "marries"], [0, 2, "meets"]]

    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt]->(b) WHERE ((b.ID > 1)) AND (a.ID = 0) RETURN a.ID, b.ID,label(e);"
    )
    records = list(result)
    assert records == [[0, 2, "meets"], [0, 2, "marries"]]

    result = conn.execute(
        "MATCH (a:person)-[e:meets|:marries|:studyAt]->(b) WHERE (b.ID > 5) RETURN a.ID, b.ID"
    )
    records = list(result)
    assert records == [[3, 7], [7, 8]]


@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_shortest_path():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    result = conn.execute(
        "Match (n:PERSON {id: 933})-[k:KNOWS* SHORTEST  1..3]-(m:PERSON {id: 2199023256668}) Return LENGTH(k) Limit 1;"
    )
    for record in result:
        assert record[0] == 3, f"Expected value 3, got {record[0]}"
    conn.close()
    db.close()


def test_weight_shortest_path(modern_graph):
    conn = modern_graph
    res = conn.execute(
        """
        Match (a:person {name : 'marko'})-[k * WSHORTEST(weight)]-(b:person {name: 'josh'})
        Return a.name, b.name, cost(k);
        """
    )
    records = list(res)
    assert records == [["marko", "josh", 0.8]]


@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_length():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    result = conn.execute(
        "Match (n:PERSON {id: 933})-[k:KNOWS*1..3]->(m) Return LENGTH(k) as len Order by len Limit 1"
    )
    for record in result:
        assert record[0] == 1, f"Expected value 1, got {record[0]}"
    result = conn.execute(
        """
    MATCH (:TAGCLASS {name: "OfficeHolder"})<-[:HASTYPE]-(:TAG)<-[:HASTAG]-(message)-[:REPLYOF*0..30]->(p:POST)
        RETURN count(p) AS numPosts"""
    )
    for record in result:
        assert record[0] == 19519, f"Expected value 19519, got {record[0]}"
    conn.close()
    db.close()


@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_nodes_rels():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    submit_cypher_query(
        conn=conn,
        query="Match (n:PERSON {id: 933})-[k:KNOWS*1..3]-(m:PERSON {id: 2199023256668})"
        " Return nodes(k) as n1, rels(k) as n2 LIMIT 1;",
        lambda_func=ensure_result_cnt_gt_zero,
    )
    conn.close()
    db.close()


@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_properties():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    submit_cypher_query(
        conn=conn,
        query="Match (n:PERSON {id: 933})-[k:KNOWS*1..3]-(m:PERSON {id: 2199023256668})"
        " Return properties(nodes(k), 'firstName') as n1, properties(rels(k),'creationDate') as n2 LIMIT 1;",
        lambda_func=ensure_result_cnt_gt_zero,
    )
    conn.close()
    db.close()


@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_start_end_node():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    submit_cypher_query(
        conn=conn,
        query="Match (n:PERSON {id: 933})-[k:KNOWS]->(m:PERSON {id: 2199023256077})"
        " Return START_NODE(k) as n1, END_NODE(k) as n2;",
        lambda_func=ensure_result_cnt_gt_zero,
    )
    conn.close()
    db.close()


@pytest.mark.skipif(not HAS_LDBC, reason="LDBC data not found")
def test_internal_id_filter():
    db = Database(db_path=LDBC_DIR, mode="r")
    conn = db.connect()
    result = conn.execute(
        "Match (n:PERSON {id: 933}) Where id(n) = 72057594037927936 Return n.id;"
    )
    for record in result:
        assert record[0] == 933, f"Expected value 933, got {record[0]}"
    conn.close()
    db.close()
