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
import shutil
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from neug.database import Database
from neug.proto.error_pb2 import ERR_TYPE_CONVERSION
from neug.proto.error_pb2 import ERR_TYPE_OVERFLOW


def test_not_exist_result(tmp_path):
    """Test complex query patterns and edge cases"""
    db_dir = tmp_path / "complex_queries"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    # Create schema
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));"
    )
    conn.execute("CREATE NODE TABLE company(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE works_at(FROM person TO company, since INT32);")

    # Insert test data
    conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});")
    conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});")
    conn.execute("CREATE (c:company {id: 1, name: 'TechCorp'});")
    conn.execute(
        "MATCH (p:person), (c:company) WHERE p.id = 1 AND c.id = 1 CREATE (p)-[:works_at {since: 2020}]->(c);"
    )

    # Test complex pattern with OPTIONAL MATCH
    result = conn.execute(
        """
        MATCH (p:person)
        OPTIONAL MATCH (p)-[w:works_at]->(c:company)
        RETURN p.name, c.name, w.since
        ORDER BY p.name;
    """
    )

    records = list(result)
    assert len(records) == 2
    assert records[0][0] == "Alice"  # Alice's name
    assert records[0][1] == "TechCorp"  # Alice's company
    assert records[0][2] == 2020  # Alice's since
    assert records[1][0] == "Bob"  # Bob's name
    assert records[1][1] is None  # Bob's company (None)
    assert records[1][2] is None  # Bob's since (None)

    conn.close()
    db.close()


@pytest.mark.skip(reason="failed with multiple aggregate functions")
def test_count_avg_aggregation(tmp_path):
    """Test aggregation functions with edge cases"""
    db_dir = tmp_path / "aggregation"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    # Create schema
    conn.execute(
        "CREATE NODE TABLE test_node(id INT64, value INT32, category STRING, PRIMARY KEY(id));"
    )

    # Insert test data
    conn.execute("CREATE (n:test_node {id: 1, value: 10, category: 'A'});")
    conn.execute("CREATE (n:test_node {id: 2, value: 20, category: 'A'});")
    conn.execute("CREATE (n:test_node {id: 3, value: 30, category: 'B'});")

    # Test aggregation with grouping , AVG(n.value)
    result = conn.execute(
        "MATCH (n:test_node) RETURN n.category, COUNT(n.value), AVG(n.value) ORDER BY n.category;"
    )
    records = list(result)
    # assert len(records) == 2
    assert records[0][0] == "A"  # Category A
    assert records[0][1] == 2  # Count for category A
    assert records[0][2] == 15.0  # Average for category A
    assert records[1][0] == "B"  # Category B
    assert records[1][1] == 1  # Count for category B (NULL excluded)
    assert records[1][2] == 30.0  # Average for category B

    conn.close()
    db.close()


def test_multiple_hops(tmp_path):
    """Test path-related queries with edge cases"""
    db_dir = tmp_path / "path_queries"
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    # Create schema for a circular graph
    conn.execute("CREATE NODE TABLE node(id INT64, name STRING, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE connects(FROM node TO node);")

    # Create a cycle: A -> B -> C -> A
    conn.execute(
        "CREATE (a:node {id: 1, name: 'A'}), (b:node {id: 2, name: 'B'}), (c:node {id: 3, name: 'C'});"
    )
    conn.execute(
        "MATCH (a:node {name: 'A'}), (b:node {name: 'B'}) CREATE (a)-[:connects]->(b);"
    )
    conn.execute(
        "MATCH (b:node {name: 'B'}), (c:node {name: 'C'}) CREATE (b)-[:connects]->(c);"
    )
    conn.execute(
        "MATCH (c:node {name: 'C'}), (a:node {name: 'A'}) CREATE (c)-[:connects]->(a);"
    )

    # Test variable length path with cycle
    result = conn.execute(
        "MATCH (a:node {name: 'A'})-[:connects*1..10]->(b) RETURN b.name, COUNT(*) AS cnt ORDER BY b.name;"
    )
    records = list(result)
    # Should find B and C, each once, despite the cycle
    # NOTE: default model is "arbitrary", so the result is repeat
    assert records[0][0] == "A"
    assert records[0][1] == 3
    assert records[1][0] == "B"
    assert records[1][1] == 4
    assert records[2][0] == "C"
    assert records[2][1] == 3

    conn.close()
    db.close()


@pytest.mark.skip(reason="failed with mixed operators and expressions")
def test_mixed_operators_and_expressions(tmp_path):
    """
    Test combinations of different operators and expressions.
    This might expose issues with operator precedence or expression evaluation.
    """
    db_dir = tmp_path / "mixed_operators_test"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    # Create node table with numeric properties
    conn.execute(
        "CREATE NODE TABLE Calculation("
        "id INT64 PRIMARY KEY, "
        "a INT32, "
        "b INT32, "
        "c DOUBLE"
        ")"
    )

    # Insert test data
    conn.execute("CREATE (c:Calculation {id: 1, a: 10, b: 5, c: 2.5})")
    conn.execute("CREATE (c:Calculation {id: 2, a: 20, b: 3, c: 1.8})")
    conn.execute("CREATE (c:Calculation {id: 3, a: 15, b: 7, c: 3.2})")

    # Test 1: Mathematical expressions with field references
    result = conn.execute(
        "MATCH (c:Calculation) " "WHERE c.id = 1 " "RETURN c.a + c.b * c.c"
    )
    record = result.__next__()
    # Should evaluate as: 10 + 5 * 2.5 = 10 + 12.5 = 22.5
    assert abs(record[0] - 22.5) < 1e-5

    # Test 2: Complex expression with parentheses
    result = conn.execute(
        "MATCH (c:Calculation) " "WHERE c.id = 2 " "RETURN (c.a + c.b) * c.c"
    )
    record = result.__next__()
    # Should evaluate as: (20 + 3) * 1.8 = 23 * 1.8 = 41.4
    assert abs(record[0] - 41.4) < 1e-5

    # Test 3: Expression with comparison operators
    result = conn.execute(
        "MATCH (c:Calculation) " "WHERE c.a > c.b * 2 " "RETURN c.id, c.a, c.b"
    )
    records = list(result)
    # Should return records where a > b*2:
    # Record 1: 10 > 5*2 = 10 > 10 = False
    # Record 2: 20 > 3*2 = 20 > 6 = True
    # Record 3: 15 > 7*2 = 15 > 14 = True
    assert len(records) == 2
    ids = [record[0] for record in records]
    assert 2 in ids
    assert 3 in ids

    # Test 4: Complex logical expression with multiple conditions
    result = conn.execute(
        "MATCH (c:Calculation) "
        "WHERE (c.a > 10 AND c.b < 6) OR (c.c > 3.0) "
        "RETURN c.id"
    )
    records = list(result)
    # Should return:
    # Record 1: (10>10 AND 5<6) OR (2.5>3.0) = (False AND True) OR False = False OR False = False
    # Record 2: (20>10 AND 3<6) OR (1.8>3.0) = (True AND True) OR False = True OR False = True
    # Record 3: (15>10 AND 7<6) OR (3.2>3.0) = (True AND False) OR True = False OR True = True
    assert len(records) == 2
    ids = [record[0] for record in records]
    assert 2 in ids
    assert 3 in ids

    # Test 5: Expression with field updates
    conn.execute("MATCH (c:Calculation) " "WHERE c.id = 1 " "SET c.a = c.a + c.b * 2")

    # Verify the update
    result = conn.execute("MATCH (c:Calculation) " "WHERE c.id = 1 " "RETURN c.a")
    record = result.__next__()
    # Should be: 10 + 5 * 2 = 10 + 10 = 20
    assert abs(record[0] - 20) < 1e-5

    # Test 6: Complex expression in SET clause
    conn.execute("MATCH (c:Calculation) " "WHERE c.id = 2 " "SET c.b = (c.a + c.b) / 2")

    # Verify the update
    result = conn.execute("MATCH (c:Calculation) " "WHERE c.id = 2 " "RETURN c.b")
    record = result.__next__()
    # Should be: (20 + 3) / 2 = 23 / 2 = 11 (integer division)
    assert record[0] == 11

    conn.close()
    db.close()
