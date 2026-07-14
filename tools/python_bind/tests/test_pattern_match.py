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
# Tests for the pattern_matching extension: exact / sampled PATTERN_MATCH,
# YIELD, ORDER BY, count / count(DISTINCT), early termination, and the
# optional leading MATCH keyword.

import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database

# A small directed social graph (5 Person nodes, 7 knows edges).
PEOPLE = [
    (0, "Alice", 20, "NYC"),
    (1, "Bob", 30, "LA"),
    (2, "Carol", 20, "NYC"),
    (3, "Dave", 40, "SF"),
    (4, "Erin", 30, "LA"),
]
EDGES = [
    (0, 1, 0.5),
    (1, 2, 1.5),
    (2, 0, 0.5),
    (0, 3, 2.5),
    (3, 1, 0.5),
    (1, 4, 3.5),
    (4, 2, 1.0),
]
# Pattern used throughout; leading MATCH is optional.
PATTERN = "(a:Person)-[r:person_knows_person]->(b:Person)"


@pytest.fixture()
def conn(tmp_path):
    """Writable DB seeded with the social graph and pattern_matching loaded.

    Skips the whole module if the pattern_matching extension is not built.
    """
    db = Database(db_path=str(tmp_path / "pm_db"), mode="w")
    connection = db.connect()
    connection.execute(
        "CREATE NODE TABLE Person("
        "id INT32 PRIMARY KEY, name STRING, age INT32, city STRING);"
    )
    connection.execute(
        "CREATE REL TABLE person_knows_person(" "FROM Person TO Person, weight DOUBLE);"
    )
    for pid, name, age, city in PEOPLE:
        connection.execute(
            f"CREATE (n:Person {{id: {pid}, name: '{name}', "
            f"age: {age}, city: '{city}'}});"
        )
    for src, dst, w in EDGES:
        connection.execute(
            "MATCH (a:Person), (b:Person) "
            f"WHERE a.id = {src} AND b.id = {dst} "
            f"CREATE (a)-[:person_knows_person {{weight: {w}}}]->(b);"
        )
    try:
        connection.execute("LOAD pattern_matching;")
    except RuntimeError as exc:
        pytest.skip(f"pattern_matching extension not available: {exc}")
    try:
        yield connection
    finally:
        connection.close()
        db.close()


def test_exact_pattern_match_returns_all(conn):
    """PATTERN_MATCH(cypher) enumerates all 7 directed edges as a|r|b."""
    result = conn.execute(f"CALL PATTERN_MATCH('{PATTERN}') RETURN *;")
    assert result.column_names() == ["a", "r", "b"]
    rows = list(result)
    assert len(rows) == 7


def test_bare_pattern_equals_explicit_match(conn):
    """The leading MATCH keyword is optional."""
    bare = list(conn.execute(f"CALL PATTERN_MATCH('{PATTERN}') RETURN *;"))
    explicit = list(conn.execute(f"CALL PATTERN_MATCH('MATCH {PATTERN}') RETURN *;"))
    assert len(bare) == len(explicit) == 7


def test_exact_early_termination(conn):
    """is_sampled=false stops after `size` matches."""
    rows = list(conn.execute(f"CALL PATTERN_MATCH('{PATTERN}', 2, false) RETURN *;"))
    assert len(rows) == 2
    # A size larger than the total still returns all matches.
    all_rows = list(
        conn.execute(f"CALL PATTERN_MATCH('{PATTERN}', 100, false) RETURN *;")
    )
    assert len(all_rows) == 7


def test_sampled_pattern_match(conn):
    """is_sampled=true runs FaSTest sampling and returns rows."""
    rows = list(
        conn.execute(
            f"CALL PATTERN_MATCH('{PATTERN}', 4, true) "
            "RETURN a.name AS src, b.name AS dst;"
        )
    )
    assert len(rows) > 0


def test_size_below_one_is_rejected(conn):
    """size must be >= 1 in both modes."""
    with pytest.raises(RuntimeError):
        conn.execute(f"CALL PATTERN_MATCH('{PATTERN}', 0, true) RETURN *;")
    with pytest.raises(RuntimeError):
        conn.execute(f"CALL PATTERN_MATCH('{PATTERN}', 0, false) RETURN *;")


def test_order_by_property_with_limit(conn):
    """ORDER BY a scalar property + LIMIT on the matched output."""
    result = conn.execute(
        f"CALL PATTERN_MATCH('{PATTERN}') "
        "RETURN a.name AS src, r.weight AS weight "
        "ORDER BY r.weight DESC LIMIT 1;"
    )
    rows = list(result)
    assert len(rows) == 1
    # Heaviest edge is Bob->Erin with weight 3.5.
    assert rows[0][0] == "Bob"
    assert rows[0][1] == pytest.approx(3.5)


def test_count_aggregate(conn):
    """count(a) over the matches."""
    rows = list(
        conn.execute(f"CALL PATTERN_MATCH('{PATTERN}') RETURN count(a) AS cnt;")
    )
    assert rows[0][0] == 7


def test_count_distinct(conn):
    """count(DISTINCT ...) over scalar properties of matched vertices."""
    rows = list(
        conn.execute(
            f"CALL PATTERN_MATCH('{PATTERN}') "
            "RETURN count(DISTINCT a.name) AS sources, "
            "count(DISTINCT a.city) AS cities;"
        )
    )
    # 5 distinct source names, 3 distinct cities (NYC, LA, SF).
    assert rows[0][0] == 5
    assert rows[0][1] == 3


def test_yield_rename(conn):
    """YIELD a AS x renames the bound variable; x.* resolves to a.*."""
    result = conn.execute(
        f"CALL PATTERN_MATCH('{PATTERN}') YIELD a AS x, b AS y "
        "RETURN x.name AS src, y.name AS dst "
        "ORDER BY x.age DESC LIMIT 1;"
    )
    rows = list(result)
    assert len(rows) == 1
    # Highest-age source is Dave(40), who knows Bob.
    assert rows[0][0] == "Dave"
    assert rows[0][1] == "Bob"


def test_yield_subset_selects_correct_column(conn):
    """YIELD a subset reads the correct underlying column (not positional)."""
    rows = list(
        conn.execute(
            f"CALL PATTERN_MATCH('{PATTERN}') YIELD b "
            "RETURN b.name AS dst ORDER BY b.name;"
        )
    )
    # Destinations across the 7 edges, sorted by name.
    dsts = [row[0] for row in rows]
    assert dsts == ["Alice", "Bob", "Bob", "Carol", "Carol", "Dave", "Erin"]


def test_yield_hidden_variable_not_in_scope(conn):
    """A variable not listed in YIELD is hidden from the trailing RETURN."""
    with pytest.raises(RuntimeError):
        conn.execute(f"CALL PATTERN_MATCH('{PATTERN}') YIELD a RETURN b.name;")
