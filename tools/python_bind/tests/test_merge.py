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

"""
Regression tests for MERGE semantics described in specs/005-merge/design.md.
"""

from __future__ import annotations

import logging
import os
import shutil
import sys

import pytest

from neug.database import Database

logger = logging.getLogger(__name__)


def _merge_db_dir() -> str:
    tmp = os.environ.get("TMPDIR", "/tmp")
    return os.path.join(tmp, "test_merge_design")


def _open_merge_database(db_dir: str) -> tuple[Database, object]:
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE NODE TABLE User2(name STRING, age INT64, PRIMARY KEY(name));")
    conn.execute("CREATE REL TABLE follows(FROM User TO User, date INT64);")
    return db, conn


def _seed_users_adam_marko(conn) -> None:
    """user.csv: Adam,29 and marko,32 from design.md."""
    conn.execute("CREATE (:User {name: 'Adam', age: 29});")
    conn.execute("CREATE (:User {name: 'marko', age: 32});")


def _seed_follows_adam_marko_2012(conn) -> None:
    """follows.csv row: Adam -> marko, date 2012."""
    conn.execute(
        "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'marko'}) "
        "CREATE (u1)-[:follows {date: 2012}]->(u2);"
    )


def test_merge_node_match_existing_adam():
    """MERGE (n:User {name: 'Adam'}) RETURN — Adam exists, no new vertex."""
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        rows = list(conn.execute("MERGE (n:User {name: 'Adam'}) RETURN n.age;"))
        assert rows == [[29]]
        cnt = list(conn.execute("MATCH (n:User) RETURN count(n);"))[0][0]
        assert cnt == 2
    finally:
        conn.close()
        db.close()


def test_merge_node_create_bob():
    """MERGE (n:User {name: 'Bob', age: 45}) — Bob absent, equivalent to CREATE."""
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        rows = list(
            conn.execute("MERGE (n:User {name: 'Bob', age: 45}) RETURN n.name, n.age;")
        )
        assert rows == [["Bob", 45]]
        names = sorted(r[0] for r in conn.execute("MATCH (n:User) RETURN n.name;"))
        assert names == ["Adam", "Bob", "marko"]
    finally:
        conn.close()
        db.close()


def test_merge_edge_match_existing_adam_marko():
    """MERGE edge when Adam-marko with date=2012 already exists."""
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        _seed_follows_adam_marko_2012(conn)
        rows = list(
            conn.execute(
                "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'marko'}) "
                "MERGE (u1)-[e:follows {date: 2012}]->(u2) "
                "RETURN u1.name, e.date, u2.name;"
            )
        )
        assert rows == [["Adam", 2012, "marko"]]
        ecnt = list(conn.execute("MATCH ()-[e:follows]->() RETURN count(e);"))[0][0]
        assert ecnt == 1
    finally:
        conn.close()
        db.close()


def test_merge_edge_create_adam_bob():
    """MERGE edge Adam-Bob when only Adam-marko exists — inserts one edge."""
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        conn.execute("CREATE (:User {name: 'Bob', age: 40});")
        _seed_follows_adam_marko_2012(conn)
        rows = list(
            conn.execute(
                "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'Bob'}) "
                "MERGE (u1)-[e:follows {date: 2012}]->(u2) "
                "RETURN u1.name, e.date, u2.name;"
            )
        )
        assert rows == [["Adam", 2012, "Bob"]]
        ecnt = list(conn.execute("MATCH ()-[e:follows]->() RETURN count(e);"))[0][0]
        assert ecnt == 2
    finally:
        conn.close()
        db.close()


def test_merge_edge_all_pairs_where_id_less_than():
    """
    MATCH all User pairs with id(u1) < id(u2), MERGE follows date 2012.

    Design outcome (three users; edges Adam-marko and Adam-Bob exist, marko-Bob absent):
    three rows returned; only marko-Bob edge is created by MERGE.
    """
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        conn.execute("CREATE (:User {name: 'Bob', age: 40});")
        conn.execute(
            "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'marko'}) "
            "CREATE (u1)-[e:follows {date: 2012}]->(u2);"
        )
        conn.execute(
            "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'Bob'}) "
            "CREATE (u1)-[:follows {date: 2012}]->(u2);"
        )
        rows = sorted(
            list(
                conn.execute(
                    "MATCH (u1:User), (u2:User) "
                    "WHERE id(u1) < id(u2) "
                    "MERGE (u1)-[e:follows {date: 2012}]->(u2) "
                    "RETURN u1.name, e.date, u2.name;"
                )
            ),
            key=lambda r: (r[0], r[2]),
        )
        expected = sorted(
            [
                ["Adam", 2012, "Bob"],
                ["Adam", 2012, "marko"],
                ["marko", 2012, "Bob"],
            ],
            key=lambda r: (r[0], r[2]),
        )
        assert rows == expected
        ecnt = list(conn.execute("MATCH ()-[e:follows]->() RETURN count(e);"))[0][0]
        assert ecnt == 3
    finally:
        conn.close()
        db.close()


def test_merge_edge_on_match_set_date():
    """ON MATCH SET runs when the edge exists — updates stored date."""
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        _seed_follows_adam_marko_2012(conn)
        rows = list(
            conn.execute(
                "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'marko'}) "
                "MERGE (u1)-[e:follows {date: 2012}]->(u2) "
                "ON MATCH SET e.date = 9000 "
                "RETURN e.date;"
            )
        )
        assert rows == [[9000]]
        stored = list(
            conn.execute(
                "MATCH (u1:User {name: 'Adam'})-[e:follows]->"
                "(u2:User {name: 'marko'}) RETURN e.date;"
            )
        )
        assert stored == [[9000]]
        assert (
            list(conn.execute("MATCH ()-[e:follows]->() RETURN count(e);"))[0][0] == 1
        )
    finally:
        conn.close()
        db.close()


def test_merge_edge_on_create_set_date_for_new_edge():
    """
    ON CREATE SET applies when MERGE creates the edge — final date from ON CREATE.
    """
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        conn.execute("CREATE (:User {name: 'Bob', age: 40});")
        _seed_follows_adam_marko_2012(conn)
        rows = list(
            conn.execute(
                "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'Bob'}) "
                "MERGE (u1)-[e:follows {date: 2012}]->(u2) "
                "ON CREATE SET e.date = 7001 "
                "RETURN e.date;"
            )
        )
        assert rows == [[7001]]
        stored = list(
            conn.execute(
                "MATCH (u1:User {name: 'Adam'})-[e:follows]->"
                "(u2:User {name: 'Bob'}) RETURN e.date;"
            )
        )
        assert stored == [[7001]]
    finally:
        conn.close()
        db.close()


def test_merge_edge_on_create_ignored_when_edge_exists():
    """ON CREATE SET does not run on match — Adam-marko stays 2012."""
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        _seed_follows_adam_marko_2012(conn)
        rows = list(
            conn.execute(
                "MATCH (u1:User {name: 'Adam'}), (u2:User {name: 'marko'}) "
                "MERGE (u1)-[e:follows {date: 2012}]->(u2) "
                "ON CREATE SET e.date = 11111 "
                "RETURN u1.name, u2.name, e.date;"
            )
        )
        assert rows == [["Adam", "marko", 2012]]
        stored = list(
            conn.execute(
                "MATCH (u1:User {name: 'Adam'})-[e:follows]->"
                "(u2:User {name: 'marko'}) RETURN e.date;"
            )
        )
        assert stored == [[2012]]
    finally:
        conn.close()
        db.close()


def test_merge_on_create_set_bob_age():
    """ON CREATE SET only when vertex is created — Bob gets age 60."""
    db_dir = _merge_db_dir()
    shutil.rmtree(db_dir, ignore_errors=True)
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        rows = list(
            conn.execute(
                "MERGE (n:User {name: 'Bob'}) "
                "ON CREATE SET n.age = 60 RETURN n.name, n.age;"
            )
        )
        assert rows == [["Bob", 60]]
    finally:
        conn.close()
        db.close()


def test_merge_on_match_set_adam_age():
    """ON MATCH SET when Adam exists — age becomes 35."""
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        rows = list(
            conn.execute(
                "MERGE (n:User {name: 'Adam'}) "
                "ON MATCH SET n.age = 35 RETURN n.name, n.age;"
            )
        )
        assert rows == [["Adam", 35]]
    finally:
        conn.close()
        db.close()


def test_merge_on_create_set_does_not_fire_for_existing_adam():
    """ON CREATE SET ignored when Adam matches — age stays 29."""
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        rows = list(
            conn.execute(
                "MERGE (n:User {name: 'Adam'}) "
                "ON CREATE SET n.age = 35 RETURN n.name, n.age;"
            )
        )
        assert rows == [["Adam", 29]]
    finally:
        conn.close()
        db.close()


def test_optional_match_follows_where_age_correlated_count():
    """
    OPTIONAL MATCH with a predicate on inner and outer rows (b.age > a.age).

    Same shape as compiler join_test A_OPTIONAL_AB_11
    (MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) WHERE b.age > a.age
    RETURN COUNT(*)), but on the MERGE test schema (User / follows).

    Regression for merge-related planner changes that touch optional joins and
    correlated WHERE handling.

    Data: Adam (29), marko (32); single edge Adam -[follows]-> marko.
    The WHERE is applied inside the optional branch; each outer `a` still
    contributes a row to COUNT(*), so the count is 2.
    """
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        _seed_follows_adam_marko_2012(conn)
        cnt = list(
            conn.execute(
                "MATCH (a:User) OPTIONAL MATCH (a)-[:follows]->(b:User) "
                "WHERE b.age > a.age RETURN COUNT(*);"
            )
        )[0][0]
        assert cnt == 2
    finally:
        conn.close()
        db.close()


# ---------------------------------------------------------------------------
# Unsupported MERGE patterns (see specs/005-merge/spec.md §限制)
# ---------------------------------------------------------------------------


def test_merge_multiple_nodes_unsupported():
    """
    MERGE (u1:User {name: 'Adam'}), (u2:User {name: 'Bob'}) is NOT supported.

    Per spec: merging multiple nodes in a single MERGE clause has ambiguous
    semantics around partial existence (e.g. u1 exists but u2 does not) and
    must be rejected.
    """
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        with pytest.raises(Exception):
            conn.execute("MERGE (u1:User {name: 'Adam'}), (u2:User {name: 'Bob'});")
    finally:
        conn.close()
        db.close()


def test_merge_path_unsupported():
    """
    MERGE (:User {name:'A'})-[:follows {date: 2012}]->(:User {name:'B'})
    is NOT supported.

    Per spec: MERGE on a full path pattern would create all vertices and the
    edge without checking individual vertex existence, leading to duplicates
    or correctness issues. Must be rejected.
    """
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        with pytest.raises(Exception):
            conn.execute(
                "MERGE (:User {name: 'Adam'})-[:follows {date: 2012}]->(:User {name: 'marko'});"
            )
    finally:
        conn.close()
        db.close()


def test_merge_path_with_new_nodes_unsupported():
    """
    MERGE with a path pattern containing nodes not yet in the graph is also
    unsupported — must be rejected rather than silently creating duplicates.
    """
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    try:
        _seed_users_adam_marko(conn)
        with pytest.raises(Exception):
            conn.execute(
                "MERGE (:User {name: 'X'})-[:follows {date: 2023}]->(:User {name: 'Y'});"
            )
    finally:
        conn.close()
        db.close()


def test_unwind_merge():
    db_dir = _merge_db_dir()
    db, conn = _open_merge_database(db_dir)
    res = conn.execute(
        "UNWIND ['a', 'b', 'c'] as x MERGE (u:User {name: x}) Return u.name"
    )
    assert list(res) == [["a"], ["b"], ["c"]]
