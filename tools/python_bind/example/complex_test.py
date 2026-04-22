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
NeuG Comprehensive Capability Test Script
==========================================
Tests based on official documentation:
  - installation   : import & in-memory database
  - getting_started: schema creation, data CRUD, builtin dataset
  - extensions     : JSON extension install / load / query
  - tutorials      : tinysnb builtin dataset exploration
"""

import json
import os
import shutil
import sys
import tempfile
import traceback

# ── colour helpers ──────────────────────────────────────────────
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"

passed = 0
failed = 0


def section(title):
    print(f"\n{BOLD}{CYAN}{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}{RESET}\n")


def ok(msg):
    global passed
    passed += 1
    print(f"  {GREEN}✅ PASS{RESET} — {msg}")


def fail(msg, err=None):
    global failed
    failed += 1
    print(f"  {RED}❌ FAIL{RESET} — {msg}")
    if err:
        print(f"       {RED}{err}{RESET}")


# ── resolve path to example JSON files shipped in the repo ─────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
JSON_ARRAY_FILE = os.path.join(
    REPO_ROOT, "example_dataset", "tinysnb", "json", "vPerson.json"
)
JSONL_FILE = os.path.join(
    REPO_ROOT, "example_dataset", "tinysnb", "json", "vPerson.jsonl"
)
PARQUET_FILE = os.path.join(
    REPO_ROOT, "example_dataset", "tinysnb", "parquet", "vPerson.parquet"
)


def run_statement(conn, desc, statement):
    """Execute a statement and report pass/fail."""
    try:
        conn.execute(statement)
        ok(desc)
    except Exception as e:
        fail(desc, e)


def run_query_with_handler(conn, desc, query, handler, *, print_traceback=False):
    """Run a query, hand results to handler, and surface handler message."""
    try:
        result = list(conn.execute(query))
        message = handler(result) or desc
        ok(message)
    except Exception as e:
        fail(desc, e)
        if print_traceback:
            traceback.print_exc()


def verify_json_extension_loaded(conn_json):
    try:
        ext_result = list(conn_json.execute("CALL SHOW_LOADED_EXTENSIONS() RETURN *;"))
        print("       Loaded extensions:")
        for row in ext_result:
            print(f"         {row}")
        found_json = any("JSON" in str(row) or "json" in str(row) for row in ext_result)
        if found_json:
            ok("JSON extension appears in SHOW_LOADED_EXTENSIONS")
        else:
            fail("JSON extension NOT found in SHOW_LOADED_EXTENSIONS")
    except Exception as e:
        fail("SHOW_LOADED_EXTENSIONS", e)


def run_json_array_tests(conn_json, export_dir=None):
    if not os.path.isfile(JSON_ARRAY_FILE):
        fail(f"JSON Array file not found: {JSON_ARRAY_FILE}")
        return

    def _load_all(rows):
        print(f"       JSON Array: loaded {len(rows)} rows from vPerson.json")
        if rows:
            print(f"       First row sample: {rows[0]}")
        assert len(rows) > 0, "Expected at least 1 row"
        return f"LOAD FROM JSON Array file returned {len(rows)} rows"

    run_query_with_handler(
        conn_json,
        "LOAD FROM JSON Array file",
        f'LOAD FROM "{JSON_ARRAY_FILE}" RETURN *;',
        _load_all,
        print_traceback=True,
    )

    def _projection(rows):
        print(f"       Column projection (fName, age): {len(rows)} rows")
        if rows:
            print(f"       Sample: {rows[0]}")
        return "JSON column projection"

    run_query_with_handler(
        conn_json,
        "JSON column projection",
        f'LOAD FROM "{JSON_ARRAY_FILE}" RETURN fName, age;',
        _projection,
    )

    def _alias(rows):
        if rows:
            print(f"       Column alias sample: {rows[0]}")
        return "JSON column alias"

    run_query_with_handler(
        conn_json,
        "JSON column alias",
        f'LOAD FROM "{JSON_ARRAY_FILE}" RETURN fName AS name, age AS years;',
        _alias,
    )

    # Export test: COPY LOAD result to JSON array file and verify
    if export_dir:
        export_path = os.path.join(export_dir, "export_array.json")
        try:
            conn_json.execute(
                f'COPY (LOAD FROM "{JSON_ARRAY_FILE}" RETURN fName, age) TO '
                f"'{export_path}';"
            )
            with open(export_path, encoding="utf-8") as f:
                data = json.load(f)
            assert isinstance(data, list), "Expected JSON array"
            assert len(data) > 0, "Expected at least one exported row"
            if data:
                first = data[0]
                assert isinstance(first, dict), "Each row should be a JSON object"
            ok(f"Export to JSON array: {len(data)} rows written to {export_path}")
        except Exception as e:
            fail("Export LOAD result to JSON array", e)


def run_jsonl_tests(conn_json, export_dir=None):
    if not os.path.isfile(JSONL_FILE):
        fail(f"JSONL file not found: {JSONL_FILE}")
        return

    def _load_all(rows):
        print(f"       JSONL: loaded {len(rows)} rows from vPerson.jsonl")
        if rows:
            print(f"       First row sample: {rows[0]}")
        assert len(rows) > 0, "Expected at least 1 row"
        return f"LOAD FROM JSONL file returned {len(rows)} rows"

    run_query_with_handler(
        conn_json,
        "LOAD FROM JSONL file",
        f'LOAD FROM "{JSONL_FILE}" RETURN *;',
        _load_all,
        print_traceback=True,
    )

    def _projection(rows):
        print(f"       JSONL column projection: {len(rows)} rows")
        return "JSONL column projection"

    run_query_with_handler(
        conn_json,
        "JSONL column projection",
        f'LOAD FROM "{JSONL_FILE}" RETURN fName, age;',
        _projection,
    )

    # Export test: COPY LOAD result to JSONL file and verify
    if export_dir:
        export_path = os.path.join(export_dir, "export_lines.jsonl")
        try:
            conn_json.execute(
                f'COPY (LOAD FROM "{JSONL_FILE}" RETURN fName, age) TO '
                f"'{export_path}';"
            )
            with open(export_path, encoding="utf-8") as f:
                lines = [line.strip() for line in f if line.strip()]
            data = [json.loads(line) for line in lines]
            assert len(data) > 0, "Expected at least one exported line"
            if data:
                first = data[0]
                assert isinstance(first, dict), "Each line should be a JSON object"
            ok(f"Export to JSONL: {len(data)} lines written to {export_path}")
        except Exception as e:
            fail("Export LOAD result to JSONL", e)


def run_parquet_extension_suite(db_parquet, conn_parquet, db_path_parquet):
    statements = [
        ("LOAD PARQUET succeeded", "LOAD PARQUET;"),
    ]

    for desc, stmt in statements:
        run_statement(conn_parquet, desc, stmt)

    if not os.path.isfile(PARQUET_FILE):
        fail(f"Parquet file not found: {PARQUET_FILE}")
    else:

        def _load_all(rows):
            print(f"       Parquet: loaded {len(rows)} rows from vPerson.parquet")
            if rows:
                print(f"       First row sample: {rows[0]}")
            assert len(rows) > 0, "Expected at least 1 row"
            return f"LOAD FROM Parquet file returned {len(rows)} rows"

        run_query_with_handler(
            conn_parquet,
            "LOAD FROM Parquet file",
            f'LOAD FROM "{PARQUET_FILE}" RETURN *;',
            _load_all,
            print_traceback=True,
        )

        def _projection(rows):
            print(f"       Column projection (fName, age): {len(rows)} rows")
            if rows:
                print(f"       Sample: {rows[0]}")
            assert len(rows) > 0, "Expected at least 1 row"
            assert len(rows[0]) == 2, "Should return only 2 columns"
            return "Parquet column projection"

        run_query_with_handler(
            conn_parquet,
            "Parquet column projection",
            f'LOAD FROM "{PARQUET_FILE}" RETURN fName, age;',
            _projection,
        )

        def _filter(rows):
            print(f"       WHERE age > 30: {len(rows)} rows")
            for row in rows:
                assert row[1] > 30, f"age {row[1]} should be > 30"
            return f"Parquet WHERE filter returned {len(rows)} rows"

        run_query_with_handler(
            conn_parquet,
            "Parquet WHERE filter (age > 30)",
            f'LOAD FROM "{PARQUET_FILE}" WHERE age > 30 RETURN fName, age;',
            _filter,
        )

    conn_parquet.close()
    db_parquet.close()
    ok("Closed Parquet extension test database")
    shutil.rmtree(db_path_parquet, ignore_errors=True)


def run_json_extension_suite(db_json, conn_json, db_path_json):
    statements = [
        ("LOAD JSON succeeded", "LOAD JSON;"),
    ]

    for desc, stmt in statements:
        run_statement(conn_json, desc, stmt)

    verify_json_extension_loaded(conn_json)
    run_json_array_tests(conn_json, export_dir=db_path_json)
    run_jsonl_tests(conn_json, export_dir=db_path_json)

    conn_json.close()
    db_json.close()
    ok("Closed JSON extension test database")
    shutil.rmtree(db_path_json, ignore_errors=True)


def run_tinysnb_suite(db_snb, db_path_tinysnb):
    try:
        conn_snb = db_snb.connect()
    except Exception as e:
        fail("Connect to tinysnb database", e)
        shutil.rmtree(db_path_tinysnb, ignore_errors=True)
        return

    def _count_and_print(rows, print_label, ok_label):
        value = rows[0][0] if rows else 0
        print(f"       {print_label}: {value}")
        return f"{ok_label}: {value}"

    count_queries = [
        ("Total nodes", "MATCH (n) RETURN count(n)", "Total nodes in tinysnb graph"),
        ("Persons", "MATCH (p:person) RETURN count(p)", "Person count"),
        (
            "Organisations",
            "MATCH (o:organisation) RETURN count(o)",
            "Organisation count",
        ),
        ("Movies", "MATCH (m:movies) RETURN count(m)", "Movie count"),
    ]

    for print_label, query, ok_label in count_queries:
        run_query_with_handler(
            conn_snb,
            f"Count {print_label.lower()}",
            query,
            lambda rows, pl=print_label, ok_lab=ok_label: _count_and_print(
                rows, pl, ok_lab
            ),
        )

    def _list_people(rows):
        print("       People in the social network:")
        for name, age, is_student, is_worker in rows:
            status = []
            if is_student:
                status.append("Student")
            if is_worker:
                status.append("Worker")
            status_str = " & ".join(status) if status else "—"
            print(f"         {name} (age {age}): {status_str}")
        return f"Listed {len(rows)} people from tinysnb"

    run_query_with_handler(
        conn_snb,
        "List people",
        """
            MATCH (p:person)
            RETURN p.fName, p.age, p.isStudent, p.isWorker
            ORDER BY p.age
        """,
        _list_people,
    )

    def _social_connections(rows):
        print("       Most connected people:")
        for person, friend_count in rows:
            print(f"         {person} knows {friend_count} people")
        return "Social connection analysis"

    run_query_with_handler(
        conn_snb,
        "Social connection analysis",
        """
            MATCH (p:person)-[k:knows]->(friend:person)
            RETURN p.fName, count(friend) as friend_count
            ORDER BY friend_count DESC
            LIMIT 5
        """,
        _social_connections,
    )

    def _academic_affiliations(rows):
        if rows:
            print("       Academic affiliations:")
            for person, org, year in rows:
                print(f"         {person} studied at {org} in {year}")
        return f"Academic affiliation query returned {len(rows)} rows"

    run_query_with_handler(
        conn_snb,
        "Academic affiliation query",
        """
            MATCH (p:person)-[s:studyAt]->(o:organisation)
            RETURN p.fName, o.name, s.year
            ORDER BY s.year DESC
        """,
        _academic_affiliations,
    )

    def _network_stats():
        try:
            person_count = list(conn_snb.execute("MATCH (p:person) RETURN count(p)"))[
                0
            ][0]
            actual_conn = list(
                conn_snb.execute("MATCH ()-[k:knows]->() RETURN count(k)")
            )[0][0]
            max_possible = person_count * (person_count - 1)
            density = (actual_conn / max_possible) * 100 if max_possible > 0 else 0
            print(
                f"       Network: {person_count} people, {actual_conn} connections, density {density:.2f}%"
            )
            ok("Network density calculation")
        except Exception as e:
            fail("Network density calculation", e)

    _network_stats()

    conn_snb.close()
    db_snb.close()
    ok("Closed tinysnb database")
    shutil.rmtree(db_path_tinysnb, ignore_errors=True)


# ================================================================
#  1. Installation — import & version
# ================================================================
section("1. Installation — Import & Basic Check")

try:
    import neug

    ok("import neug succeeded")
except ImportError as e:
    fail("import neug", e)
    print(f"\n{RED}Cannot continue without neug. Exiting.{RESET}")
    sys.exit(1)

# ================================================================
#  2. In-Memory Database & Connection
# ================================================================
section("2. In-Memory Database & Connection")

try:
    db_mem = neug.Database("")
    ok("Created in-memory database")
except Exception as e:
    fail("Create in-memory database", e)
    sys.exit(1)

try:
    conn_mem = db_mem.connect()
    ok("Connected to in-memory database")
except Exception as e:
    fail("Connect to in-memory database", e)
    sys.exit(1)

# ================================================================
#  3. Getting Started — Schema, Insert, Query
# ================================================================
section("3. Getting Started — Schema Creation")

try:
    conn_mem.execute(
        """
        CREATE NODE TABLE Person(
            id INT64 PRIMARY KEY,
            name STRING,
            age INT64,
            email STRING
        )
    """
    )
    ok("Created node table Person")
except Exception as e:
    fail("Create node table Person", e)

try:
    conn_mem.execute(
        """
        CREATE NODE TABLE Company(
            id INT64 PRIMARY KEY,
            name STRING,
            industry STRING,
            founded_year INT64
        )
    """
    )
    ok("Created node table Company")
except Exception as e:
    fail("Create node table Company", e)

try:
    conn_mem.execute(
        """
        CREATE REL TABLE WORKS_FOR(
            FROM Person TO Company,
            position STRING,
            start_date DATE,
            salary DOUBLE
        )
    """
    )
    ok("Created rel table WORKS_FOR")
except Exception as e:
    fail("Create rel table WORKS_FOR", e)

try:
    conn_mem.execute(
        """
        CREATE REL TABLE KNOWS(
            FROM Person TO Person,
            since_year INT64,
            relationship_type STRING
        )
    """
    )
    ok("Created rel table KNOWS")
except Exception as e:
    fail("Create rel table KNOWS", e)

# ── Insert Data ─────────────────────────────────────────────────
section("3b. Getting Started — Data Insertion")

insert_stmts = [
    (
        "Insert Person Alice",
        "CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})",
    ),
    (
        "Insert Person Bob",
        "CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})",
    ),
    (
        "Insert Company TechCorp",
        "CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})",
    ),
    (
        "Insert WORKS_FOR edge",
        """MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 1
        CREATE (p)-[:WORKS_FOR {position: 'Software Engineer', start_date: date('2020-01-15'), salary: 75000.0}]->(c)""",
    ),
    (
        "Insert KNOWS edge",
        """MATCH (p1:Person {id: 2}), (p2:Person {id: 1})
        CREATE (p1)-[:KNOWS {since_year: 2018, relationship_type: 'colleague'}]->(p2)""",
    ),
]

for desc, stmt in insert_stmts:
    try:
        conn_mem.execute(stmt)
        ok(desc)
    except Exception as e:
        fail(desc, e)

# ── Query Data ──────────────────────────────────────────────────
section("3c. Getting Started — Querying Data")

try:
    result = list(
        conn_mem.execute("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age")
    )
    assert len(result) == 2, f"Expected 2 persons, got {len(result)}"
    print(f"       Persons: {result}")
    ok("Simple node query returned 2 persons")
except Exception as e:
    fail("Simple node query", e)

try:
    result = list(
        conn_mem.execute(
            """
        MATCH (p:Person)-[w:WORKS_FOR]->(c:Company)
        RETURN p.name, w.position, c.name
    """
        )
    )
    assert len(result) >= 1, f"Expected >=1 row, got {len(result)}"
    row = result[0]
    print(f"       {row[0]} works as {row[1]} at {row[2]}")
    ok("Relationship query (Person-WORKS_FOR->Company)")
except Exception as e:
    fail("Relationship query", e)

try:
    result = list(
        conn_mem.execute(
            """
        MATCH (p1:Person)-[:KNOWS]->(p2:Person)-[:WORKS_FOR]->(c:Company)
        RETURN p1.name, p2.name, c.name
    """
        )
    )
    assert len(result) >= 1
    row = result[0]
    print(f"       {row[0]} knows {row[1]} who works at {row[2]}")
    ok("Complex pattern query (KNOWS->WORKS_FOR)")
except Exception as e:
    fail("Complex pattern query", e)

# clean up in-memory db
conn_mem.close()
db_mem.close()
ok("Closed in-memory database")

# ================================================================
#  4. Tutorials — TinySNB Builtin Dataset
# ================================================================
section("4. Tutorials — TinySNB Builtin Dataset")

db_path_tinysnb = tempfile.mkdtemp(prefix="neug_tinysnb_")
try:
    db_snb = neug.Database(db_path_tinysnb)
    db_snb.load_builtin_dataset("tinysnb")
    ok(f"Loaded tinysnb builtin dataset into {db_path_tinysnb}")
except Exception as e:
    fail("Load tinysnb builtin dataset", e)
    traceback.print_exc()
    # try to clean up and skip rest of section
    shutil.rmtree(db_path_tinysnb, ignore_errors=True)
    db_snb = None

if db_snb is not None:
    run_tinysnb_suite(db_snb, db_path_tinysnb)

# ================================================================
#  5. Extensions — JSON Extension
# ================================================================
section("5. Extensions — JSON Extension (Install / Load / Query)")

_run_ext_tests = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").strip().lower()
_run_ext_tests = _run_ext_tests in ("1", "true", "on", "yes")

if not _run_ext_tests:
    print("  (skipped: set NEUG_RUN_EXTENSION_TESTS=1 to run extension tests)")
else:
    conn_json = None
    db_path_json = tempfile.mkdtemp(prefix="neug_json_ext_")
    try:
        db_json = neug.Database(db_path_json)
        conn_json = db_json.connect()
        ok(f"Created persistent database for JSON extension test at {db_path_json}")
    except Exception as e:
        fail("Create database for JSON extension", e)
        db_json = None

    if db_json is not None and conn_json is not None:
        run_json_extension_suite(db_json, conn_json, db_path_json)

# ================================================================
#  6. Extensions — Parquet Extension
# ================================================================
section("6. Extensions — Parquet Extension (Install / Load / Query)")

_run_ext_tests = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").strip().lower()
_run_ext_tests = _run_ext_tests in ("1", "true", "on", "yes")

if not _run_ext_tests:
    print("  (skipped: set NEUG_RUN_EXTENSION_TESTS=1 to run extension tests)")
else:
    conn_parquet = None
    db_path_parquet = tempfile.mkdtemp(prefix="neug_parquet_ext_")
    try:
        db_parquet = neug.Database(db_path_parquet)
        conn_parquet = db_parquet.connect()
        ok(
            f"Created persistent database for Parquet extension test at {db_path_parquet}"
        )
    except Exception as e:
        fail("Create database for Parquet extension", e)
        db_parquet = None

    if db_parquet is not None and conn_parquet is not None:
        run_parquet_extension_suite(db_parquet, conn_parquet, db_path_parquet)

# ================================================================
#  Summary
# ================================================================
section("Summary")
total = passed + failed
print(f"  Total : {total}")
print(f"  {GREEN}Passed: {passed}{RESET}")
print(f"  {RED}Failed: {failed}{RESET}")
print()

if failed == 0:
    print(
        f"  {GREEN}{BOLD}🎉 All tests passed! NeuG is working correctly on this platform.{RESET}"
    )
else:
    print(
        f"  {YELLOW}{BOLD}⚠️  {failed} test(s) failed. Please review the output above.{RESET}"
    )

sys.exit(0 if failed == 0 else 1)
