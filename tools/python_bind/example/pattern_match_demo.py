#!/usr/bin/env python3
"""Demo: pattern_matching extension + NeuG aggregation operators.

This script:
  1. builds a small social graph (Person nodes + person_knows_person edges);
  2. runs the unified PATTERN_MATCH table function in its modes:
       - exact (all)      : CALL PATTERN_MATCH(cypher)
       - exact early-stop : CALL PATTERN_MATCH(cypher, size, false)  # size >= 1
       - sampled          : CALL PATTERN_MATCH(cypher, size, true)   # size >= 1
  3. drives NeuG's own pipeline operators on the PATTERN_MATCH output:
       ORDER BY, LIMIT, count(), count(DISTINCT ...).

Run it with the repo's Python bindings on the path, e.g. from the repo root:

    python3 tools/python_bind/example/pattern_match_demo.py

It auto-adds tools/python_bind to sys.path and relies on the built artifacts
under <repo>/build (set NEUG_BUILD_DIR to override).
"""

import os
import shutil
import sys

# Make the in-repo `neug` package importable without installing the wheel.
_HERE = os.path.dirname(os.path.abspath(__file__))
_PY_BIND_DIR = os.path.dirname(_HERE)  # tools/python_bind
if _PY_BIND_DIR not in sys.path:
    sys.path.insert(0, _PY_BIND_DIR)

import neug  # noqa: E402  (import for side effects: .so discovery + env setup)
from neug.database import Database  # noqa: E402

DB_PATH = os.path.join(os.environ.get("TMPDIR", "/tmp"), "neug_pattern_match_demo_db")


def run(conn, query, title):
    """Execute a query and pretty-print the result as a simple table."""
    print()
    print("=" * 72)
    print(title)
    print("-" * 72)
    print("query:", " ".join(query.split()))
    result = conn.execute(query)
    cols = result.column_names()
    rows = list(result)
    print("columns:", cols)
    if not rows:
        print("(no rows)")
        return rows
    widths = [len(str(c)) for c in cols]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    header = " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
    print(header)
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(" | ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)))
    print(f"({len(rows)} row(s))")
    return rows


def truncate_vertex(cell, limit=70):
    """Vertex/Edge cells are JSON strings; shorten them for display."""
    s = str(cell)
    return s if len(s) <= limit else s[: limit - 3] + "..."


def main():
    shutil.rmtree(DB_PATH, ignore_errors=True)
    db = Database(DB_PATH, "w")
    conn = db.connect()
    try:
        # ---- schema -----------------------------------------------------
        conn.execute(
            "CREATE NODE TABLE Person("
            "id INT32 PRIMARY KEY, name STRING, age INT32, city STRING);"
        )
        conn.execute(
            "CREATE REL TABLE person_knows_person("
            "FROM Person TO Person, weight DOUBLE);"
        )

        # ---- nodes ------------------------------------------------------
        people = [
            (0, "Alice", 20, "NYC"),
            (1, "Bob", 30, "LA"),
            (2, "Carol", 20, "NYC"),
            (3, "Dave", 40, "SF"),
            (4, "Erin", 30, "LA"),
        ]
        for pid, name, age, city in people:
            conn.execute(
                f"CREATE (n:Person {{id: {pid}, name: '{name}', "
                f"age: {age}, city: '{city}'}});"
            )

        # ---- edges (directed knows, with a weight) ----------------------
        edges = [
            (0, 1, 0.5),
            (1, 2, 1.5),
            (2, 0, 0.5),
            (0, 3, 2.5),
            (3, 1, 0.5),
            (1, 4, 3.5),
            (4, 2, 1.0),
        ]
        for src, dst, w in edges:
            conn.execute(
                "MATCH (a:Person), (b:Person) "
                f"WHERE a.id = {src} AND b.id = {dst} "
                f"CREATE (a)-[:person_knows_person {{weight: {w}}}]->(b);"
            )

        print(f"Loaded {len(people)} Person nodes and {len(edges)} edges.")

        # ---- load the pattern_matching extension ------------------------
        conn.execute("LOAD pattern_matching;")

        # The leading MATCH keyword is optional; a bare pattern is accepted.
        PATTERN = "(a:Person)-[r:person_knows_person]->(b:Person)"

        # =================================================================
        # 1. EXACT pattern matching: CALL PATTERN_MATCH(cypher)
        # =================================================================
        rows = run(
            conn,
            f"CALL PATTERN_MATCH('{PATTERN}') RETURN *;",
            "1) EXACT pattern match  -  CALL PATTERN_MATCH(cypher) RETURN *",
        )
        # The a/r/b columns are whole Vertex/Edge JSON objects; show shortened.
        print("\n  (vertex/edge cells are JSON objects, e.g. first row:)")
        if rows:
            for col, cell in zip(["a", "r", "b"], rows[0]):
                print(f"    {col}: {truncate_vertex(cell)}")

        # =================================================================
        # 2. EXACT with early termination: PATTERN_MATCH(cypher, size, false)
        #    Stops after the first `size` matches instead of enumerating all.
        # =================================================================
        run(
            conn,
            f"CALL PATTERN_MATCH('{PATTERN}', 3, false) "
            "RETURN a.name AS src, b.name AS dst;",
            "2) EXACT + early termination  -  PATTERN_MATCH(cypher, 3, false)",
        )
        print(
            "\n  (exact matching stops once 3 matches are found; the full graph\n"
            "   has 7 matching edges, so this returns exactly 3.)"
        )

        # =================================================================
        # 3. SAMPLED matching: CALL PATTERN_MATCH(cypher, size, true)
        # =================================================================
        run(
            conn,
            f"CALL PATTERN_MATCH('{PATTERN}', 4, true) "
            "RETURN a.name AS src, b.name AS dst;",
            "3) SAMPLED pattern match  -  PATTERN_MATCH(cypher, 4, true)",
        )
        print(
            "\n  (sampled matching uses FaSTest; the sample size (>= 1) caps the\n"
            "   number of sampled embeddings, so results may vary per run.)"
        )

        # =================================================================
        # 4. NeuG pipeline operators on the PATTERN_MATCH output
        #    These project scalar properties out of the matched vertices/edges
        #    and apply ORDER BY / LIMIT / count / count(DISTINCT).
        # =================================================================

        # 4a. ORDER BY a scalar property + LIMIT
        run(
            conn,
            f"CALL PATTERN_MATCH('{PATTERN}') "
            "RETURN a.name AS src, a.age AS src_age, "
            "b.name AS dst, r.weight AS weight "
            "ORDER BY r.weight DESC, src ASC LIMIT 5;",
            "4a) ORDER BY r.weight DESC, src ASC  +  LIMIT 5",
        )

        # 4b. LIMIT only
        run(
            conn,
            f"CALL PATTERN_MATCH('{PATTERN}') "
            "RETURN a.name AS src, b.name AS dst LIMIT 3;",
            "4b) LIMIT 3 (no ordering)",
        )

        # 4c. count() over all matches
        run(
            conn,
            f"CALL PATTERN_MATCH('{PATTERN}') RETURN count(a) AS match_count;",
            "4c) count(a)  -  total number of matched edges",
        )

        # 4d. count(DISTINCT ...) - distinct source people / cities
        run(
            conn,
            f"CALL PATTERN_MATCH('{PATTERN}') "
            "RETURN count(DISTINCT a.name) AS distinct_sources, "
            "count(DISTINCT a.city) AS distinct_src_cities, "
            "count(DISTINCT b.name) AS distinct_targets;",
            "4d) count(DISTINCT ...)  -  distinct sources / cities / targets",
        )

        # 4e. group-by-style aggregation: per source, how many it knows
        run(
            conn,
            f"CALL PATTERN_MATCH('{PATTERN}') "
            "RETURN a.name AS person, count(b) AS knows_count, "
            "count(DISTINCT b.city) AS distinct_cities_known "
            "ORDER BY knows_count DESC, person ASC;",
            "4e) GROUP BY source: count(b) + count(DISTINCT b.city), ordered",
        )

        print()
        print("=" * 72)
        print("Done.")
    finally:
        conn.close()
        db.close()
        shutil.rmtree(DB_PATH, ignore_errors=True)


if __name__ == "__main__":
    main()
