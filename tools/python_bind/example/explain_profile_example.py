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
Example demonstrating PROFILE and EXPLAIN modes in NeuG.

This script shows how to:
1. Execute queries with PROFILE mode to get execution statistics
2. Execute queries with EXPLAIN mode to see the execution plan without running
3. Access profile metrics both as formatted text (for CLI) and structured dict (for programming)

Usage:
    python3 explain_profile_example.py                      # Uses default db at /tmp/neug_profile_example_db
    python3 explain_profile_example.py /path/to/custom/db   # Uses custom db path
"""

import os
import shutil
import sys

# Automatically add the parent directory (tools/python_bind) to sys.path
# so that 'import neug' works without setting PYTHONPATH
_script_dir = os.path.dirname(os.path.abspath(__file__))
_tools_python_bind_dir = os.path.join(_script_dir, "..")
if _tools_python_bind_dir not in sys.path:
    sys.path.insert(0, _tools_python_bind_dir)

import neug
from neug.database import Database


def load_sample_data(conn):
    """Create and populate a simple graph with person and software nodes using CSV COPY."""
    import tempfile

    # Create schema
    conn.execute(
        "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")
    conn.execute("CREATE REL TABLE created(FROM person TO software, weight DOUBLE);")

    # Create temporary directory for CSV files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create person.csv
        person_csv = os.path.join(tmpdir, "person.csv")
        with open(person_csv, "w") as f:
            f.write("id|name|age\n")
            f.write("1|marko|29\n")
            f.write("2|vadas|27\n")
            f.write("3|josh|32\n")
            f.write("4|peter|35\n")

        # Create software.csv
        software_csv = os.path.join(tmpdir, "software.csv")
        with open(software_csv, "w") as f:
            f.write("id|name|lang\n")
            f.write("10|lop|java\n")
            f.write("11|ripple|java\n")

        # Create knows.csv
        knows_csv = os.path.join(tmpdir, "knows.csv")
        with open(knows_csv, "w") as f:
            f.write("from|to|weight\n")
            f.write("1|2|0.5\n")
            f.write("1|3|1.0\n")
            f.write("2|3|0.7\n")
            f.write("3|4|0.4\n")

        # Create created.csv
        created_csv = os.path.join(tmpdir, "created.csv")
        with open(created_csv, "w") as f:
            f.write("from|to|weight\n")
            f.write("1|10|0.4\n")
            f.write("4|10|0.6\n")
            f.write("1|11|1.0\n")

        # Load data using COPY
        conn.execute(f'COPY person FROM "{person_csv}"')
        conn.execute(f'COPY software FROM "{software_csv}"')
        conn.execute(f'COPY knows FROM "{knows_csv}" (from="person", to="person")')
        conn.execute(
            f'COPY created FROM "{created_csv}" (from="person", to="software")'
        )

    print("✓ Sample data loaded successfully")


def example_profile_single_scan(conn):
    """Example: PROFILE with single table scan"""
    print("\n" + "=" * 70)
    print("Example 1: PROFILE - Single Table Scan")
    print("=" * 70)

    query = "PROFILE MATCH (p:person) RETURN p.name, p.age"
    print(f"\nQuery: {query}\n")

    result = conn.execute(query)

    # Print query results
    print("--- Query Results ---")
    rows = list(result)  # Consume all rows
    if rows:
        print(f"Columns: {result.column_names()}")
        for i, row in enumerate(rows, 1):
            print(f"  Row {i}: {row}")
        print(f"Total rows returned: {len(rows)}\n")
    else:
        print("✗ No results returned\n")

    # Print profile information
    if result.has_profile_result():
        print("--- Formatted Text Output (suitable for CLI) ---")
        print(result.get_profile_text())

        print("\n--- Structured Metrics (suitable for programming) ---")
        metrics = result.get_profile_metrics()
        print(f"Total time: {metrics['total_elapsed_ms']:.2f} ms")
        print(f"Output rows: {metrics['total_output_rows']}")
        print(f"Number of operators: {len(metrics['operators'])}")

        for op in metrics["operators"]:
            print(
                f"  - {op['operator_name']}: {op['elapsed_ms']:.2f} ms, {op['output_rows']} rows"
            )
    else:
        print("✗ No profile result (query may have failed)")


def example_profile_with_join(conn):
    """Example: PROFILE with join operation"""
    print("\n" + "=" * 70)
    print("Example 2: PROFILE - Cross Product Join")
    print("=" * 70)

    query = "PROFILE MATCH (p1:person), (p2:person) WHERE p1.id < p2.id RETURN p1.name, p2.name"
    print(f"\nQuery: {query}\n")

    result = conn.execute(query)

    # Print query results
    print("--- Query Results (first 5 rows) ---")
    rows = list(result)
    print(f"Columns: {result.column_names()}")
    for i, row in enumerate(rows[:5], 1):
        print(f"  Row {i}: {row}")
    if len(rows) > 5:
        print(f"  ... ({len(rows) - 5} more rows)")
    print(f"Total rows returned: {len(rows)}\n")

    # Print profile information
    if result.has_profile_result():
        print("--- Formatted Text Output ---")
        print(result.get_profile_text())

        print("\n--- Structured Metrics ---")
        metrics = result.get_profile_metrics()
        print(f"Total time: {metrics['total_elapsed_ms']:.3f} ms")
        print(f"Output rows: {metrics['total_output_rows']}")
        print(f"Operators count: {len(metrics['operators'])}")


def example_profile_with_aggregation(conn):
    """Example: PROFILE with aggregation"""
    print("\n" + "=" * 70)
    print("Example 3: PROFILE - Aggregation")
    print("=" * 70)

    query = "PROFILE MATCH (p:person) RETURN COUNT(*) as person_count"
    print(f"\nQuery: {query}\n")

    result = conn.execute(query)

    # Print query results
    print("--- Query Results ---")
    rows = list(result)
    print(f"Columns: {result.column_names()}")
    for i, row in enumerate(rows, 1):
        print(f"  Row {i}: {row}")
    print()

    # Print profile information
    if result.has_profile_result():
        print("--- Formatted Text Output ---")
        print(result.get_profile_text())

        print("\n--- Structured Metrics ---")
        metrics = result.get_profile_metrics()
        print(f"Total time: {metrics['total_elapsed_ms']:.3f} ms")
        print(f"Output rows: {metrics['total_output_rows']}")


def example_explain_mode(conn):
    """Example: EXPLAIN mode (show plan without executing)"""
    print("\n" + "=" * 70)
    print("Example 4: EXPLAIN - Show Execution Plan")
    print("=" * 70)

    query = "EXPLAIN MATCH (p:person)-[e:knows]->(q:person) RETURN p.name, q.name"
    print(f"\nQuery: {query}\n")

    result = conn.execute(query)

    # Note: EXPLAIN mode doesn't execute, so no query results
    rows = list(result)
    print("--- Query Results ---")
    print(f"Rows returned: {len(rows)} (EXPLAIN mode doesn't execute)\n")

    if result.has_profile_result():
        print("--- Execution Plan (no execution) ---")
        print(result.get_profile_text())

        print("\n--- Metrics from EXPLAIN ---")
        metrics = result.get_profile_metrics()
        print(
            f"Total elapsed: {metrics['total_elapsed_ms']} ms (should be 0 for EXPLAIN)"
        )
        print(f"Output rows: {metrics['total_output_rows']} (should be 0 for EXPLAIN)")
        print(f"Plan operators: {len(metrics['operators'])}")


def example_dict_interface(conn):
    """Example: Working with dict metrics programmatically"""
    print("\n" + "=" * 70)
    print("Example 5: Dict Interface - Programmatic Access")
    print("=" * 70)

    query = "PROFILE MATCH (p:person) RETURN p.name"
    print(f"\nQuery: {query}\n")

    result = conn.execute(query)

    # Print query results
    print("--- Query Results ---")
    rows = list(result)
    print(f"Columns: {result.column_names()}")
    for i, row in enumerate(rows, 1):
        print(f"  Row {i}: {row}")
    print()

    if result.has_profile_result():
        metrics = result.get_profile_metrics()

        print("--- Building a performance report ---")
        print(f"Query took {metrics['total_elapsed_ms']:.2f} ms")
        print(f"Returned {metrics['total_output_rows']} rows\n")

        print("Operator breakdown:")
        for op in metrics["operators"]:
            parent_id = op["parent_id"]
            parent_str = (
                f" (parent: {parent_id})" if parent_id != -1 else " (root operator)"
            )
            print(f"  [{op['operator_id']}] {op['operator_name']}{parent_str}")
            print(f"       Time: {op['elapsed_ms']:.3f} ms, Rows: {op['output_rows']}")
            if op["child_ids"]:
                print(f"       Children: {op['child_ids']}")


def main():
    # Use provided db_dir from command line, or default to /tmp/neug_profile_example_db
    if len(sys.argv) > 2:
        print("Usage: python3 explain_profile_example.py [db_dir]")
        print("       db_dir defaults to /tmp/neug_profile_example_db if not provided")
        sys.exit(1)

    print(f"NeuG version {neug.__version__}\n")

    db_dir = sys.argv[1] if len(sys.argv) == 2 else "/tmp/neug_profile_example_db"
    shutil.rmtree(db_dir, ignore_errors=True)

    print(f"Creating database at {db_dir}")
    db = Database(db_dir, "w")
    conn = db.connect()

    try:
        # Load sample data
        load_sample_data(conn)

        # Run examples
        example_profile_single_scan(conn)
        example_profile_with_join(conn)
        example_profile_with_aggregation(conn)
        example_explain_mode(conn)
        example_dict_interface(conn)

        print("\n" + "=" * 70)
        print("✓ All examples completed successfully!")
        print("=" * 70)

    finally:
        conn.close()
        db.close()


if __name__ == "__main__":
    main()
