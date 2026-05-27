#!/usr/bin/env python3
"""
Test script for Louvain algorithm with custom output properties.

This test demonstrates:
1. Basic louvain call without properties
2. Louvain call with custom vertex properties
"""

import os
import sys

# Add the build directory to Python path
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "build/lib.macosx-14.0-arm64-cpython-314"),
)

from neug import Connection
from neug import Database


def test_louvain_with_properties():
    """Test louvain algorithm with custom property output."""

    # Create an in-memory database
    db = Database()
    conn = db.connect()

    print("=" * 60)
    print("Test 1: Create a simple graph with properties")
    print("=" * 60)

    # Create vertex labels with properties
    conn.execute(
        """
        CREATE NODE TABLE person (
            id INT64,
            name STRING,
            age INT64,
            city STRING,
            PRIMARY KEY (id)
        )
    """
    )

    conn.execute("CREATE REL TABLE knows (FROM person TO person, weight DOUBLE)")

    print("✓ Created graph schema")

    # Insert some test data
    conn.execute("CREATE (:person {id: 1, name: 'Alice', age: 30, city: 'Beijing'});")
    conn.execute("CREATE (:person {id: 2, name: 'Bob', age: 25, city: 'Shanghai'});")
    conn.execute("CREATE (:person {id: 3, name: 'Charlie', age: 35, city: 'Beijing'});")
    conn.execute("CREATE (:person {id: 4, name: 'David', age: 28, city: 'Shanghai'});")
    conn.execute("CREATE (:person {id: 5, name: 'Eve', age: 32, city: 'Beijing'});")

    # Create connections (Alice, Charlie, Eve are in Beijing - likely same community)
    # (Bob, David are in Shanghai - likely same community)
    conn.execute(
        "MATCH (a:person {id: 1}), (b:person {id: 2}) CREATE (a)-[:knows {weight: 1.0}]->(b);"
    )
    conn.execute(
        "MATCH (a:person {id: 1}), (b:person {id: 3}) CREATE (a)-[:knows {weight: 1.0}]->(b);"
    )
    conn.execute(
        "MATCH (a:person {id: 2}), (b:person {id: 4}) CREATE (a)-[:knows {weight: 1.0}]->(b);"
    )
    conn.execute(
        "MATCH (a:person {id: 3}), (b:person {id: 5}) CREATE (a)-[:knows {weight: 1.0}]->(b);"
    )
    conn.execute(
        "MATCH (a:person {id: 4}), (b:person {id: 5}) CREATE (a)-[:knows {weight: 1.0}]->(b);"
    )

    print("✓ Inserted test data")

    print("\n" + "=" * 60)
    print("Test 2: Load Louvain extension")
    print("=" * 60)

    conn.execute("LOAD LOUVAIN")
    print("✓ Loaded louvain extension")

    print("\n" + "=" * 60)
    print("Test 3: Run Louvain WITHOUT custom properties")
    print("=" * 60)

    result1 = conn.execute(
        """
        CALL louvain()
    """
    )

    # Get column names
    col_names = result1.column_names()
    print(f"Column names: {col_names}")

    rows1 = list(result1)
    print(f"Total rows: {len(rows1)}")
    print("\nResults:")
    for i, row in enumerate(rows1[:3]):  # Show first 3 rows
        print(f"  Row {i}: {row}")

    if len(rows1) > 3:
        print(f"  ... and {len(rows1) - 3} more rows")

    print("\n" + "=" * 60)
    print("Test 4: Run Louvain WITH custom properties (name,age,city)")
    print("=" * 60)

    result2 = conn.execute(
        """
        CALL louvain(1.0, false, 1e-7, 'name,age,city')
    """
    )

    rows2 = list(result2)
    print(f"Total rows: {len(rows2)}")
    print("\nResults:")
    for row in rows2:
        print(f"  node_id={row[0]}, community={row[1]}")
        print(f"  properties={row[2]}")

    print("\n" + "=" * 60)
    print("Test 5: Run Louvain with different resolution parameter")
    print("=" * 60)

    result3 = conn.execute(
        """
        CALL louvain(0.5, false, 1e-7, 'name,city')
    """
    )

    rows3 = list(result3)
    print(f"Total rows: {len(rows3)}")
    print("\nResults:")
    for row in rows3:
        print(f"  node_id={row[0]}, community={row[1]}")
        print(f"  properties={row[2]}")

    print("\n" + "=" * 60)
    print("All tests completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    try:
        test_louvain_with_properties()
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
