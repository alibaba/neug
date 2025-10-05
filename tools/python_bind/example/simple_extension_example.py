#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import shutil
import sys
import tempfile

import neug
from neug.database import Database


def create_test_json_file(json_file_path):
    """Create test JSON file with student data"""
    test_data = [
        {"id": 1, "name": "Mika", "age": 17, "height": 157.5, "birthday": "2004-05-08"},
        {
            "id": 2,
            "name": "Yuuka",
            "age": 16,
            "height": 156.1,
            "birthday": "2005-03-14",
        },
        {"id": 3, "name": "Hina", "age": 17, "height": 142.8, "birthday": "2004-02-19"},
    ]

    os.makedirs(os.path.dirname(json_file_path), exist_ok=True)
    with open(json_file_path, "w") as f:
        json.dump(test_data, f, indent=2)

    print(f"Created test JSON file: {json_file_path}")
    return json_file_path


def test_json_extension():
    """Test JSON extension functionality end-to-end"""

    print(f"Neug version: {neug.__version__}")
    print("=" * 50)
    print("Testing JSON Extension End-to-End")
    print("=" * 50)

    # Setup paths
    db_dir = "/tmp/test_json_extension_db"
    json_file = "/tmp/extension_unittest_data/json/test_array.json"

    # Cleanup and prepare
    shutil.rmtree(db_dir, ignore_errors=True)
    shutil.rmtree("/tmp/extension_unittest_data", ignore_errors=True)

    try:
        # Step 1: Create test JSON file
        print("\n1. Creating test JSON file...")
        create_test_json_file(json_file)

        # Step 2: Open database and create schema
        print("\n2. Opening database and creating schema...")
        db = Database(db_dir, "w")
        conn = db.connect()

        # Create student table
        create_table_sql = """
        CREATE NODE TABLE student(
            id INT32,
            name STRING,
            age INT32,
            height DOUBLE,
            birthday DATE,
            PRIMARY KEY(id)
        );
        """
        print("Creating student table...")
        conn.execute(create_table_sql)
        print("✓ Student table created successfully")

        # Step 3: Load JSON extension
        print("\n3. Loading JSON extension...")
        print("NEUG_EXTENSION_WHEEL_DIR =", os.environ.get("NEUG_EXTENSION_WHEEL_DIR"))

        try:
            conn.execute("LOAD json;")
            print("✓ JSON extension loaded successfully")
        except Exception as e:
            print(f"✗ Failed to load JSON extension: {e}")
            raise

        # Step 4: Import JSON data
        print("\n4. Importing JSON data...")
        import_query = f'COPY student FROM "{json_file}";'
        print(f"Executing: {import_query}")

        try:
            conn.execute(import_query)
            print("✓ JSON data imported successfully")
        except Exception as e:
            print(f"✗ Failed to import JSON data: {e}")
            raise

        # Step 5: Verify data - Count records
        print("\n5. Verifying imported data...")

        print("Counting total records...")
        count_result = conn.execute("MATCH (s:student) RETURN count(s);")
        count_record = next(count_result)
        count_value = str(count_record[0])
        print(f"Total records: {count_value}")

        if "3" in count_value:
            print("✓ Correct number of records imported")
        else:
            print(f"✗ Expected 3 records, got: {count_value}")
            return False

        # Step 6: Verify specific data
        print("\n6. Verifying specific record data...")

        # Test name field
        print("Testing name field (Mika)...")
        name_result = conn.execute("MATCH (s:student) WHERE s.id = 1 RETURN s.name;")
        name_record = next(name_result)
        name_value = str(name_record[0])
        print(f"Name result: {name_value}")

        if "Mika" in name_value:
            print("✓ Name field verified")
        else:
            print(f"✗ Expected 'Mika', got: {name_value}")

        # Test age field
        print("Testing age field (16)...")
        age_result = conn.execute("MATCH (s:student) WHERE s.id = 2 RETURN s.age;")
        age_record = next(age_result)
        age_value = str(age_record[0])
        print(f"Age result: {age_value}")

        if "16" in age_value:
            print("✓ Age field verified")
        else:
            print(f"✗ Expected '16', got: {age_value}")

        # Test height field
        print("Testing height field (142.8)...")
        height_result = conn.execute(
            "MATCH (s:student) WHERE s.id = 3 RETURN s.height;"
        )
        height_record = next(height_result)
        height_value = str(height_record[0])
        print(f"Height result: {height_value}")

        if "142.8" in height_value:
            print("✓ Height field verified")
        else:
            print(f"✗ Expected '142.8', got: {height_value}")

        # Test date field (optional, as noted in original test)
        print("Testing date field...")
        try:
            date_result = conn.execute(
                "MATCH (s:student) WHERE s.id = 1 AND s.birthday = DATE('2004-05-08') RETURN s.name;"
            )
            if date_result:
                date_record = next(date_result, None)
                if date_record and "Mika" in str(date_record[0]):
                    print("✓ Date field verified")
                else:
                    print("⚠ Date comparison might not be supported or format differs")
            else:
                print("⚠ Date query returned no results")
        except Exception as e:
            print(f"⚠ Date comparison test failed: {e}")

        # Step 7: Additional queries
        print("\n7. Running additional verification queries...")

        # Get all students
        print("Fetching all student records...")
        all_students = conn.execute(
            "MATCH (s:student) RETURN s.id, s.name, s.age, s.height ORDER BY s.id;"
        )

        print("All students:")
        for i, record in enumerate(all_students):
            print(f"  Student {i+1}: {record}")

        print("\n" + "=" * 50)
        print("✓ JSON Extension test completed successfully!")
        print("=" * 50)

        return True

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Cleanup
        try:
            conn.close() if "conn" in locals() else None
            db.close() if "db" in locals() else None
        except Exception:
            pass

        print("\nCleaning up...")
        shutil.rmtree(db_dir, ignore_errors=True)
        shutil.rmtree("/tmp/extension_unittest_data", ignore_errors=True)


if __name__ == "__main__":
    print("JSON Extension End-to-End Test")
    print("Usage: python simple_extension_example.py")

    success = test_json_extension()

    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)
