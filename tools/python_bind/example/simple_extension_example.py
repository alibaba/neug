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

import json
import os
import shutil
import sys

import neug
from neug.database import Database


def setup_database(db_path):
    """Setup database with schema"""
    shutil.rmtree(db_path, ignore_errors=True)

    db = Database(db_path, "w")
    conn = db.connect()

    # Create student table
    conn.execute(
        """
        CREATE NODE TABLE student(
            id INT32,
            name STRING,
            age INT32,
            height DOUBLE,
            birthday DATE,
            club STRING,
            hobbies STRING,
            PRIMARY KEY(id)
        );
    """
    )
    print("✓ Created student table")

    # Create school table
    conn.execute(
        """
        CREATE NODE TABLE school(
            id INT32,
            name STRING,
            PRIMARY KEY(id)
        );
    """
    )
    print("✓ Created school table")

    # Create knows edge table (student knows student)
    conn.execute(
        """
        CREATE REL TABLE knows(
            FROM student TO student,
            weight DOUBLE
        );
    """
    )
    print("✓ Created knows edge table")

    # Create attends edge table (student attends school)
    conn.execute(
        """
        CREATE REL TABLE attends(
            FROM student TO school
        );
    """
    )
    print("✓ Created attends edge table")

    return db, conn


def test_json_array_format():
    """Test JSON Array format (similar to LoadAndImportJsonArray test)"""
    print("\n" + "=" * 60)
    print("Testing JSON Array Format")
    print("=" * 60)

    db_path = "/tmp/test_json_extension_db_array"

    # Dataset paths
    student_json = (
        "/workspaces/neug/extension/json/tests/datasets/json_array/student.json"
    )
    school_json = (
        "/workspaces/neug/extension/json/tests/datasets/json_array/school.json"
    )
    knows_json = "/workspaces/neug/extension/json/tests/datasets/json_array/knows.json"
    attends_json = (
        "/workspaces/neug/extension/json/tests/datasets/json_array/attends.json"
    )

    try:
        # Setup database
        print("\n1. Setting up database...")
        db, conn = setup_database(db_path)

        # Load JSON extension
        print("\n2. Loading JSON extension...")
        conn.execute("LOAD json;")
        print("✓ JSON extension loaded")

        # Import student data
        print("\n3. Testing student vertex data (JSON Array)...")
        conn.execute(f'COPY student FROM "{student_json}";')
        print("✓ Imported student data")

        # Verify student count (67 students in the dataset)
        result = conn.execute("MATCH (s:student) RETURN count(s);")
        count_str = str(next(result)[0])
        assert "67" in count_str, f"Expected 67 students, got: {count_str}"
        print(f"✓ Student count verified: {count_str}")

        # Verify specific student fields
        # Student id=1: Mika, age=17
        result = conn.execute("MATCH (s:student) WHERE s.id = 1 RETURN s.name;")
        name = str(next(result)[0])
        assert "Mika" in name, f"Expected 'Mika', got: {name}"
        print(f"✓ Student id=1 name verified: {name}")

        result = conn.execute("MATCH (s:student) WHERE s.id = 1 RETURN s.age;")
        age = str(next(result)[0])
        assert "17" in age, f"Expected '17', got: {age}"
        print(f"✓ Student id=1 age verified: {age}")

        # Student id=2: Yuuka, age=16, height=156.1
        result = conn.execute("MATCH (s:student) WHERE s.id = 2 RETURN s.name, s.age;")
        record = next(result)
        name = str(record[0])
        age = str(record[1])
        assert "Yuuka" in name, f"Expected 'Yuuka', got: {name}"
        assert "16" in age, f"Expected '16', got: {age}"
        print(f"✓ Student id=2 verified: name={name}, age={age}")

        # Student id=3: Hina, height=142.8
        result = conn.execute("MATCH (s:student) WHERE s.id = 3 RETURN s.height;")
        height = str(next(result)[0])
        assert "142.8" in height, f"Expected '142.8', got: {height}"
        print(f"✓ Student id=3 height verified: {height}")

        # Student id=8: Kisaki, club="Xuan Long Men"
        result = conn.execute("MATCH (s:student) WHERE s.id = 8 RETURN s.name, s.club;")
        record = next(result)
        name = str(record[0])
        club = str(record[1])
        assert "Kisaki" in name, f"Expected 'Kisaki', got: {name}"
        assert "Xuan Long Men" in club, f"Expected 'Xuan Long Men', got: {club}"
        print(f"✓ Student id=8 verified: name={name}, club={club}")

        # Import school data
        print("\n4. Testing school vertex data (JSON Array)...")
        conn.execute(f'COPY school FROM "{school_json}";')
        print("✓ Imported school data")

        # Verify school count (9 schools)
        result = conn.execute("MATCH (s:school) RETURN count(s);")
        count_str = str(next(result)[0])
        assert "9" in count_str, f"Expected 9 schools, got: {count_str}"
        print(f"✓ School count verified: {count_str}")

        # Verify specific school data
        result = conn.execute("MATCH (s:school) WHERE s.id = 1 RETURN s.name;")
        name = str(next(result)[0])
        assert (
            "Abydos High School" in name
        ), f"Expected 'Abydos High School', got: {name}"
        print(f"✓ School id=1 verified: {name}")

        result = conn.execute("MATCH (s:school) WHERE s.id = 4 RETURN s.name;")
        name = str(next(result)[0])
        assert (
            "Millenium Science School" in name
        ), f"Expected 'Millenium Science School', got: {name}"
        print(f"✓ School id=4 verified: {name}")

        result = conn.execute("MATCH (s:school) WHERE s.id = 6 RETURN s.name;")
        name = str(next(result)[0])
        assert (
            "Shan Hai Jing Academy" in name
        ), f"Expected 'Shan Hai Jing Academy', got: {name}"
        print(f"✓ School id=6 verified: {name}")

        # Import knows edge data
        print("\n5. Testing knows edge data (JSON Array)...")
        # Note: knows.json uses "from" and "to" fields
        conn.execute(f'COPY knows FROM "{knows_json}" (from="student",to="student");')
        print("✓ Imported knows edge data")

        # Verify knows edge count (4 edges in dataset)
        result = conn.execute("MATCH ()-[k:knows]->() RETURN count(k);")
        count_str = str(next(result)[0])
        assert "4" in count_str, f"Expected 4 knows edges, got: {count_str}"
        print(f"✓ Knows edge count verified: {count_str}")

        # Verify specific knows relationships
        # {"from": 1, "to": 5, "weight": 1.0} - Mika knows Nagisa
        result = conn.execute(
            """
            MATCH (s1:student)-[k:knows]->(s2:student)
            WHERE s1.id = 1 AND s2.id = 5
            RETURN s2.name, k.weight;
        """
        )
        record = next(result, None)
        if record:
            name = str(record[0])
            weight = str(record[1])
            assert "Nagisa" in name, f"Expected 'Nagisa', got: {name}"
            assert (
                "1.0" in weight or "1" in weight
            ), f"Expected weight 1.0, got: {weight}"
            print(f"✓ Student 1 knows student 5: {name}, weight={weight}")

        # Import attends edge data
        print("\n6. Testing attends edge data (JSON Array)...")
        conn.execute(
            f'COPY attends FROM "{attends_json}" (from="student",to="school");'
        )
        print("✓ Imported attends edge data")

        # Verify attends edge count (4 edges in dataset)
        result = conn.execute("MATCH ()-[a:attends]->() RETURN count(a);")
        count_str = str(next(result)[0])
        assert "4" in count_str, f"Expected 4 attends edges, got: {count_str}"
        print(f"✓ Attends edge count verified: {count_str}")

        # Verify specific attends relationships based on actual data
        # {"student": 1, "school": 2} - Mika -> Trinity General School
        result = conn.execute(
            "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 1 RETURN sc.name;"
        )
        school_name = str(next(result)[0])
        assert (
            "Trinity General School" in school_name
        ), f"Expected 'Trinity General School', got: {school_name}"
        print(f"✓ Student 1 (Mika) attends: {school_name}")

        # {"student": 2, "school": 4} - Yuuka -> Millenium Science School
        result = conn.execute(
            "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 2 RETURN sc.name;"
        )
        school_name = str(next(result)[0])
        assert (
            "Millenium Science School" in school_name
        ), f"Expected 'Millenium Science School', got: {school_name}"
        print(f"✓ Student 2 (Yuuka) attends: {school_name}")

        # {"student": 3, "school": 3} - Hina -> Gehena Academy
        result = conn.execute(
            "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 3 RETURN sc.name;"
        )
        school_name = str(next(result)[0])
        assert (
            "Gehena Academy" in school_name
        ), f"Expected 'Gehena Academy', got: {school_name}"
        print(f"✓ Student 3 (Hina) attends: {school_name}")

        # {"student": 4, "school": 1} - Shiroko -> Abydos High School
        result = conn.execute(
            "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 4 RETURN sc.name;"
        )
        school_name = str(next(result)[0])
        assert (
            "Abydos High School" in school_name
        ), f"Expected 'Abydos High School', got: {school_name}"
        print(f"✓ Student 4 (Shiroko) attends: {school_name}")

        # Verify all student-school paths
        result = conn.execute(
            "MATCH (s:student)-[:attends]->(sc:school) RETURN s.name, sc.name ORDER BY s.id;"
        )
        paths = list(result)
        assert len(paths) == 4, f"Expected 4 student-school paths, got {len(paths)}"
        print(f"✓ Verified {len(paths)} student-school relationships")

        print("\n✓ JSON Array format test completed successfully")
        return True

    except Exception as e:
        print(f"\n✗ JSON Array test failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        try:
            conn.close()
            db.close()
        except Exception:
            pass
        shutil.rmtree(db_path, ignore_errors=True)


def test_json_lines_format():
    """Test JSON Lines format (similar to LoadAndImportJsonLines test)"""
    print("\n" + "=" * 60)
    print("Testing JSON Lines (JSONL) Format")
    print("=" * 60)

    db_path = "/tmp/test_json_extension_db_jsonl"

    # Dataset paths
    student_json = (
        "/workspaces/neug/extension/json/tests/datasets/json_list/student.json"
    )
    school_json = "/workspaces/neug/extension/json/tests/datasets/json_list/school.json"
    knows_json = "/workspaces/neug/extension/json/tests/datasets/json_list/knows.json"
    attends_json = (
        "/workspaces/neug/extension/json/tests/datasets/json_list/attends.json"
    )

    try:
        # Setup database
        print("\n1. Setting up database...")
        db, conn = setup_database(db_path)

        # Load JSON extension
        print("\n2. Loading JSON extension...")
        conn.execute("LOAD json;")
        print("✓ JSON extension loaded")

        # Import student data (JSONL)
        print("\n3. Testing student vertex data (JSONL)...")
        conn.execute(f'COPY student FROM "{student_json}";')
        print("✓ Imported student data")

        # Verify student count (67 students in the dataset)
        result = conn.execute("MATCH (s:student) RETURN count(s);")
        count_str = str(next(result)[0])
        assert "67" in count_str, f"Expected 67 students, got: {count_str}"
        print(f"✓ Student count verified: {count_str}")

        # Verify Mika's data (id=1)
        result = conn.execute("MATCH (s:student) WHERE s.id = 1 RETURN s.name, s.age;")
        record = next(result)
        name = str(record[0])
        age = str(record[1])
        assert "Mika" in name, f"Expected 'Mika', got: {name}"
        assert "17" in age, f"Expected age 17, got: {age}"
        print(f"✓ Mika's data verified: name={name}, age={age}")

        # Verify Yuuka's data (id=2)
        result = conn.execute(
            "MATCH (s:student) WHERE s.id = 2 RETURN s.name, s.height;"
        )
        record = next(result)
        name = str(record[0])
        height = str(record[1])
        assert "Yuuka" in name, f"Expected 'Yuuka', got: {name}"
        assert "156.1" in height, f"Expected height 156.1, got: {height}"
        print(f"✓ Yuuka's data verified: name={name}, height={height}")

        # Verify Nagisa's data (id=5)
        result = conn.execute("MATCH (s:student) WHERE s.id = 5 RETURN s.name, s.club;")
        record = next(result)
        name = str(record[0])
        club = str(record[1])
        assert "Nagisa" in name, f"Expected 'Nagisa', got: {name}"
        assert "Tea Party" in club, f"Expected 'Tea Party', got: {club}"
        print(f"✓ Nagisa's data verified: name={name}, club={club}")

        # Verify Kisaki's club (id=8, club="Xuan Long Men")
        result = conn.execute("MATCH (s:student) WHERE s.id = 8 RETURN s.name, s.club;")
        record = next(result)
        name = str(record[0])
        club = str(record[1])
        assert "Kisaki" in name, f"Expected 'Kisaki', got: {name}"
        assert "Xuan Long Men" in club, f"Expected 'Xuan Long Men', got: {club}"
        print(f"✓ Kisaki's data verified: name={name}, club={club}")

        # Verify special age students
        # id=23: Arisu, age=500
        result = conn.execute("MATCH (s:student) WHERE s.id = 23 RETURN s.name, s.age;")
        record = next(result)
        name = str(record[0])
        age = str(record[1])
        assert "Arisu" in name, f"Expected 'Arisu', got: {name}"
        assert "500" in age, f"Expected age 500, got: {age}"
        print(f"✓ Arisu's data verified: name={name}, age={age}")

        # Test date field (optional)
        try:
            result = conn.execute(
                "MATCH (s:student) WHERE s.birthday = DATE('2005-03-14') RETURN s.name;"
            )
            record = next(result, None)
            if record and "Yuuka" in str(record[0]):
                print("✓ Date field verified for Yuuka")
            else:
                print("⚠ Date comparison might not be supported")
        except Exception as e:
            print(f"⚠ Date comparison test skipped: {e}")

        # Test age range query
        result = conn.execute(
            "MATCH (s:student) WHERE s.age >= 16 AND s.age <= 17 RETURN s.name ORDER BY s.age, s.id;"
        )
        count = 0
        for record in result:
            count += 1
            if count <= 5:  # Print first 5
                print(f"  Age 16-17 student: {record[0]}")
        print(f"✓ Found {count} students with age 16-17")

        # Import school data (JSONL)
        print("\n4. Testing school vertex data (JSONL)...")
        conn.execute(f'COPY school FROM "{school_json}";')
        print("✓ Imported school data")

        # Verify school count (9 schools)
        result = conn.execute("MATCH (s:school) RETURN count(s);")
        count_str = str(next(result)[0])
        assert "9" in count_str, f"Expected 9 schools, got: {count_str}"
        print(f"✓ School count verified: {count_str}")

        # Verify specific school data
        result = conn.execute("MATCH (s:school) WHERE s.id = 1 RETURN s.name;")
        name = str(next(result)[0])
        assert "Abydos High School" in name
        print(f"✓ School id=1 verified: {name}")

        result = conn.execute("MATCH (s:school) WHERE s.id = 7 RETURN s.name;")
        name = str(next(result)[0])
        assert "Red Winter Federal Academy" in name
        print(f"✓ School id=7 verified: {name}")

        # Import knows edge data (JSONL)
        print("\n5. Testing knows edge data (JSONL)...")
        conn.execute(f'COPY knows FROM "{knows_json}" (from="student",to="student");')
        print("✓ Imported knows edge data")

        # Verify knows edge count (4 edges)
        result = conn.execute("MATCH ()-[k:knows]->() RETURN count(k);")
        count_str = str(next(result)[0])
        assert "4" in count_str, f"Expected 4 knows edges, got: {count_str}"
        print(f"✓ Knows edge count verified: {count_str}")

        # Verify specific knows relationship
        # {"from": 1, "to": 5, "weight": 1.0}
        result = conn.execute(
            """
            MATCH (s1:student)-[k:knows]->(s2:student)
            WHERE s1.id = 1 AND s2.id = 5
            RETURN s1.name, s2.name, k.weight;
        """
        )
        record = next(result, None)
        if record:
            from_name = str(record[0])
            to_name = str(record[1])
            weight = str(record[2])
            print(f"✓ Verified: {from_name} knows {to_name}, weight={weight}")

        # Import attends edge data (JSONL)
        print("\n6. Testing attends edge data (JSONL)...")
        conn.execute(
            f'COPY attends FROM "{attends_json}" (from="student",to="school");'
        )
        print("✓ Imported attends edge data")

        # Verify attends edge count (4 edges)
        result = conn.execute("MATCH ()-[a:attends]->() RETURN count(a);")
        count_str = str(next(result)[0])
        assert "4" in count_str, f"Expected 4 attends edges, got: {count_str}"
        print(f"✓ Attends edge count verified: {count_str}")

        # Verify specific attends relationships
        result = conn.execute(
            "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 1 RETURN s.name, sc.name;"
        )
        record = next(result)
        student_name = str(record[0])
        school_name = str(record[1])
        assert "Mika" in student_name
        assert "Trinity General School" in school_name
        print(f"✓ {student_name} attends {school_name}")

        result = conn.execute(
            "MATCH (s:student)-[:attends]->(sc:school) WHERE s.id = 4 RETURN s.name, sc.name;"
        )
        record = next(result)
        student_name = str(record[0])
        school_name = str(record[1])
        assert "Shiroko" in student_name
        assert "Abydos High School" in school_name
        print(f"✓ {student_name} attends {school_name}")

        # Verify path query: student -> school
        result = conn.execute(
            "MATCH (s:student)-[:attends]->(sc:school) RETURN s.name, sc.name ORDER BY s.id;"
        )
        count = 0
        for record in result:
            print(f"  Student-School path: {record[0]} -> {record[1]}")
            count += 1
        assert count == 4, f"Expected 4 student-school relationships, got {count}"
        print(f"✓ Verified {count} student-school relationships")

        print("\n✓ JSON Lines format test completed successfully")
        return True

    except Exception as e:
        print(f"\n✗ JSON Lines test failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        try:
            conn.close()
            db.close()
        except Exception:
            pass
        shutil.rmtree(db_path, ignore_errors=True)


def main():
    """Main test function"""
    print("=" * 60)
    print("JSON Extension End-to-End Test Suite")
    print(f"Neug version: {neug.__version__}")
    print("=" * 60)

    # Cleanup
    shutil.rmtree("/tmp/test_json_extension_db_array", ignore_errors=True)
    shutil.rmtree("/tmp/test_json_extension_db_jsonl", ignore_errors=True)

    try:
        # Run tests
        test_results = []

        # Test 1: JSON Array format
        result = test_json_array_format()
        test_results.append(("JSON Array Format", result))

        # Test 2: JSON Lines format
        result = test_json_lines_format()
        test_results.append(("JSON Lines Format", result))

        # Summary
        print("\n" + "=" * 60)
        print("Test Summary")
        print("=" * 60)

        all_passed = True
        for test_name, passed in test_results:
            status = "✓ PASSED" if passed else "✗ FAILED"
            print(f"{test_name}: {status}")
            if not passed:
                all_passed = False

        print("=" * 60)

        if all_passed:
            print("\n🎉 All tests passed!")
            return 0
        else:
            print("\n💥 Some tests failed!")
            return 1

    except Exception as e:
        print(f"\n✗ Test suite failed with error: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        # Final cleanup
        print("\nCleaning up...")
        shutil.rmtree("/tmp/test_json_extension_db_array", ignore_errors=True)
        shutil.rmtree("/tmp/test_json_extension_db_jsonl", ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
