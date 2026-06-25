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

"""E2E tests for ExampleIndex via the example_index extension.

Tests the index framework through:
- CREATE INDEX DDL
- Append / Delete / Update index data
- COPY FROM first, then CREATE INDEX
- CREATE INDEX first, then COPY FROM
- CREATE INDEX first, then COPY FROM multiple times (incremental)
"""

import csv
import os
import shutil
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug import Database


@pytest.fixture
def db_dir(tmp_path):
    """Provide a temporary directory for the database."""
    d = str(tmp_path / "test_example_index")
    shutil.rmtree(d, ignore_errors=True)
    yield d
    shutil.rmtree(d, ignore_errors=True)


def make_db(db_dir):
    db = Database(db_dir, "w")
    conn = db.connect()
    conn.execute("LOAD example_index;")
    return db, conn


def create_person_table(conn):
    conn.execute(
        "CREATE NODE TABLE Person(" "id INT64 PRIMARY KEY, name STRING, age INT32);"
    )


def write_csv(path, rows):
    """Write rows (list of dicts) to a CSV file with header."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


PERSONS = [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 30},
    {"id": 4, "name": "Diana", "age": 40},
    {"id": 5, "name": "Eve", "age": 25},
]


class TestCreateIndex:
    """Test CREATE INDEX DDL with the example_index extension."""

    def test_create_index_empty_graph(self, db_dir):
        """CREATE INDEX on an empty table should succeed."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")
        conn.close()
        db.close()

    def test_create_index_if_not_exists(self, db_dir):
        """CREATE INDEX IF NOT EXISTS should not error on duplicate."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_person_age ON Person USING EXAMPLE (age);"
        )
        conn.close()
        db.close()

    def test_create_index_duplicate_errors(self, db_dir):
        """CREATE INDEX with same name without IF NOT EXISTS should raise."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")
        with pytest.raises(RuntimeError):
            conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")
        conn.close()
        db.close()

    def test_create_index_after_inserts(self, db_dir):
        """CREATE INDEX after inserting data should index existing vertices."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        for p in PERSONS:
            conn.execute(
                f"CREATE (:Person {{id: {p['id']}, name: '{p['name']}', age: {p['age']}}});"
            )
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")
        conn.close()
        db.close()


class TestAppendDeleteUpdate:
    """Test index data maintenance through INSERT / DELETE / SET."""

    def test_insert_updates_index(self, db_dir):
        """Inserting a vertex after index creation should be searchable."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")
        conn.execute("CREATE (:Person {id: 1, name: 'Alice', age: 30});")
        conn.execute("CREATE (:Person {id: 2, name: 'Bob', age: 25});")
        conn.execute("CREATE (:Person {id: 3, name: 'Charlie', age: 30});")

        result = conn.execute(
            "MATCH (p:Person) WHERE p.age = 30 RETURN p.name ORDER BY p.name;"
        )
        names = sorted([row[0] for row in result])
        assert names == ["Alice", "Charlie"], f"Expected Alice and Charlie, got {names}"
        conn.close()
        db.close()

    def test_delete_updates_index(self, db_dir):
        """Deleting a vertex should remove it from index results."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")
        conn.execute("CREATE (:Person {id: 1, name: 'Alice', age: 30});")
        conn.execute("CREATE (:Person {id: 2, name: 'Bob', age: 30});")

        conn.execute("MATCH (p:Person {id: 1}) DELETE p;")

        result = conn.execute("MATCH (p:Person) WHERE p.age = 30 RETURN p.name;")
        names = [row[0] for row in result]
        assert names == ["Bob"], f"Expected only Bob after delete, got {names}"
        conn.close()
        db.close()

    def test_update_property_reflects_in_index(self, db_dir):
        """Updating a property value should be reflected in index search."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")
        conn.execute("CREATE (:Person {id: 1, name: 'Alice', age: 30});")
        conn.execute("CREATE (:Person {id: 2, name: 'Bob', age: 25});")

        conn.execute("MATCH (p:Person {id: 1}) SET p.age = 25;")

        result = conn.execute(
            "MATCH (p:Person) WHERE p.age = 25 RETURN p.name ORDER BY p.name;"
        )
        names = sorted([row[0] for row in result])
        assert "Alice" in names, f"Alice should have age 25 now, got {names}"
        assert "Bob" in names, f"Bob should still have age 25, got {names}"

        result = conn.execute("MATCH (p:Person) WHERE p.age = 30 RETURN p.name;")
        names = [row[0] for row in result]
        assert names == [], f"No one should have age 30 now, got {names}"
        conn.close()
        db.close()


class TestCopyFromThenCreateIndex:
    """Test: load data via COPY FROM first, then create the index."""

    def test_copy_from_then_create_index(self, db_dir):
        """Index should cover data loaded before index creation."""
        db, conn = make_db(db_dir)
        create_person_table(conn)

        csv_path = os.path.join(db_dir, "persons.csv")
        write_csv(csv_path, PERSONS)
        conn.execute(f"COPY Person FROM '{csv_path}' (HEADER TRUE, DELIMITER=',');")

        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")

        result = conn.execute(
            "MATCH (p:Person) WHERE p.age = 25 RETURN p.name ORDER BY p.name;"
        )
        names = sorted([row[0] for row in result])
        assert names == ["Bob", "Eve"], f"Expected Bob and Eve with age 25, got {names}"
        conn.close()
        db.close()


class TestCreateIndexThenCopyFrom:
    """Test: create the index first, then load data via COPY FROM."""

    def test_create_index_then_copy_from(self, db_dir):
        """Data loaded after index creation should be indexed."""
        db, conn = make_db(db_dir)
        create_person_table(conn)

        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")

        csv_path = os.path.join(db_dir, "persons.csv")
        write_csv(csv_path, PERSONS)
        conn.execute(f"COPY Person FROM '{csv_path}' (HEADER TRUE, DELIMITER=',');")

        result = conn.execute(
            "MATCH (p:Person) WHERE p.age = 30 RETURN p.name ORDER BY p.name;"
        )
        names = sorted([row[0] for row in result])
        assert names == [
            "Alice",
            "Charlie",
        ], f"Expected Alice and Charlie with age 30, got {names}"
        conn.close()
        db.close()


class TestIncrementalCopyFrom:
    """Test: create index, then COPY FROM multiple times (incremental)."""

    def test_incremental_copy_from(self, db_dir):
        """Multiple COPY FROM after index creation should accumulate data."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")

        batch1 = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]
        csv1 = os.path.join(db_dir, "batch1.csv")
        write_csv(csv1, batch1)
        conn.execute(f"COPY Person FROM '{csv1}' (HEADER TRUE, DELIMITER=',');")

        result = conn.execute("MATCH (p:Person) WHERE p.age = 30 RETURN p.name;")
        names = [row[0] for row in result]
        assert names == ["Alice"], f"After batch1, expected [Alice], got {names}"

        batch2 = [
            {"id": 3, "name": "Charlie", "age": 30},
            {"id": 4, "name": "Diana", "age": 40},
        ]
        csv2 = os.path.join(db_dir, "batch2.csv")
        write_csv(csv2, batch2)
        conn.execute(f"COPY Person FROM '{csv2}' (HEADER TRUE, DELIMITER=',');")

        result = conn.execute(
            "MATCH (p:Person) WHERE p.age = 30 RETURN p.name ORDER BY p.name;"
        )
        names = sorted([row[0] for row in result])
        assert names == [
            "Alice",
            "Charlie",
        ], f"After batch2, expected [Alice, Charlie], got {names}"

        batch3 = [
            {"id": 5, "name": "Eve", "age": 25},
        ]
        csv3 = os.path.join(db_dir, "batch3.csv")
        write_csv(csv3, batch3)
        conn.execute(f"COPY Person FROM '{csv3}' (HEADER TRUE, DELIMITER=',');")

        result = conn.execute(
            "MATCH (p:Person) WHERE p.age = 25 RETURN p.name ORDER BY p.name;"
        )
        names = sorted([row[0] for row in result])
        assert names == [
            "Bob",
            "Eve",
        ], f"After batch3, expected [Bob, Eve], got {names}"

        result = conn.execute("MATCH (p:Person) RETURN count(p);")
        total = list(result)[0][0]
        assert total == 5, f"Expected 5 total persons, got {total}"

        conn.close()
        db.close()

    def test_incremental_copy_from_duplicate_pk(self, db_dir):
        """COPY FROM with duplicate PKs across batches: only new PKs are
        appended to the index; existing PKs keep their original index entry."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")

        batch1 = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]
        csv1 = os.path.join(db_dir, "dup_batch1.csv")
        write_csv(csv1, batch1)
        conn.execute(f"COPY Person FROM '{csv1}' (HEADER TRUE, DELIMITER=',');")

        result = conn.execute("MATCH (p:Person) WHERE p.age = 30 RETURN p.name;")
        assert [row[0] for row in result] == ["Alice"]

        # batch2: id=1 appears again with age=40 (duplicate PK),
        # id=3 is genuinely new
        batch2 = [
            {"id": 1, "name": "Alice", "age": 40},
            {"id": 3, "name": "Charlie", "age": 30},
        ]
        csv2 = os.path.join(db_dir, "dup_batch2.csv")
        write_csv(csv2, batch2)
        conn.execute(f"COPY Person FROM '{csv2}' (HEADER TRUE, DELIMITER=',');")

        # Duplicate pk=1 is skipped, so Alice keeps age=30;
        # Charlie (new pk=3) is appended with age=30
        result = conn.execute(
            "MATCH (p:Person) WHERE p.age = 30 RETURN p.name ORDER BY p.name;"
        )
        names_30 = sorted([row[0] for row in result])
        assert names_30 == [
            "Alice",
            "Charlie",
        ], f"Expected [Alice, Charlie] for age=30, got {names_30}"

        result = conn.execute("MATCH (p:Person) WHERE p.age = 25 RETURN p.name;")
        names_25 = [row[0] for row in result]
        assert names_25 == ["Bob"], f"Expected [Bob] for age=25, got {names_25}"

        # age=40 should not exist — the duplicate row was skipped entirely
        result = conn.execute("MATCH (p:Person) WHERE p.age = 40 RETURN p.name;")
        names_40 = [row[0] for row in result]
        assert names_40 == [], f"Expected no results for age=40, got {names_40}"

        result = conn.execute("MATCH (p:Person) RETURN count(p);")
        total = list(result)[0][0]
        assert total == 3, f"Expected 3 total persons, got {total}"

        conn.close()
        db.close()


class TestDbReopen:
    """Test that index data survives database close and reopen."""

    def test_index_persists_after_reopen(self, db_dir):
        """Index created and populated before close should be queryable
        after reopening the database."""
        db, conn = make_db(db_dir)
        create_person_table(conn)
        conn.execute("CREATE INDEX idx_person_age ON Person USING EXAMPLE (age);")
        for p in PERSONS:
            conn.execute(
                f"CREATE (:Person {{id: {p['id']}, name: '{p['name']}', age: {p['age']}}});"
            )

        # Sanity check before close
        result = conn.execute(
            "MATCH (p:Person) WHERE p.age = 30 RETURN p.name ORDER BY p.name;"
        )
        names = sorted([row[0] for row in result])
        assert names == ["Alice", "Charlie"]

        conn.close()
        db.close()

        # Reopen the database and reload the extension
        db2 = Database(db_dir, "w")
        conn2 = db2.connect()
        conn2.execute("LOAD example_index;")

        result = conn2.execute(
            "MATCH (p:Person) WHERE p.age = 30 RETURN p.name ORDER BY p.name;"
        )
        names = sorted([row[0] for row in result])
        assert names == [
            "Alice",
            "Charlie",
        ], f"After reopen, expected [Alice, Charlie] for age=30, got {names}"

        result = conn2.execute(
            "MATCH (p:Person) WHERE p.age = 25 RETURN p.name ORDER BY p.name;"
        )
        names = sorted([row[0] for row in result])
        assert names == [
            "Bob",
            "Eve",
        ], f"After reopen, expected [Bob, Eve] for age=25, got {names}"

        result = conn2.execute("MATCH (p:Person) WHERE p.age = 40 RETURN p.name;")
        names = [row[0] for row in result]
        assert names == [
            "Diana"
        ], f"After reopen, expected [Diana] for age=40, got {names}"

        conn2.close()
        db2.close()
