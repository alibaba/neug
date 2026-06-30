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
Tests for documentation examples in doc/source/data_io/.

This module validates the Cypher code examples from the Data I/O
documentation pages:
  - load_data.md  (LOAD FROM)
  - import_data.md (COPY FROM)
  - export_data.md (COPY TO)

The tests use the modern_graph dataset (Person, KNOWS, Software)
which matches the examples in the documentation.
"""

import os
import shutil
import sys

import pytest

from neug.database import Database


def get_modern_graph_path():
    """Get the path to the modern_graph dataset."""
    flex_data_dir = os.environ.get("FLEX_DATA_DIR")
    if flex_data_dir and "modern_graph" in flex_data_dir:
        return flex_data_dir

    current_file = os.path.abspath(__file__)
    tests_dir = os.path.dirname(current_file)
    python_bind_dir = os.path.dirname(tests_dir)
    tools_dir = os.path.dirname(python_bind_dir)
    workspace_root = os.path.dirname(tools_dir)

    path = os.path.join(workspace_root, "example_dataset", "modern_graph")
    if os.path.exists(path):
        return path
    return None


# ============================================================
# LOAD FROM tests (load_data.md)
# ============================================================


class TestLoadFromDocs:
    """Tests for LOAD FROM examples in load_data.md."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Setup test database."""
        self.db_dir = str(tmp_path / "test_load_docs")
        self.tmp_path = tmp_path
        shutil.rmtree(self.db_dir, ignore_errors=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.data_path = get_modern_graph_path()
        if self.data_path is None:
            pytest.skip("modern_graph dataset not found")
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_load_from_csv_basic(self):
        """load_data.md: LOAD FROM 'person.csv' (delim=',', header=true)
        RETURN name, age;"""
        csv_path = os.path.join(self.data_path, "person.csv")
        result = self.conn.execute(
            f'LOAD FROM "{csv_path}" (header=true) RETURN name, age;'
        )
        records = list(result)
        assert len(records) == 4
        # Verify column types
        for r in records:
            assert isinstance(r[0], str)
            assert isinstance(r[1], int)

    def test_load_from_column_reordering(self):
        """load_data.md: columns can be returned in any order."""
        csv_path = os.path.join(self.data_path, "person.csv")
        # Return columns in reverse order (age, name, id) vs CSV order (id, name, age)
        result = self.conn.execute(
            f'LOAD FROM "{csv_path}" (header=true) RETURN age, name, id;'
        )
        records = list(result)
        assert len(records) == 4
        for r in records:
            assert isinstance(r[0], int)  # age
            assert isinstance(r[1], str)  # name
            assert isinstance(r[2], int)  # id

    def test_load_from_column_aliases(self):
        """load_data.md: RETURN src_name AS src, dst_name AS dst,
        weight AS score;"""
        csv_path = os.path.join(self.data_path, "person.csv")
        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true)
            RETURN name AS person_name, age AS years
            """
        )
        records = list(result)
        assert len(records) == 4
        assert isinstance(records[0][0], str)
        assert isinstance(records[0][1], int)

    def test_load_from_type_casting(self):
        """load_data.md: RETURN name, CAST(age, 'DOUBLE') AS double_age;"""
        csv_path = os.path.join(self.data_path, "person.csv")
        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true)
            RETURN name, CAST(age, 'DOUBLE') AS double_age
            """
        )
        records = list(result)
        assert len(records) == 4
        for r in records:
            assert isinstance(r[1], float), f"Expected float, got {type(r[1])}"

    def test_load_from_where_filtering(self):
        """load_data.md: WHERE age > 25 AND age < 40."""
        csv_path = os.path.join(self.data_path, "person.csv")
        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true)
            WHERE age > 25 AND age < 40
            RETURN name, age
            """
        )
        records = list(result)
        assert len(records) > 0
        for r in records:
            assert 25 < r[1] < 40

    def test_load_from_aggregation(self):
        """load_data.md: COUNT, AVG, MIN, MAX aggregate functions."""
        csv_path = os.path.join(self.data_path, "person.csv")
        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true)
            RETURN
                COUNT(*) AS total,
                AVG(age) AS avg_age,
                MIN(age) AS min_age,
                MAX(age) AS max_age
            """
        )
        records = list(result)
        assert len(records) == 1
        total, avg_age, min_age, max_age = records[0]
        assert total == 4
        assert min_age == 27  # vadas
        assert max_age == 35  # peter
        assert isinstance(avg_age, (int, float))

    def test_load_from_sorting_and_limiting(self):
        """load_data.md: ORDER BY age DESC, name ASC LIMIT 10."""
        csv_path = os.path.join(self.data_path, "person.csv")
        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true)
            RETURN name, age
            ORDER BY age DESC
            LIMIT 2
            """
        )
        records = list(result)
        assert len(records) == 2
        # First record should have the highest age
        assert records[0][1] >= records[1][1]

    def test_load_from_performance_options(self):
        """load_data.md: parallel option for LOAD FROM.

        Note: batch_read and batch_size are only supported in COPY FROM,
        not in LOAD FROM, because LOAD FROM may require downstream
        computation (filtering, projection, etc.) that the engine does
        not yet support in batch mode.
        """
        csv_path = os.path.join(self.data_path, "person.csv")
        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (
                header = true,
                parallel = true
            )
            RETURN name, age
            """
        )
        records = list(result)
        assert len(records) == 4

    def test_load_from_csv_array_limitation(self):
        """load_data.md: CSV fields are not parsed as fixed-size ARRAY values."""
        csv_path = self.tmp_path / "sensor_arrays.csv"
        csv_path.write_text('id,readings\n1,"[1,2,3]"\n')

        rows = list(
            self.conn.execute(
                f'LOAD FROM "{csv_path}" (header=true, delimiter=",") '
                "RETURN id, readings;"
            )
        )
        assert rows == [[1, "[1,2,3]"]]

        with pytest.raises(RuntimeError) as exc_info:
            self.conn.execute(
                f'LOAD FROM "{csv_path}" (header=true, delimiter=",") '
                "RETURN id, CAST(readings, 'INT64[3]');"
            )
        assert "not supported" in str(exc_info.value).lower()


# ============================================================
# COPY FROM tests (import_data.md)
# ============================================================


class TestCopyFromDocs:
    """Tests for COPY FROM examples in import_data.md."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Setup test database."""
        self.db_dir = str(tmp_path / "test_copy_from_docs")
        self.tmp_path = tmp_path
        shutil.rmtree(self.db_dir, ignore_errors=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.data_path = get_modern_graph_path()
        if self.data_path is None:
            pytest.skip("modern_graph dataset not found")
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def _create_modern_graph_schema(self):
        """Create the modern_graph schema (Person + KNOWS)."""
        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        self.conn.execute(
            "CREATE REL TABLE KNOWS(" "FROM Person TO Person, weight DOUBLE);"
        )

    def _load_modern_graph_data(self):
        """Load Person and KNOWS data from modern_graph CSVs."""
        person_csv = os.path.join(self.data_path, "person.csv")
        knows_csv = os.path.join(self.data_path, "person_knows_person.csv")
        self.conn.execute(f'COPY Person FROM "{person_csv}" (header=true);')
        self.conn.execute(
            f'COPY KNOWS FROM "{knows_csv}" '
            f'(from="Person", to="Person", header=true);'
        )

    def test_quick_start_complete_workflow(self):
        """import_data.md: Quick Start — create schema, import, verify."""
        # Step 1: Prepare data files
        users_csv = self.tmp_path / "users.csv"
        friendships_csv = self.tmp_path / "friendships.csv"
        users_csv.write_text(
            "id,name,age,email\n"
            "1,Alice Johnson,30,alice@example.com\n"
            "2,Bob Smith,25,bob@example.com\n"
            "3,Carol Davis,28,carol@example.com\n"
        )
        friendships_csv.write_text(
            "from_user_id,to_user_id,since_year\n"
            "1,2,2020\n"
            "2,3,2019\n"
            "1,3,2021\n"
        )

        # Step 2: Create schema
        self.conn.execute(
            "CREATE NODE TABLE User("
            "id INT64 PRIMARY KEY, name STRING, age INT64, email STRING);"
        )
        self.conn.execute(
            "CREATE REL TABLE FRIENDS(" "FROM User TO User, since_year INT64);"
        )

        # Step 3: Import data
        self.conn.execute(f'COPY User FROM "{users_csv}" (header=true, delimiter=",");')
        self.conn.execute(
            f'COPY FRIENDS FROM "{friendships_csv}" '
            f'(from="User", to="User", header=true, delimiter=",");'
        )

        # Step 4: Verify
        res = self.conn.execute("MATCH (u:User) RETURN count(u) AS user_count;")
        records = list(res)
        assert records[0][0] == 3

        res = self.conn.execute(
            "MATCH (u1:User)-[f:FRIENDS]->(u2:User) "
            "RETURN u1.name, u2.name, f.since_year LIMIT 5;"
        )
        records = list(res)
        assert len(records) == 3

    def test_copy_from_node_table(self):
        """import_data.md: COPY Person FROM 'person.csv' (header=true)."""
        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        csv_path = os.path.join(self.data_path, "person.csv")
        self.conn.execute(f'COPY Person FROM "{csv_path}" (header=true);')
        res = self.conn.execute("MATCH (p:Person) RETURN count(p);")
        assert list(res)[0][0] == 4

    def test_copy_from_wildcard(self):
        """import_data.md: COPY Person FROM 'person*.csv' (header=true)."""
        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        # modern_graph has person.part1.csv and person.part2.csv
        part_pattern = os.path.join(self.data_path, "test_data/person.part*.csv")
        self.conn.execute(f'COPY Person FROM "{part_pattern}" (header=true);')
        res = self.conn.execute("MATCH (p:Person) RETURN count(p);")
        assert list(res)[0][0] == 4

    def test_copy_from_relationship_table(self):
        """import_data.md: COPY KNOWS FROM ... (from='Person', to='Person',
        header=true)."""
        self._create_modern_graph_schema()
        person_csv = os.path.join(self.data_path, "person.csv")
        knows_csv = os.path.join(self.data_path, "person_knows_person.csv")
        self.conn.execute(f'COPY Person FROM "{person_csv}" (header=true);')
        self.conn.execute(
            f'COPY KNOWS FROM "{knows_csv}" '
            f'(from="Person", to="Person", header=true);'
        )
        res = self.conn.execute("MATCH ()-[k:KNOWS]->() RETURN count(k);")
        assert list(res)[0][0] == 2

    def test_copy_from_with_column_remapping(self):
        """import_data.md: COPY Person FROM (LOAD FROM ... RETURN id, name, age).

        Demonstrates column reordering via LOAD FROM subquery.
        The CSV has columns in a different order (age, name, id) from the
        table schema (id, name, age), and LOAD FROM reorders them.
        """
        # Create a CSV with columns in different order than schema
        remap_csv = self.tmp_path / "person_remap.csv"
        remap_csv.write_text(
            "age,name,id\n39,marko,1\n27,vadas,2\n32,josh,3\n35,peter,4\n"
        )

        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )

        # Use LOAD FROM subquery to reorder columns to match table schema
        self.conn.execute(
            f"""
            COPY Person FROM (
                LOAD FROM "{remap_csv}" (header=true, delimiter=",")
                RETURN id, name, age
            )
            """
        )
        res = self.conn.execute("MATCH (p:Person) RETURN count(p);")
        assert list(res)[0][0] == 4

    def test_copy_from_with_filtering(self):
        """import_data.md: COPY Person FROM (LOAD FROM ... WHERE age >= 18
        RETURN *)."""
        csv_path = self.tmp_path / "person_filter.csv"
        csv_path.write_text(
            "id|name|age\n" "1|Alice|30\n" "2|Bob|15\n" "3|Carol|28\n" "4|Dave|12\n"
        )

        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        self.conn.execute(
            f"""
            COPY Person FROM (
                LOAD FROM "{csv_path}" (header=true)
                WHERE age >= 18
                RETURN *
            )
            """
        )
        res = self.conn.execute("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age;")
        records = list(res)
        # Only Alice (30) and Carol (28) should be imported
        assert len(records) == 2
        for r in records:
            assert r[1] >= 18

    def test_copy_from_parallel(self):
        """import_data.md: COPY Person FROM ... (header=true, parallel=true)."""
        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        csv_path = os.path.join(self.data_path, "person.csv")
        self.conn.execute(
            f'COPY Person FROM "{csv_path}" (header=true, parallel=true);'
        )
        res = self.conn.execute("MATCH (p:Person) RETURN count(p);")
        assert list(res)[0][0] == 4

    def test_copy_from_csv_array_limitation(self):
        """import_data.md: COPY FROM CSV cannot materialize fixed-size ARRAY columns."""
        csv_path = self.tmp_path / "sensor_arrays.csv"
        csv_path.write_text('id,readings\n1,"[1,2,3]"\n')

        self.conn.execute(
            "CREATE NODE TABLE Sensor(" "id INT64, readings INT64[3], PRIMARY KEY(id));"
        )
        with pytest.raises(RuntimeError) as exc_info:
            self.conn.execute(
                f'COPY Sensor FROM "{csv_path}" (header=true, delimiter=",");'
            )
        assert "Unsupported data type in CSV parser" in str(exc_info.value)

    def test_copy_from_batch_read(self):
        """import_data.md: COPY FROM with batch_read and batch_size options.

        batch_read and batch_size are only supported in COPY FROM (not LOAD
        FROM), because COPY FROM feeds data directly into storage without
        downstream computation.
        """
        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        csv_path = os.path.join(self.data_path, "person.csv")
        self.conn.execute(
            f"""
            COPY Person FROM "{csv_path}" (
                header = true,
                batch_read = true,
                batch_size = 2097152
            );
            """
        )
        res = self.conn.execute("MATCH (p:Person) RETURN count(p);")
        assert list(res)[0][0] == 4

    def test_import_order_nodes_before_edges(self):
        """import_data.md: always import nodes before relationships."""
        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        self.conn.execute(
            "CREATE NODE TABLE Software("
            "id INT64, name STRING, lang STRING, PRIMARY KEY(id));"
        )
        self.conn.execute(
            "CREATE REL TABLE KNOWS(" "FROM Person TO Person, weight DOUBLE);"
        )
        self.conn.execute(
            "CREATE REL TABLE CREATED("
            "FROM Person TO Software, weight DOUBLE, since INT64);"
        )

        # Import all nodes first
        person_csv = os.path.join(self.data_path, "person.csv")
        software_csv = os.path.join(self.data_path, "software.csv")
        self.conn.execute(f'COPY Person FROM "{person_csv}" (header=true);')
        self.conn.execute(f'COPY Software FROM "{software_csv}" (header=true);')

        # Then import edges
        knows_csv = os.path.join(self.data_path, "person_knows_person.csv")
        created_csv = os.path.join(self.data_path, "person_created_software.csv")
        self.conn.execute(
            f'COPY KNOWS FROM "{knows_csv}" '
            f'(from="Person", to="Person", header=true);'
        )
        self.conn.execute(
            f'COPY CREATED FROM "{created_csv}" '
            f'(from="Person", to="Software", header=true);'
        )

        # Verify
        res = self.conn.execute("MATCH (p:Person) RETURN count(p);")
        assert list(res)[0][0] == 4
        res = self.conn.execute("MATCH (s:Software) RETURN count(s);")
        assert list(res)[0][0] == 2
        res = self.conn.execute("MATCH ()-[k:KNOWS]->() RETURN count(k);")
        assert list(res)[0][0] == 2
        res = self.conn.execute("MATCH ()-[c:CREATED]->() RETURN count(c);")
        assert list(res)[0][0] == 4


# ============================================================
# COPY TO tests (export_data.md)
# ============================================================


class TestCopyToDocs:
    """Tests for COPY TO examples in export_data.md."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Setup test database with modern_graph data."""
        self.db_dir = str(tmp_path / "test_copy_to_docs")
        self.tmp_path = tmp_path
        shutil.rmtree(self.db_dir, ignore_errors=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.data_path = get_modern_graph_path()
        if self.data_path is None:
            pytest.skip("modern_graph dataset not found")

        # Load modern_graph data
        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        self.conn.execute(
            "CREATE REL TABLE KNOWS(" "FROM Person TO Person, weight DOUBLE);"
        )
        person_csv = os.path.join(self.data_path, "person.csv")
        knows_csv = os.path.join(self.data_path, "person_knows_person.csv")
        self.conn.execute(f'COPY Person FROM "{person_csv}" (header=true);')
        self.conn.execute(
            f'COPY KNOWS FROM "{knows_csv}" '
            f'(from="Person", to="Person", header=true);'
        )
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_copy_to_csv_nodes(self):
        """export_data.md: COPY (MATCH (p:Person) RETURN p.*) TO
        'person.csv' (header=true)."""
        out_path = self.tmp_path / "person_export.csv"
        self.conn.execute(
            f"COPY (MATCH (p:Person) RETURN p.*) " f"TO '{out_path}' (header=true);"
        )
        assert out_path.exists()
        content = out_path.read_text()
        lines = content.strip().split("\n")
        # Header + 4 data rows
        assert len(lines) == 5, f"Expected 5 lines, got {len(lines)}"
        # Verify header contains expected columns
        header = lines[0]
        assert "id" in header.lower() or "p.id" in header.lower()

    def test_copy_to_csv_edges(self):
        """export_data.md: COPY (MATCH (:Person)-[e:KNOWS]->(:Person)
        RETURN e) TO 'knows.csv' (header=true)."""
        out_path = self.tmp_path / "knows_export.csv"
        self.conn.execute(
            f"COPY (MATCH (:Person)-[e:KNOWS]->(:Person) RETURN e) "
            f"TO '{out_path}' (header=true);"
        )
        assert out_path.exists()
        content = out_path.read_text()
        lines = content.strip().split("\n")
        # Header + 2 knows edges
        assert len(lines) == 3, f"Expected 3 lines, got {len(lines)}"

    def test_copy_to_csv_with_delimiter(self):
        """export_data.md: DELIMITER option."""
        out_path = self.tmp_path / "person_comma.csv"
        self.conn.execute(
            f"COPY (MATCH (p:Person) RETURN p.id, p.name, p.age) "
            f"TO '{out_path}' (header=true, delimiter=',');"
        )
        assert out_path.exists()
        content = out_path.read_text()
        lines = content.strip().split("\n")
        assert len(lines) == 5
        # Verify comma delimiter is used (not pipe)
        assert "," in lines[0]

    def test_copy_to_csv_no_header(self):
        """export_data.md: HEADER=false (default)."""
        out_path = self.tmp_path / "person_no_header.csv"
        self.conn.execute(
            f"COPY (MATCH (p:Person) RETURN p.id, p.name) "
            f"TO '{out_path}' (HEADER = false);"
        )
        assert out_path.exists()
        content = out_path.read_text()
        lines = content.strip().split("\n")
        # No header, just 4 data rows
        assert len(lines) == 4
