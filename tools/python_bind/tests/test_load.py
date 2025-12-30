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

import os
import shutil
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug import Database


def get_tinysnb_dataset_path():
    """Get the path to tinysnb dataset CSV files."""
    # Try to get from environment variable first
    flex_data_dir = os.environ.get("FLEX_DATA_DIR")
    if flex_data_dir:
        # Check if it's already pointing to tinysnb
        if "tinysnb" in flex_data_dir:
            return flex_data_dir
        # Or check if tinysnb exists as subdirectory
        tinysnb_subdir = os.path.join(flex_data_dir, "tinysnb")
        if os.path.exists(tinysnb_subdir):
            return tinysnb_subdir

    # Try relative path from workspace root
    # Go up from tests/ -> python_bind/ -> tools/ -> workspace root
    current_file = os.path.abspath(__file__)
    tests_dir = os.path.dirname(current_file)
    python_bind_dir = os.path.dirname(tests_dir)
    tools_dir = os.path.dirname(python_bind_dir)
    workspace_root = os.path.dirname(tools_dir)

    tinysnb_path = os.path.join(workspace_root, "example_dataset", "tinysnb")
    if os.path.exists(tinysnb_path):
        return tinysnb_path

    # Default path (assumes dataset is loaded there)
    return "/tmp/tinysnb"


class TestLoadFrom:
    """Test cases for LOAD FROM functionality with tinysnb dataset."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Setup test database."""
        self.db_dir = str(tmp_path / "test_load_db")
        shutil.rmtree(self.db_dir, ignore_errors=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.tinysnb_path = get_tinysnb_dataset_path()
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_load_from_basic_return_all(self):
        """Test basic LOAD FROM with RETURN *."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN *
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one record"
        # vPerson.csv has header + 8 data rows
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        # Check first record structure (should have all columns)
        first_record = records[0]
        assert len(first_record) > 0, "Record should have columns"

    def test_load_from_return_specific_columns(self):
        """Test LOAD FROM with column projection."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN fName, age
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        # Check that only specified columns are returned
        first_record = records[0]
        assert len(first_record) == 2, "Should return only 2 columns"
        assert isinstance(first_record[0], str), "fName should be string"
        assert isinstance(first_record[1], int), "age should be integer"

    def test_load_from_with_where(self):
        """Test LOAD FROM with WHERE clause filtering."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        WHERE age > 30
        RETURN fName, age
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one record"

        # Verify all returned records satisfy the condition
        for record in records:
            age = record[1]
            assert age > 30, f"Age {age} should be greater than 30"

    def test_load_from_with_order_by(self):
        """Test LOAD FROM with ORDER BY clause."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN fName, age
        ORDER BY age
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        # Verify records are sorted by age
        ages = [record[1] for record in records]
        assert ages == sorted(ages), "Records should be sorted by age ascending"

    def test_load_from_with_order_by_desc(self):
        """Test LOAD FROM with ORDER BY DESC clause."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN fName, age
        ORDER BY age DESC
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        # Verify records are sorted by age descending
        ages = [record[1] for record in records]
        assert ages == sorted(
            ages, reverse=True
        ), "Records should be sorted by age descending"

    def test_load_from_with_limit(self):
        """Test LOAD FROM with LIMIT clause."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN fName, age
        LIMIT 3
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 3, f"Expected 3 records, got {len(records)}"

    def test_load_from_with_group_by(self):
        """Test LOAD FROM with grouping (RETURN with aggregate function)."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN gender, COUNT(*) as cnt
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one group"

        # Verify group by structure
        for record in records:
            assert len(record) == 2, "Should return gender and count"
            assert isinstance(record[1], int), "Count should be integer"
            assert record[1] > 0, "Count should be positive"

    def test_load_from_with_group_by_and_aggregate(self):
        """Test LOAD FROM with grouping and aggregate functions."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN gender, AVG(age) as avg_age, MAX(age) as max_age
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one group"

        # Verify aggregate results
        for record in records:
            assert len(record) == 3, "Should return gender, avg_age, and max_age"
            assert isinstance(record[1], (int, float)), "avg_age should be numeric"
            assert isinstance(record[2], int), "max_age should be integer"

    def test_load_from_where_and_order_by(self):
        """Test LOAD FROM with WHERE and ORDER BY combination."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        WHERE age > 25
        RETURN fName, age
        ORDER BY age DESC
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one record"

        # Verify filtering and sorting
        ages = [record[1] for record in records]
        assert all(age > 25 for age in ages), "All ages should be greater than 25"
        assert ages == sorted(
            ages, reverse=True
        ), "Records should be sorted by age descending"

    def test_load_from_where_and_limit(self):
        """Test LOAD FROM with WHERE and LIMIT combination."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        WHERE age > 30
        RETURN fName, age
        LIMIT 2
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) <= 2, f"Should return at most 2 records, got {len(records)}"

        # Verify filtering
        for record in records:
            assert record[1] > 30, f"Age {record[1]} should be greater than 30"

    def test_load_from_group_by_and_order_by(self):
        """Test LOAD FROM with grouping and ORDER BY combination."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN gender, COUNT(*) as cnt
        ORDER BY cnt DESC
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one group"

        # Verify sorting by count
        counts = [record[1] for record in records]
        assert counts == sorted(
            counts, reverse=True
        ), "Groups should be sorted by count descending"

    def test_load_from_complex_query(self):
        """Test LOAD FROM with multiple relational operators."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        WHERE age > 25
        RETURN gender, AVG(age) as avg_age
        ORDER BY avg_age DESC
        LIMIT 2
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) <= 2, f"Should return at most 2 records, got {len(records)}"

        # Verify structure
        for record in records:
            assert len(record) == 2, "Should return gender and avg_age"
            assert isinstance(record[1], (int, float)), "avg_age should be numeric"

    def test_load_from_edge_data(self):
        """Test LOAD FROM with edge CSV file."""
        csv_path = os.path.join(self.tinysnb_path, "eMeets.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN *
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one record"

        # Verify record structure
        first_record = records[0]
        assert len(first_record) > 0, "Record should have columns"

    def test_load_from_edge_with_where(self):
        """Test LOAD FROM edge data with WHERE clause."""
        csv_path = os.path.join(self.tinysnb_path, "eMeets.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        # Assuming eMeets.csv has location and times columns
        # This is a basic test - adjust based on actual CSV structure
        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN to, from, location
        ORDER BY to, from
        LIMIT 5
        """
        result = self.conn.execute(query)

        records = list(result)

        assert records == [
            [2, 0, "[7.82,3.54]"],
            [2, 10, "[3.5,1.1]"],
            [3, 7, "[2.11,3.1]"],
            [3, 8, "[2.2,9.0]"],
            [3, 9, "[3,5.2]"],
        ], "unexpected records"

    def test_load_from_with_column_alias(self):
        """Test LOAD FROM with column aliases in RETURN."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN fName AS name, age AS years
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        # Verify aliased columns
        first_record = records[0]
        assert len(first_record) == 2, "Should return 2 columns"
        assert isinstance(first_record[0], str), "name should be string"
        assert isinstance(first_record[1], int), "years should be integer"

    def test_load_from_with_multiple_conditions(self):
        """Test LOAD FROM with multiple WHERE conditions."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        WHERE age > 25 AND age < 40
        RETURN fName, age
        """
        result = self.conn.execute(query)

        records = list(result)

        # Verify all records satisfy both conditions
        for record in records:
            age = record[1]
            assert 25 < age < 40, f"Age {age} should be between 25 and 40"

    def test_load_from_with_sum_aggregate(self):
        """Test LOAD FROM with SUM aggregate function."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN SUM(age) as total_age
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 1, "Should return one record with sum"

        total_age = records[0][0]
        assert isinstance(total_age, (int, float)), "total_age should be numeric"
        assert total_age > 0, "total_age should be positive"

    def test_load_from_with_count_aggregate(self):
        """Test LOAD FROM with COUNT aggregate function."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN COUNT(*) as total_count
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 1, "Should return one record with count"

        total_count = records[0][0]
        assert isinstance(total_count, int), "total_count should be integer"
        assert total_count == 8, f"Expected 8 records, got {total_count}"

    def test_load_from_with_min_max_aggregate(self):
        """Test LOAD FROM with MIN and MAX aggregate functions."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN MIN(age) as min_age, MAX(age) as max_age
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 1, "Should return one record"

        min_age, max_age = records[0]
        assert isinstance(min_age, int), "min_age should be integer"
        assert isinstance(max_age, int), "max_age should be integer"
        assert min_age <= max_age, "min_age should be less than or equal to max_age"
