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

JSON_TESTS_ENABLED = os.environ.get("NEUG_RUN_JSON_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
json_test = pytest.mark.skipif(
    not JSON_TESTS_ENABLED,
    reason="JSON tests disabled by default; set NEUG_RUN_JSON_TESTS=1 to enable.",
)


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

    def test_load_from_return_distinct_single_column_bool(self):
        """Test DISTINCT with single boolean column."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN DISTINCT CAST(isStudent, 'BOOL')
        """
        result = self.conn.execute(query)
        records = list(result)

        # Should have distinct boolean values (True, False)
        assert len(records) > 0, "Should return at least one distinct value"
        assert len(records) <= 2, "Boolean should have at most 2 distinct values"

        # Verify all values are boolean
        for record in records:
            assert len(record) == 1, "Should return only 1 column"
            assert isinstance(
                record[0], bool
            ), f"isStudent should be boolean, got {type(record[0])}"

    def test_load_from_return_distinct_single_column_numeric(self):
        """Test DISTINCT with single numeric column."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN DISTINCT age
        """
        result = self.conn.execute(query)
        records = list(result)

        # Should have distinct age values
        assert len(records) > 0, "Should return at least one distinct value"

        # Verify all values are numeric and distinct
        ages = []
        for record in records:
            assert len(record) == 1, "Should return only 1 column"
            assert isinstance(
                record[0], int
            ), f"age should be integer, got {type(record[0])}"
            ages.append(record[0])

        # Verify distinctness
        assert len(ages) == len(set(ages)), "All returned ages should be distinct"

    def test_load_from_return_distinct_single_column_string(self):
        """Test DISTINCT with single string column."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN DISTINCT fName
        """
        result = self.conn.execute(query)
        records = list(result)

        # Should have distinct names
        assert len(records) > 0, "Should return at least one distinct value"

        # Verify all values are strings and distinct
        names = []
        for record in records:
            assert len(record) == 1, "Should return only 1 column"
            assert isinstance(
                record[0], str
            ), f"fName should be string, got {type(record[0])}"
            names.append(record[0])

        # Verify distinctness
        assert len(names) == len(set(names)), "All returned names should be distinct"

    def test_load_from_return_distinct_single_column_date(self):
        """Test DISTINCT with single date column."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN DISTINCT CAST(birthdate, 'DATE')
        """
        result = self.conn.execute(query)
        records = list(result)

        # Should have distinct birthdates
        assert len(records) > 0, "Should return at least one distinct value"

        # Verify all values are dates and distinct
        dates = []
        for record in records:
            assert len(record) == 1, "Should return only 1 column"
            # Date can be returned as date object or string depending on implementation
            dates.append(record[0])

        # Verify distinctness
        assert len(dates) == len(set(dates)), "All returned dates should be distinct"

    def test_load_from_return_distinct_single_column_datetime(self):
        """Test DISTINCT with single datetime/timestamp column."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN DISTINCT CAST(registerTime, 'TIMESTAMP')
        """
        result = self.conn.execute(query)
        records = list(result)

        # Should have distinct timestamps
        assert len(records) > 0, "Should return at least one distinct value"

        # Verify all values are distinct
        timestamps = []
        for record in records:
            assert len(record) == 1, "Should return only 1 column"
            timestamps.append(record[0])

        # Verify distinctness
        assert len(timestamps) == len(
            set(timestamps)
        ), "All returned timestamps should be distinct"

    def test_load_from_return_distinct_two_columns(self):
        """Test DISTINCT with two columns: boolean and numeric."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN DISTINCT isStudent, age
        """
        result = self.conn.execute(query)
        records = list(result)

        # Should have distinct combinations
        assert len(records) > 0, "Should return at least one distinct combination"

        # Verify all combinations are distinct
        combinations = []
        for record in records:
            assert len(record) == 2, "Should return 2 columns"
            assert isinstance(
                record[0], bool
            ), f"isStudent should be boolean, got {type(record[0])}"
            assert isinstance(
                record[1], int
            ), f"age should be integer, got {type(record[1])}"
            combinations.append((record[0], record[1]))

        # Verify distinctness
        assert len(combinations) == len(
            set(combinations)
        ), "All returned combinations should be distinct"

    def test_load_from_return_distinct_multiple_columns(self):
        """Test DISTINCT with multiple columns: string, date, datetime."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN DISTINCT fName, CAST(birthdate, 'DATE'), CAST(registerTime, 'TIMESTAMP')
        """
        result = self.conn.execute(query)
        records = list(result)

        # Should have distinct combinations
        assert len(records) > 0, "Should return at least one distinct combination"

        # Verify all combinations are distinct
        combinations = []
        for record in records:
            assert len(record) == 3, "Should return 3 columns"
            assert isinstance(
                record[0], str
            ), f"fName should be string, got {type(record[0])}"
            combinations.append((record[0], record[1], record[2]))

        # Verify distinctness
        assert len(combinations) == len(
            set(combinations)
        ), "All returned combinations should be distinct"

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

    def test_load_from_with_cast_int_to_double(self):
        """Test LOAD FROM with CAST to convert integer to double."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN fName, CAST(age, 'DOUBLE') as age_double
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        # Verify type conversion
        first_record = records[0]
        assert len(first_record) == 2, "Should return 2 columns"
        assert isinstance(first_record[0], str), "fName should be string"
        assert isinstance(first_record[1], float), "age_double should be float"
        assert first_record[1] == 35.0, "Age should be converted to 35.0"

    def test_load_from_with_cast_double_to_string(self):
        """Test LOAD FROM with CAST to convert double to string."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN fName, CAST(eyeSight, 'STRING') as eyeSight_str
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        # Verify type conversion
        first_record = records[0]
        assert len(first_record) == 2, "Should return 2 columns"
        assert isinstance(first_record[0], str), "fName should be string"
        assert isinstance(first_record[1], str), "eyeSight_str should be string"
        assert first_record[1] == "5.0", "eyeSight should be converted to '5.0'"

    def test_load_from_with_cast_multiple_columns(self):
        """Test LOAD FROM with CAST on multiple columns."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        RETURN
            CAST(ID, 'INT64') as id_int,
            fName,
            CAST(age, 'DOUBLE') as age_double,
            CAST(eyeSight, 'STRING') as eyeSight_str
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        # Verify type conversions
        first_record = records[0]
        assert len(first_record) == 4, "Should return 4 columns"
        assert isinstance(first_record[0], int), "id_int should be integer"
        assert isinstance(first_record[1], str), "fName should be string"
        assert isinstance(first_record[2], float), "age_double should be float"
        assert isinstance(first_record[3], str), "eyeSight_str should be string"
        assert first_record[0] == 0, "ID should be 0"
        assert first_record[2] == 35.0, "Age should be 35.0"
        assert first_record[3] == "5.0", "eyeSight should be converted to '5.0'"

    def test_load_from_with_cast_and_where(self):
        """Test LOAD FROM with CAST and WHERE clause."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        query = f"""
        LOAD FROM "{csv_path}" (delim=',')
        WHERE age > 30.0
        RETURN fName, CAST(age, 'DOUBLE') as age_double
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one record"

        # Verify filtering and type conversion
        for record in records:
            assert isinstance(record[1], float), "age_double should be float"
            assert record[1] > 30.0, f"Age {record[1]} should be greater than 30.0"

    @json_test
    def test_load_from_json_basic_return_all(self):
        """Test basic LOAD FROM JSON with RETURN *."""
        json_path = os.path.join(self.tinysnb_path, "json", "vPerson.json")
        if not os.path.exists(json_path):
            pytest.skip(f"JSON file not found: {json_path}")

        # json should be loaded as extension first
        self.conn.execute("LOAD JSON")

        query = f"""
        LOAD FROM "{json_path}"
        RETURN *
        """
        result = self.conn.execute(query)

        records = list(result)

        # vPerson.json has 8 data rows
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        # Check first record structure (should have all columns)
        first_record = records[0]
        assert len(first_record) == 16, f"Expected 16 columns, got {len(first_record)}"

    @json_test
    def test_load_from_json_return_specific_columns(self):
        """Test LOAD FROM JSON Array with column projection."""
        json_path = os.path.join(self.tinysnb_path, "json", "vPerson.json")
        if not os.path.exists(json_path):
            pytest.skip(f"JSON file not found: {json_path}")

        self.conn.execute("LOAD JSON")

        query = f"""
        LOAD FROM "{json_path}"
        RETURN fName, age
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        first_record = records[0]
        assert len(first_record) == 2, "Should return only 2 columns"
        assert isinstance(first_record[0], str), "fName should be string"
        assert isinstance(first_record[1], int), "age should be integer"

    @json_test
    def test_load_from_json_with_column_alias(self):
        """Test LOAD FROM JSON Array with column aliases in RETURN.

        Regression test: column aliases (e.g., fName AS name) must not be used
        as the physical column name when reading the JSON file. The entry schema
        sent to the Arrow JSON reader must use the original field names.
        """
        json_path = os.path.join(self.tinysnb_path, "json", "vPerson.json")
        if not os.path.exists(json_path):
            pytest.skip(f"JSON file not found: {json_path}")

        self.conn.execute("LOAD JSON")

        query = f"""
        LOAD FROM "{json_path}"
        RETURN fName AS name, age AS years
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        first_record = records[0]
        assert len(first_record) == 2, "Should return 2 columns"
        assert isinstance(first_record[0], str), "name (aliased fName) should be string"
        assert isinstance(first_record[1], int), "years (aliased age) should be integer"
        # Verify actual data: first person is Alice, age 35
        assert first_record[0] == "Alice", f"Expected 'Alice', got '{first_record[0]}'"
        assert first_record[1] == 35, f"Expected 35, got {first_record[1]}"

    @json_test
    def test_load_from_jsonl_with_column_alias(self):
        """Test LOAD FROM JSONL with column aliases in RETURN."""
        jsonl_path = os.path.join(self.tinysnb_path, "json", "vPerson.jsonl")
        if not os.path.exists(jsonl_path):
            pytest.skip(f"JSONL file not found: {jsonl_path}")

        self.conn.execute("LOAD JSON")

        query = f"""
        LOAD FROM "{jsonl_path}" (newline_delimited=true)
        RETURN fName AS name, age AS years
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) == 8, f"Expected 8 records, got {len(records)}"

        first_record = records[0]
        assert len(first_record) == 2, "Should return 2 columns"
        assert isinstance(first_record[0], str), "name (aliased fName) should be string"
        assert isinstance(first_record[1], int), "years (aliased age) should be integer"
        assert first_record[0] == "Alice", f"Expected 'Alice', got '{first_record[0]}'"
        assert first_record[1] == 35, f"Expected 35, got {first_record[1]}"

    @json_test
    def test_load_from_jsonl_return_specific_columns(self):
        """Test LOAD FROM JSONL with column projection."""
        jsonl_path = os.path.join(self.tinysnb_path, "json", "vPerson.jsonl")
        if not os.path.exists(jsonl_path):
            pytest.skip(f"JSONL file not found: {jsonl_path}")

        self.conn.execute("LOAD JSON")

        query = f"""
        LOAD FROM "{jsonl_path}" (newline_delimited=true)
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
        print(first_record)

    @json_test
    def test_load_from_jsonl_with_multiple_where_conditions(self):
        """Test LOAD FROM JSONL with multiple WHERE conditions."""
        jsonl_path = os.path.join(self.tinysnb_path, "json", "vPerson.jsonl")
        if not os.path.exists(jsonl_path):
            pytest.skip(f"JSONL file not found: {jsonl_path}")

        self.conn.execute("LOAD JSON")

        # Test with multiple conditions: age > 25 AND age < 40 AND gender == 1
        query = f"""
        LOAD FROM "{jsonl_path}" (newline_delimited=true)
        WHERE age > 25 AND age < 40 AND gender = 1
        RETURN fName, age, gender, eyeSight
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one record"

        # Verify all returned records satisfy all conditions
        for record in records:
            fname, age, gender, eye_sight = record
            assert 25 < age < 40, f"Age {age} should be between 25 and 40"
            assert gender == 1, f"Gender {gender} should be 1"
            assert isinstance(fname, str), "fName should be string"
            assert isinstance(eye_sight, (int, float)), "eyeSight should be numeric"

    @json_test
    def test_load_from_jsonl_with_complex_where_conditions(self):
        """Test LOAD FROM JSONL with complex WHERE conditions (age, eyeSight, height)."""
        jsonl_path = os.path.join(self.tinysnb_path, "json", "vPerson.jsonl")
        if not os.path.exists(jsonl_path):
            pytest.skip(f"JSONL file not found: {jsonl_path}")

        self.conn.execute("LOAD JSON")

        # Test with multiple conditions: age >= 30 AND eyeSight >= 5.0 AND height > 1.0
        query = f"""
        LOAD FROM "{jsonl_path}" (newline_delimited=true)
        WHERE age >= 30 AND eyeSight >= 5.0 AND height > 1.0
        RETURN fName, age, eyeSight, height
        """
        result = self.conn.execute(query)

        records = list(result)
        assert len(records) > 0, "Should return at least one record"
        # May return 0 or more records depending on data

        # Verify all returned records satisfy all conditions
        for record in records:
            fname, age, eye_sight, height = record
            assert age >= 30, f"Age {age} should be >= 30"
            assert eye_sight >= 5.0, f"eyeSight {eye_sight} should be >= 5.0"
            assert height > 1.0, f"height {height} should be > 1.0"
            assert isinstance(fname, str), "fName should be string"


class TestCopyFrom:
    """Test cases for COPY FROM functionality with schema creation and data verification."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Setup test database."""
        self.db_dir = str(tmp_path / "test_copy_from_db")
        shutil.rmtree(self.db_dir, ignore_errors=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        self.tinysnb_path = get_tinysnb_dataset_path()
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_copy_from_node_basic(self):
        """Test basic COPY FROM for node table."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        # Create schema
        create_schema = """
        CREATE NODE TABLE person (
            ID INT64,
            fName STRING,
            gender INT64,
            isStudent BOOLEAN,
            isWorker BOOLEAN,
            age INT64,
            eyeSight DOUBLE,
            birthdate DATE,
            registerTime TIMESTAMP,
            lastJobDuration INTERVAL,
            workedHours STRING,
            usedNames STRING,
            courseScoresPerTerm STRING,
            grades STRING,
            height FLOAT,
            u STRING,
            PRIMARY KEY (ID)
        )
        """
        self.conn.execute(create_schema)

        # Copy data from CSV
        copy_query = f'COPY person FROM "{csv_path}" (header=true, delimiter=",")'
        self.conn.execute(copy_query)

        # Verify data with MATCH query
        query = "MATCH (p:person) RETURN p.ID, p.fName, p.age ORDER BY p.ID LIMIT 5"
        result = self.conn.execute(query)
        records = list(result)

        assert len(records) > 0, "Should have loaded at least one person"
        # Verify first record (ID=0, Alice)
        assert records[0][0] == 0, "First person ID should be 0"
        assert records[0][1] == "Alice", "First person name should be Alice"
        assert records[0][2] == 35, "Alice's age should be 35"

    def test_copy_from_node_with_column_remapping(self):
        """Test COPY FROM for node table with column remapping when CSV order differs from schema."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        # Create schema with different property order than CSV
        # CSV order: ID, fName, gender, isStudent, isWorker, age, ...
        # Schema order: ID, age, fName, gender, ...
        create_schema = """
        CREATE NODE TABLE person_remap (
            ID INT64,
            age INT32,
            fName STRING,
            gender INT64,
            isStudent BOOLEAN,
            PRIMARY KEY (ID)
        )
        """
        self.conn.execute(create_schema)

        # Copy data with column remapping using LOAD FROM subquery
        # CSV has: ID, fName, gender, isStudent, isWorker, age
        # We want: ID, age, fName, gender, isStudent
        copy_query = f"""
        COPY person_remap FROM (
            LOAD FROM "{csv_path}" (header=true, delimiter=",")
            RETURN ID, CAST(age, 'INT32') as age, fName, gender, isStudent
        )
        """
        self.conn.execute(copy_query)

        # Verify data with MATCH query
        query = "MATCH (p:person_remap) RETURN p.ID, p.age, p.fName, p.gender ORDER BY p.ID LIMIT 3"
        result = self.conn.execute(query)
        records = list(result)

        assert len(records) >= 3, "Should have loaded at least 3 persons"
        # Verify first record (ID=0, Alice, age=35)
        assert records[0][0] == 0, "First person ID should be 0"
        assert records[0][1] == 35, "Alice's age should be 35"
        assert records[0][2] == "Alice", "First person name should be Alice"
        assert records[0][3] == 1, "Alice's gender should be 1"

    @json_test
    def test_copy_from_node_jsonl_with_column_remapping(self):
        """Test COPY FROM for node table with column remapping using JSONL file."""
        jsonl_path = os.path.join(self.tinysnb_path, "json", "vPerson.jsonl")
        if not os.path.exists(jsonl_path):
            pytest.skip(f"JSONL file not found: {jsonl_path}")

        # Create schema with different property order than JSONL
        # JSONL has: ID, fName, gender, isStudent, isWorker, age, eyeSight, ...
        # Schema order: ID, age, fName, gender, eyeSight, isStudent
        create_schema = """
        CREATE NODE TABLE person_jsonl_remap (
            ID INT64,
            age INT64,
            fName STRING,
            gender INT64,
            eyeSight DOUBLE,
            isStudent BOOLEAN,
            PRIMARY KEY (ID)
        )
        """
        self.conn.execute(create_schema)

        self.conn.execute("LOAD JSON")

        # Copy data with column remapping using LOAD FROM subquery
        # JSONL has: ID, fName, gender, isStudent, isWorker, age, eyeSight, ...
        # We want: ID, age, fName, gender, eyeSight, isStudent
        copy_query = f"""
        COPY person_jsonl_remap FROM (
            LOAD FROM "{jsonl_path}" (newline_delimited=true)
            RETURN ID, age, fName, gender, eyeSight, isStudent
        )
        """
        self.conn.execute(copy_query)

        # Verify data with MATCH query
        query = "MATCH (p:person_jsonl_remap) RETURN p.ID, p.age, p.fName, p.gender, p.eyeSight ORDER BY p.ID LIMIT 3"
        result = self.conn.execute(query)
        records = list(result)

        assert len(records) >= 3, "Should have loaded at least 3 persons"
        # Verify first record (ID=0, Alice, age=35, eyeSight=5.0)
        assert records[0][0] == 0, "First person ID should be 0"
        assert records[0][1] == 35, "Alice's age should be 35"
        assert records[0][2] == "Alice", "First person name should be Alice"
        assert records[0][3] == 1, "Alice's gender should be 1"
        assert records[0][4] == 5.0, "Alice's eyeSight should be 5.0"

    def test_copy_from_edge_basic(self):
        """Test basic COPY FROM for edge/relationship table."""
        person_csv = os.path.join(self.tinysnb_path, "vPerson.csv")
        meets_csv = os.path.join(self.tinysnb_path, "eMeets.csv")
        if not os.path.exists(person_csv) or not os.path.exists(meets_csv):
            pytest.skip("CSV files not found")

        # Create node table schema
        create_person_schema = """
        CREATE NODE TABLE person (
            ID INT64,
            fName STRING,
            gender INT64,
            age INT64,
            PRIMARY KEY (ID)
        )
        """
        self.conn.execute(create_person_schema)

        # Create edge table schema for meets
        # CSV: from, to, location, times, data
        create_meets_schema = """
        CREATE REL TABLE meets (
            FROM person TO person,
            location STRING,
            times INT64,
            data STRING
        )
        """
        self.conn.execute(create_meets_schema)

        # Copy person nodes first
        copy_person = f"""
        COPY person FROM (
            LOAD FROM "{person_csv}" (header=true, delimiter=",")
            RETURN ID, fName, gender, age
        )
        """
        self.conn.execute(copy_person)

        # Copy meets edges
        copy_meets = f'COPY meets FROM "{meets_csv}" (from="person", to="person", header=true, delimiter=",")'
        self.conn.execute(copy_meets)

        # Verify data with MATCH query
        query = """
        MATCH (a:person)-[m:meets]->(b:person)
        RETURN a.ID, a.fName, b.ID, b.fName, m.times, m.location
        ORDER BY a.ID, b.ID
        LIMIT 5
        """
        result = self.conn.execute(query)
        records = list(result)

        assert len(records) > 0, "Should have loaded at least one meets relationship"
        # Verify first relationship (0->2, Alice meets Bob)
        assert records[0][0] == 0, "Source person ID should be 0"
        assert records[0][1] == "Alice", "Source person name should be Alice"
        assert records[0][2] == 2, "Target person ID should be 2"
        assert records[0][3] == "Bob", "Target person name should be Bob"
        assert records[0][4] == 5, "Times should be 5"
        assert records[0][5] is not None, "Location should not be None"

    def test_copy_from_edge_with_column_remapping(self):
        """Test COPY FROM for edge table with column remapping."""
        person_csv = os.path.join(self.tinysnb_path, "vPerson.csv")
        meets_csv = os.path.join(self.tinysnb_path, "eMeets.csv")
        if not os.path.exists(person_csv) or not os.path.exists(meets_csv):
            pytest.skip("CSV files not found")

        # Create node table schema
        create_person_schema = """
        CREATE NODE TABLE person (
            ID INT64,
            fName STRING,
            gender INT64,
            age INT64,
            PRIMARY KEY (ID)
        )
        """
        self.conn.execute(create_person_schema)

        # Create edge table schema
        # CSV order: from, to, location, times, data
        # Schema order: from, to, times, location, data
        create_meets_schema = """
        CREATE REL TABLE meets (
            FROM person TO person,
            times INT64,
            location STRING,
            data STRING
        )
        """
        self.conn.execute(create_meets_schema)

        # Copy person nodes first
        copy_person = f"""
        COPY person FROM (
            LOAD FROM "{person_csv}" (header=true, delimiter=",")
            RETURN ID, fName, gender, age
        )
        """
        self.conn.execute(copy_person)

        # Copy meets edges with column remapping
        # CSV: from, to, location, times, data
        # We want: from, to, times, location, data
        copy_meets = f"""
        COPY meets FROM (
            LOAD FROM "{meets_csv}" (header=true, delimiter=",")
            RETURN from, to, times, location, data
        )
        """
        self.conn.execute(copy_meets)

        # Verify data with MATCH query
        query = """
        MATCH (a:person)-[m:meets]->(b:person)
        RETURN a.ID, b.ID, m.times, m.location
        ORDER BY a.ID, b.ID
        LIMIT 3
        """
        result = self.conn.execute(query)
        records = list(result)

        assert len(records) > 0, "Should have loaded at least one meets relationship"
        # Verify first relationship (0->2)
        assert records[0][0] == 0, "Source person ID should be 0"
        assert records[0][1] == 2, "Target person ID should be 2"
        assert records[0][2] == 5, "Times should be 5"
        assert records[0][3] is not None, "Location should not be None"

    def test_copy_from_node_reordered_all_columns(self):
        """Test COPY FROM with all columns reordered (complete remapping)."""
        csv_path = os.path.join(self.tinysnb_path, "vPerson.csv")
        if not os.path.exists(csv_path):
            pytest.skip(f"CSV file not found: {csv_path}")

        # Create schema with properties in different order than CSV
        # CSV: ID, fName, gender, isStudent, isWorker, age, ...
        # Schema: ID, age, fName, gender, isStudent, isWorker, ...
        create_schema = """
        CREATE NODE TABLE person_reordered (
            ID INT64,
            age INT64,
            fName STRING,
            gender INT64,
            isStudent BOOLEAN,
            isWorker BOOLEAN,
            eyeSight DOUBLE,
            PRIMARY KEY (ID)
        )
        """
        self.conn.execute(create_schema)

        # Copy with complete column remapping
        copy_query = f"""
        COPY person_reordered FROM (
            LOAD FROM "{csv_path}" (header=true, delimiter=",")
            RETURN ID, age, fName, gender, isStudent, isWorker, eyeSight
        )
        """
        self.conn.execute(copy_query)

        # Verify data
        query = """
        MATCH (p:person_reordered)
        RETURN p.ID, p.age, p.fName, p.gender, p.isStudent
        ORDER BY p.ID
        LIMIT 3
        """
        result = self.conn.execute(query)
        records = list(result)

        assert len(records) >= 3, "Should have loaded at least 3 persons"
        # Verify data integrity
        assert records[0][0] == 0 and records[0][2] == "Alice" and records[0][1] == 35
        assert records[1][0] == 2 and records[1][2] == "Bob" and records[1][1] == 30
        assert records[2][0] == 3 and records[2][2] == "Carol" and records[2][1] == 45
