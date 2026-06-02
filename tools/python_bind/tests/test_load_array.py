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

"""End-to-end tests for CSV LOAD FROM with array (list) type columns via CAST syntax."""

import os
import shutil
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug import Database


class TestLoadArray:
    """Test cases for LOAD FROM CSV with CAST to array types."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Setup test database and CSV directory."""
        self.db_dir = str(tmp_path / "test_load_array_db")
        self.csv_dir = str(tmp_path / "csv_data")
        shutil.rmtree(self.db_dir, ignore_errors=True)
        os.makedirs(self.csv_dir, exist_ok=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def _write_csv(self, filename, content):
        """Write a CSV file to the temp directory and return its path."""
        path = os.path.join(self.csv_dir, filename)
        with open(path, "w") as f:
            f.write(content)
        return path

    def test_cast_float_array(self):
        """LOAD FROM CSV with CAST(col, 'FLOAT[]')."""
        csv_path = self._write_csv(
            "float_array.csv",
            "id|values\n" "1|[1.5, 2.5, 3.5]\n" "2|[4.0, 5.0]\n" "3|[]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(values, 'FLOAT[]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 3

        # Row 1: [1.5, 2.5, 3.5]
        assert len(result[0][1]) == 3
        assert abs(result[0][1][0] - 1.5) < 1e-5
        assert abs(result[0][1][1] - 2.5) < 1e-5
        assert abs(result[0][1][2] - 3.5) < 1e-5

        # Row 2: [4.0, 5.0]
        assert len(result[1][1]) == 2
        assert abs(result[1][1][0] - 4.0) < 1e-5

        # Row 3: empty list
        assert len(result[2][1]) == 0

    def test_cast_double_array(self):
        """LOAD FROM CSV with CAST(col, 'DOUBLE[]')."""
        csv_path = self._write_csv(
            "double_array.csv",
            "id|values\n" "1|[1.111111111, 2.222222222]\n" "2|[3.333333333]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(values, 'DOUBLE[]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2

        assert len(result[0][1]) == 2
        assert abs(result[0][1][0] - 1.111111111) < 1e-9
        assert abs(result[0][1][1] - 2.222222222) < 1e-9

        assert len(result[1][1]) == 1
        assert abs(result[1][1][0] - 3.333333333) < 1e-9

    def test_cast_int32_array(self):
        """LOAD FROM CSV with CAST(col, 'INT32[]')."""
        csv_path = self._write_csv(
            "int32_array.csv",
            "id|nums\n" "1|[10, 20, 30]\n" "2|[-1, 0, 1]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(nums, 'INT32[]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [10, 20, 30]
        assert result[1][1] == [-1, 0, 1]

    def test_cast_int64_array(self):
        """LOAD FROM CSV with CAST(col, 'INT64[]')."""
        csv_path = self._write_csv(
            "int64_array.csv",
            "id|nums\n" "1|[100000000000, 200000000000]\n" "2|[0]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(nums, 'INT64[]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [100000000000, 200000000000]
        assert result[1][1] == [0]

    def test_cast_string_array(self):
        """LOAD FROM CSV with CAST(col, 'STRING[]')."""
        csv_path = self._write_csv(
            "string_array.csv",
            "id|tags\n" "1|[hello, world]\n" "2|[foo, bar, baz]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(tags, 'STRING[]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == ["hello", "world"]
        assert result[1][1] == ["foo", "bar", "baz"]

    def test_cast_nested_int32_array(self):
        """LOAD FROM CSV with CAST(col, 'INT32[][]') — nested list."""
        csv_path = self._write_csv(
            "nested_int_array.csv",
            "id|matrix\n" "1|[[1, 2], [3, 4]]\n" "2|[[5], [6, 7, 8]]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(matrix, 'INT32[][]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [[1, 2], [3, 4]]
        assert result[1][1] == [[5], [6, 7, 8]]

    def test_cast_nested_string_array(self):
        """LOAD FROM CSV with CAST(col, 'STRING[][]') — nested string list."""
        csv_path = self._write_csv(
            "nested_string_array.csv",
            "id|data\n" "1|[[a, b], [c]]\n" "2|[[x, y, z]]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(data, 'STRING[][]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [["a", "b"], ["c"]]
        assert result[1][1] == [["x", "y", "z"]]

    def test_cast_date_array(self):
        """LOAD FROM CSV with CAST(col, 'DATE[]')."""
        csv_path = self._write_csv(
            "date_array.csv",
            "id|dates\n" "1|[1970-01-01, 2023-06-15]\n" "2|[2000-01-01]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(dates, 'DATE[]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert len(result[0][1]) == 2
        # Verify dates are returned (as date objects or strings)
        assert str(result[0][1][0]) == "1970-01-01"
        assert str(result[0][1][1]) == "2023-06-15"
        assert len(result[1][1]) == 1
        assert str(result[1][1][0]) == "2000-01-01"

    def test_cast_timestamp_array(self):
        """LOAD FROM CSV with CAST(col, 'TIMESTAMP[]')."""
        csv_path = self._write_csv(
            "timestamp_array.csv",
            "id|times\n"
            "1|[1970-01-01 00:00:00, 2023-06-15 12:30:00]\n"
            "2|[2000-01-01 00:00:00]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(times, 'TIMESTAMP[]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert len(result[0][1]) == 2
        # Verify epoch timestamp
        assert "1970-01-01" in str(result[0][1][0])
        assert "2023-06-15" in str(result[0][1][1])
        assert len(result[1][1]) == 1
        assert "2000-01-01" in str(result[1][1][0])

    def test_cast_interval_array(self):
        """LOAD FROM CSV with CAST(col, 'INTERVAL[]')."""
        csv_path = self._write_csv(
            "interval_array.csv",
            "id|durations\n" "1|[2 days, 3 hours]\n" "2|[1 year 2 months]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(durations, 'INTERVAL[]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert len(result[0][1]) == 2
        assert len(result[1][1]) == 1

    def test_cast_multiple_array_columns(self):
        """LOAD FROM CSV with multiple CAST array columns."""
        csv_path = self._write_csv(
            "multi_array.csv",
            "id|ints|floats\n" "1|[1, 2, 3]|[1.1, 2.2]\n" "2|[4, 5]|[3.3]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(ints, 'INT64[]'), CAST(floats, 'DOUBLE[]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [1, 2, 3]
        assert len(result[0][2]) == 2
        assert abs(result[0][2][0] - 1.1) < 1e-9
        assert result[1][1] == [4, 5]
