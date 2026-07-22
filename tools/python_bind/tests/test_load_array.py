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

"""End-to-end LOAD FROM tests for fixed-size array columns."""

import json
import os
import shutil
import sys
from datetime import date
from datetime import datetime
from datetime import timedelta

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug import Database

EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
extension_test = pytest.mark.skipif(
    not EXTENSION_TESTS_ENABLED,
    reason="Extension tests disabled by default; set NEUG_RUN_EXTENSION_TESTS=1 to enable.",
)


class TestLoadArray:
    """Test cases for LOAD FROM CSV with CAST to array types."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Setup test database and CSV directory."""
        self.db_dir = str(tmp_path / "test_load_array_db")
        self.csv_dir = str(tmp_path / "csv_data")
        self.json_dir = str(tmp_path / "json_data")
        self.parquet_dir = str(tmp_path / "parquet_data")
        shutil.rmtree(self.db_dir, ignore_errors=True)
        os.makedirs(self.csv_dir, exist_ok=True)
        os.makedirs(self.json_dir, exist_ok=True)
        os.makedirs(self.parquet_dir, exist_ok=True)
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

    def _write_json(self, filename, data):
        """Write a JSON file to the temp directory and return its path."""
        path = os.path.join(self.json_dir, filename)
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    def _write_parquet(self, filename, columns):
        """Write an Arrow table containing fixed-size list columns."""
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        path = os.path.join(self.parquet_dir, filename)
        pq.write_table(pa.table(columns), path)
        return path

    def test_copy_csv_array_with_explicit_cast(self):
        """COPY an ARRAY property through LOAD FROM with an explicit type."""
        csv_path = self._write_csv(
            "person_array.csv",
            "id,name,address\n"
            '1,Alice,"[Beijing,Hangzhou,Shanghai]"\n'
            '2,Bob,"[London,Paris,Berlin]"\n',
        )
        self.conn.execute(
            "CREATE NODE TABLE Person("
            "id INT64, name STRING, addresses STRING[3], PRIMARY KEY(id));"
        )

        self.conn.execute(
            f"""
            COPY Person FROM (
                LOAD FROM "{csv_path}" (delim=',')
                RETURN id, name, CAST(address, 'STRING[3]') AS addresses
            )
            """
        )

        result = list(
            self.conn.execute(
                "MATCH (p:Person) " "RETURN p.id, p.name, p.addresses ORDER BY p.id"
            )
        )
        assert result == [
            [1, "Alice", ["Beijing", "Hangzhou", "Shanghai"]],
            [2, "Bob", ["London", "Paris", "Berlin"]],
        ]

    @extension_test
    def test_parquet_float_array(self):
        """LOAD FROM Parquet with a FLOAT[3] fixed-size array."""
        pa = pytest.importorskip("pyarrow")
        parquet_path = self._write_parquet(
            "float_array.parquet",
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "values": pa.array(
                    [[1.5, 2.5, 3.5], [4.0, 5.0, 6.0]],
                    type=pa.list_(pa.float32(), 3),
                ),
            },
        )
        self.conn.execute("LOAD PARQUET")
        result = list(
            self.conn.execute(
                f'LOAD FROM "{parquet_path}" '
                "RETURN id, CAST(values, 'FLOAT[3]') ORDER BY id"
            )
        )

        assert result == [
            [1, [1.5, 2.5, 3.5]],
            [2, [4.0, 5.0, 6.0]],
        ]

    @extension_test
    def test_parquet_nested_string_array(self):
        """LOAD FROM Parquet with a nested STRING[2][2] array."""
        pa = pytest.importorskip("pyarrow")
        inner_type = pa.list_(pa.string(), 2)
        parquet_path = self._write_parquet(
            "nested_string_array.parquet",
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "values": pa.array(
                    [[["a", "b"], ["c", "d"]], [["w", "x"], ["y", "z"]]],
                    type=pa.list_(inner_type, 2),
                ),
            },
        )
        self.conn.execute("LOAD PARQUET")
        result = list(
            self.conn.execute(
                f'LOAD FROM "{parquet_path}" '
                "RETURN id, CAST(values, 'STRING[2][2]') ORDER BY id"
            )
        )

        assert result == [
            [1, [["a", "b"], ["c", "d"]]],
            [2, [["w", "x"], ["y", "z"]]],
        ]

    @extension_test
    def test_parquet_null_array(self):
        """LOAD FROM Parquet preserves a null fixed-size array."""
        pa = pytest.importorskip("pyarrow")
        parquet_path = self._write_parquet(
            "null_array.parquet",
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "values": pa.array(
                    [[1.0, 2.0, 3.0], None, [7.0, 8.0, 9.0]],
                    type=pa.list_(pa.float32(), 3),
                ),
            },
        )
        self.conn.execute("LOAD PARQUET")
        result = list(
            self.conn.execute(
                f'LOAD FROM "{parquet_path}" '
                "RETURN id, CAST(values, 'FLOAT[3]') ORDER BY id"
            )
        )

        assert result == [
            [1, [1.0, 2.0, 3.0]],
            [2, None],
            [3, [7.0, 8.0, 9.0]],
        ]

    @extension_test
    def test_parquet_array_types(self):
        """Cover the scalar and nested ARRAY types exercised by CSV."""
        pa = pytest.importorskip("pyarrow")
        parquet_path = self._write_parquet(
            "array_types.parquet",
            {
                "id": pa.array([1], type=pa.int64()),
                "floats": pa.array([[1.5, 2.5, 3.5]], pa.list_(pa.float32(), 3)),
                "doubles": pa.array([[1.1, 2.2]], pa.list_(pa.float64(), 2)),
                "int32s": pa.array([[10, 20, 30]], pa.list_(pa.int32(), 3)),
                "int64s": pa.array(
                    [[100000000000, 200000000000]], pa.list_(pa.int64(), 2)
                ),
                "strings": pa.array(
                    [["hello", "world", "test"]], pa.list_(pa.string(), 3)
                ),
                "int_matrix": pa.array(
                    [[[1, 2], [3, 4]]],
                    pa.list_(pa.list_(pa.int32(), 2), 2),
                ),
                "string_matrix": pa.array(
                    [[["a", "b"], ["c", "d"]]],
                    pa.list_(pa.list_(pa.string(), 2), 2),
                ),
                "dates": pa.array(
                    [[date(1970, 1, 1), date(2023, 6, 15)]],
                    pa.list_(pa.date64(), 2),
                ),
                "times": pa.array(
                    [[datetime(1970, 1, 1), datetime(2023, 6, 15, 12, 30)]],
                    pa.list_(pa.timestamp("ms"), 2),
                ),
                "durations": pa.array(
                    [[timedelta(days=2), timedelta(hours=3)]],
                    pa.list_(pa.duration("ms"), 2),
                ),
            },
        )
        self.conn.execute("LOAD PARQUET")
        result = list(
            self.conn.execute(
                f"""
                LOAD FROM "{parquet_path}"
                RETURN id,
                       CAST(floats, 'FLOAT[3]'),
                       CAST(doubles, 'DOUBLE[2]'),
                       CAST(int32s, 'INT32[3]'),
                       CAST(int64s, 'INT64[2]'),
                       CAST(strings, 'STRING[3]'),
                       CAST(int_matrix, 'INT32[2][2]'),
                       CAST(string_matrix, 'STRING[2][2]'),
                       CAST(dates, 'DATE[2]'),
                       CAST(times, 'TIMESTAMP[2]'),
                       CAST(durations, 'INTERVAL[2]')
                """
            )
        )

        assert result == [
            [
                1,
                [1.5, 2.5, 3.5],
                [1.1, 2.2],
                [10, 20, 30],
                [100000000000, 200000000000],
                ["hello", "world", "test"],
                [[1, 2], [3, 4]],
                [["a", "b"], ["c", "d"]],
                [date(1970, 1, 1), date(2023, 6, 15)],
                [datetime(1970, 1, 1), datetime(2023, 6, 15, 12, 30)],
                ["2 days", "3 hours"],
            ]
        ]

    def test_cast_float_array(self):
        """LOAD FROM CSV with CAST(col, 'FLOAT[3]')."""
        csv_path = self._write_csv(
            "float_array.csv",
            "id|values\n"
            "1|[1.5, 2.5, 3.5]\n"
            "2|[4.0, 5.0, 6.0]\n"
            "3|[0.0, 0.0, 0.0]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(values, 'FLOAT[3]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 3

        # Row 1: [1.5, 2.5, 3.5]
        assert len(result[0][1]) == 3
        assert abs(result[0][1][0] - 1.5) < 1e-5
        assert abs(result[0][1][1] - 2.5) < 1e-5
        assert abs(result[0][1][2] - 3.5) < 1e-5

        # Row 2: [4.0, 5.0, 6.0]
        assert len(result[1][1]) == 3
        assert abs(result[1][1][0] - 4.0) < 1e-5

        # Row 3: [0.0, 0.0, 0.0]
        assert result[2][1] == [0.0, 0.0, 0.0]

    def test_cast_double_array(self):
        """LOAD FROM CSV with CAST(col, 'DOUBLE[2]')."""
        csv_path = self._write_csv(
            "double_array.csv",
            "id|values\n"
            "1|[1.111111111, 2.222222222]\n"
            "2|[3.333333333, 4.444444444]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(values, 'DOUBLE[2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2

        assert len(result[0][1]) == 2
        assert abs(result[0][1][0] - 1.111111111) < 1e-9
        assert abs(result[0][1][1] - 2.222222222) < 1e-9

        assert len(result[1][1]) == 2
        assert abs(result[1][1][0] - 3.333333333) < 1e-9

    def test_cast_int32_array(self):
        """LOAD FROM CSV with CAST(col, 'INT32[3]')."""
        csv_path = self._write_csv(
            "int32_array.csv",
            "id|nums\n" "1|[10, 20, 30]\n" "2|[-1, 0, 1]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(nums, 'INT32[3]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [10, 20, 30]
        assert result[1][1] == [-1, 0, 1]

    def test_cast_int64_array(self):
        """LOAD FROM CSV with CAST(col, 'INT64[2]')."""
        csv_path = self._write_csv(
            "int64_array.csv",
            "id|nums\n" "1|[100000000000, 200000000000]\n" "2|[0, 1]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(nums, 'INT64[2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [100000000000, 200000000000]
        assert result[1][1] == [0, 1]

    def test_cast_string_array(self):
        """LOAD FROM CSV with CAST(col, 'STRING[3]')."""
        csv_path = self._write_csv(
            "string_array.csv",
            "id|tags\n" "1|[hello, world, test]\n" "2|[foo, bar, baz]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(tags, 'STRING[3]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == ["hello", "world", "test"]
        assert result[1][1] == ["foo", "bar", "baz"]

    def test_cast_nested_int32_array(self):
        """LOAD FROM CSV with CAST(col, 'INT32[2][2]') - nested fixed-size array."""
        csv_path = self._write_csv(
            "nested_int_array.csv",
            "id|matrix\n" "1|[[1, 2], [3, 4]]\n" "2|[[5, 6], [7, 8]]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(matrix, 'INT32[2][2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [[1, 2], [3, 4]]
        assert result[1][1] == [[5, 6], [7, 8]]

    def test_cast_nested_string_array(self):
        """LOAD FROM CSV with CAST(col, 'STRING[2][2]') - nested string array."""
        csv_path = self._write_csv(
            "nested_string_array.csv",
            "id|data\n" "1|[[a, b], [c, d]]\n" "2|[[w, x], [y, z]]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(data, 'STRING[2][2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [["a", "b"], ["c", "d"]]
        assert result[1][1] == [["w", "x"], ["y", "z"]]

    def test_cast_date_array(self):
        """LOAD FROM CSV with CAST(col, 'DATE[2]')."""
        csv_path = self._write_csv(
            "date_array.csv",
            "id|dates\n" "1|[1970-01-01, 2023-06-15]\n" "2|[2000-01-01, 2001-01-01]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(dates, 'DATE[2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert len(result[0][1]) == 2
        # Verify dates are returned as datetime.date objects
        assert result[0][1][0] == date(1970, 1, 1)
        assert result[0][1][1] == date(2023, 6, 15)
        assert len(result[1][1]) == 2
        assert result[1][1][0] == date(2000, 1, 1)
        assert result[1][1][1] == date(2001, 1, 1)

    def test_cast_timestamp_array(self):
        """LOAD FROM CSV with CAST(col, 'TIMESTAMP[2]')."""
        csv_path = self._write_csv(
            "timestamp_array.csv",
            "id|times\n"
            "1|[1970-01-01 00:00:00, 2023-06-15 12:30:00]\n"
            "2|[2000-01-01 00:00:00, 2001-01-01 00:00:00]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(times, 'TIMESTAMP[2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert len(result[0][1]) == 2
        # Verify timestamps are returned as datetime.datetime objects
        assert result[0][1][0] == datetime(1970, 1, 1, 0, 0)
        assert result[0][1][1] == datetime(2023, 6, 15, 12, 30)
        assert len(result[1][1]) == 2
        assert result[1][1][0] == datetime(2000, 1, 1, 0, 0)
        assert result[1][1][1] == datetime(2001, 1, 1, 0, 0)

    def test_cast_interval_array(self):
        """LOAD FROM CSV with CAST(col, 'INTERVAL[2]')."""
        csv_path = self._write_csv(
            "interval_array.csv",
            "id|durations\n" "1|[2 days, 3 hours]\n" "2|[1 year 2 months, 4 days]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(durations, 'INTERVAL[2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        # Verify intervals are returned as strings
        assert result[0][1] == ["2 days", "3 hours"]
        assert result[1][1] == ["1 year 2 months", "4 days"]

    def test_cast_multiple_array_columns(self):
        """LOAD FROM CSV with multiple CAST array columns."""
        csv_path = self._write_csv(
            "multi_array.csv",
            "id|ints|floats\n" "1|[1, 2, 3]|[1.1, 2.2]\n" "2|[4, 5, 6]|[3.3, 4.4]\n",
        )
        query = f"""
        LOAD FROM "{csv_path}" (delim='|')
        RETURN id, CAST(ints, 'INT64[3]'), CAST(floats, 'DOUBLE[2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == [1, 2, 3]
        assert len(result[0][2]) == 2
        assert abs(result[0][2][0] - 1.1) < 1e-9
        assert result[1][1] == [4, 5, 6]

    def test_cast_json_numeric_arrays(self):
        """LOAD FROM JSON with numeric fixed-size array casts."""
        json_path = self._write_json(
            "numeric_arrays.json",
            [
                {
                    "id": 1,
                    "floats": [1.5, 2.5, 3.5],
                    "doubles": [1.111111111, 2.222222222],
                    "int32s": [10, 20],
                    "int64s": [100000000000, 200000000000],
                },
                {
                    "id": 2,
                    "floats": [4.0, 5.0, 6.0],
                    "doubles": [3.333333333, 4.444444444],
                    "int32s": [-1, 0],
                    "int64s": [0, 1],
                },
            ],
        )
        query = f"""
        LOAD FROM "{json_path}"
        RETURN id,
               CAST(floats, 'FLOAT[3]'),
               CAST(doubles, 'DOUBLE[2]'),
               CAST(int32s, 'INT32[2]'),
               CAST(int64s, 'INT64[2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2

        assert len(result[0][1]) == 3
        assert abs(result[0][1][0] - 1.5) < 1e-5
        assert abs(result[0][1][1] - 2.5) < 1e-5
        assert abs(result[0][1][2] - 3.5) < 1e-5
        assert result[0][2] == [1.111111111, 2.222222222]
        assert result[0][3] == [10, 20]
        assert result[0][4] == [100000000000, 200000000000]

        assert len(result[1][1]) == 3
        assert abs(result[1][1][0] - 4.0) < 1e-5
        assert result[1][2] == [3.333333333, 4.444444444]
        assert result[1][3] == [-1, 0]
        assert result[1][4] == [0, 1]

    def test_cast_json_string_and_nested_arrays(self):
        """LOAD FROM JSON with string and nested fixed-size array casts."""
        json_path = self._write_json(
            "nested_arrays.json",
            [
                {
                    "id": 1,
                    "tags": ["hello", "world"],
                    "matrix": [[1, 2], [3, 4]],
                    "labels": [["a", "b"], ["c", "d"]],
                },
                {
                    "id": 2,
                    "tags": ["foo", "bar"],
                    "matrix": [[5, 6], [7, 8]],
                    "labels": [["w", "x"], ["y", "z"]],
                },
            ],
        )
        query = f"""
        LOAD FROM "{json_path}"
        RETURN id,
               CAST(tags, 'STRING[2]'),
               CAST(matrix, 'INT32[2][2]'),
               CAST(labels, 'STRING[2][2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2
        assert result[0][1] == ["hello", "world"]
        assert result[0][2] == [[1, 2], [3, 4]]
        assert result[0][3] == [["a", "b"], ["c", "d"]]
        assert result[1][1] == ["foo", "bar"]
        assert result[1][2] == [[5, 6], [7, 8]]
        assert result[1][3] == [["w", "x"], ["y", "z"]]

    def test_cast_json_temporal_arrays(self):
        """LOAD FROM JSON with temporal fixed-size array casts."""
        json_path = self._write_json(
            "temporal_arrays.json",
            [
                {
                    "id": 1,
                    "dates": ["1970-01-01", "2023-06-15"],
                    "times": ["1970-01-01 00:00:00", "2023-06-15 12:30:00"],
                    "durations": ["2 days", "3 hours"],
                },
                {
                    "id": 2,
                    "dates": ["2000-01-01", "2001-01-01"],
                    "times": ["2000-01-01 00:00:00", "2001-01-01 00:00:00"],
                    "durations": ["1 year 2 months", "4 days"],
                },
            ],
        )
        query = f"""
        LOAD FROM "{json_path}"
        RETURN id,
               CAST(dates, 'DATE[2]'),
               CAST(times, 'TIMESTAMP[2]'),
               CAST(durations, 'INTERVAL[2]')
        """
        result = list(self.conn.execute(query))
        assert len(result) == 2

        assert result[0][1] == [date(1970, 1, 1), date(2023, 6, 15)]
        assert result[0][2] == [
            datetime(1970, 1, 1, 0, 0),
            datetime(2023, 6, 15, 12, 30),
        ]
        assert result[0][3] == ["2 days", "3 hours"]

        assert result[1][1] == [date(2000, 1, 1), date(2001, 1, 1)]
        assert result[1][2] == [
            datetime(2000, 1, 1, 0, 0),
            datetime(2001, 1, 1, 0, 0),
        ]
        assert result[1][3] == ["1 year 2 months", "4 days"]
