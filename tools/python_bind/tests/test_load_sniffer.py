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
from datetime import date, datetime

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


class TestLoadSniffer:
    @staticmethod
    def _is_expected_interval(value):
        normalized = str(value).replace(" ", "")
        return normalized in {"1year2month", "1year2months"}

    @staticmethod
    def _assert_date(value, expected):
        assert isinstance(value, date)
        assert not isinstance(value, datetime)
        assert value == expected

    @staticmethod
    def _assert_datetime(value, expected):
        assert isinstance(value, datetime)
        assert value == expected

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = str(tmp_path / "test_load_sniffer_db")
        self.data_dir = tmp_path / "sniffer_data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        shutil.rmtree(self.db_dir, ignore_errors=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()
        yield
        self.conn.close()
        self.db.close()
        shutil.rmtree(self.db_dir, ignore_errors=True)

    def test_csv_type_inference_basic(self):
        csv_path = self.data_dir / "csv_basic.csv"
        csv_path.write_text(
            "i_col,f_col,b_col,s_col\n"
            "42,3.14,true,hello\n"
        )

        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true, delim=",")
            RETURN i_col, f_col, b_col, s_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]

        assert isinstance(row[0], int)
        assert isinstance(row[1], float)
        assert isinstance(row[2], bool)
        assert isinstance(row[3], str)
    def test_csv_type_inference_temporal(self):
        csv_path = self.data_dir / "csv_temporal.csv"
        csv_path.write_text(
            "d_col,ts_col,iv_col\n"
            "2012-01-02,2012-01-02 09:30:21,1 year 2 month\n"
        )

        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true, delim=",")
            RETURN d_col, ts_col, iv_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]

        self._assert_date(row[0], date(2012, 1, 2))
        self._assert_datetime(row[1], datetime(2012, 1, 2, 9, 30, 21))
        assert self._is_expected_interval(row[2])

    def test_csv_type_inference_list(self):
        csv_path = self.data_dir / "csv_list.csv"
        csv_path.write_text('list_col\n"[1, 2, 3]"\n')

        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true, delim=",")
            RETURN list_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        # CSV list is not inferred as list, remains string.
        assert isinstance(rows[0][0], str)

    def test_csv_type_inference_map(self):
        csv_path = self.data_dir / "csv_map.csv"
        csv_path.write_text('map_col\n"{\'a\': \'abc\', \'b\': \'bcd\'}"\n')

        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true, delim=",")
            RETURN map_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        # CSV map is not inferred as map, remains string.
        assert isinstance(rows[0][0], str)

    def test_load_from_return_cast_with_csv(self):
        csv_path = self.data_dir / "cast.csv"
        csv_path.write_text(
            "age,s_num,date_str,ts_str,bool_str\n"
            "35,123,2012-01-02,2012-01-02 09:30:21,true\n"
        )

        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true, delim=",")
            RETURN
                CAST(age, "DOUBLE"),
                CAST(s_num, "INT64"),
                CAST(age, "STRING"),
                CAST(date_str, "DATE"),
                CAST(ts_str, "TIMESTAMP"),
                CAST(bool_str, "BOOL")
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]

        assert isinstance(row[0], float) and row[0] == 35.0
        assert isinstance(row[1], int) and row[1] == 123
        assert isinstance(row[2], str) and row[2] == "35"
        self._assert_date(row[3], date(2012, 1, 2))
        self._assert_datetime(row[4], datetime(2012, 1, 2, 9, 30, 21))
        assert isinstance(row[5], bool) and row[5] is True

    @extension_test
    def test_json_type_inference_basic(self):
        self.conn.execute("LOAD JSON")
        json_path = self.data_dir / "json_basic.json"
        json_path.write_text(
            json.dumps(
                [
                    {
                        "i_col": 42,
                        "f_col": 3.14,
                        "b_col": True,
                        "s_col": "hello",
                    }
                ]
            )
        )

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN i_col, f_col, b_col, s_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]

        assert isinstance(row[0], int)
        assert isinstance(row[1], float)
        assert isinstance(row[2], bool)
        assert isinstance(row[3], str)
    @extension_test
    def test_json_type_inference_date(self):
        self.conn.execute("LOAD JSON")
        json_path = self.data_dir / "json_date.json"
        json_path.write_text(
            json.dumps(
                [
                    {
                        "d_col": "2012-01-02",
                    }
                ]
            )
        )

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN d_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        self._assert_date(rows[0][0], date(2012, 1, 2))

    @extension_test
    def test_json_type_inference_datetime(self):
        self.conn.execute("LOAD JSON")
        json_path = self.data_dir / "json_datetime.json"
        json_path.write_text(json.dumps([{"ts_col": "2012-01-02 09:30:21"}]))

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN ts_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        self._assert_datetime(rows[0][0], datetime(2012, 1, 2, 9, 30, 21))

    @extension_test
    def test_json_type_inference_interval(self):
        self.conn.execute("LOAD JSON")
        json_path = self.data_dir / "json_interval.json"
        json_path.write_text(json.dumps([{"iv_col": "1 year 2 month"}]))

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN iv_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        assert self._is_expected_interval(rows[0][0])

    @extension_test
    def test_json_type_inference_list(self):
        self.conn.execute("LOAD JSON")
        json_path = self.data_dir / "json_list.json"
        json_path.write_text(json.dumps([{"list_col": [1, 2, 3]}]))

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN list_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        assert isinstance(rows[0][0], list)
        assert rows[0][0] == [1, 2, 3]

    @extension_test
    def test_json_type_inference_map(self):
        self.conn.execute("LOAD JSON")
        json_path = self.data_dir / "json_map.json"
        json_path.write_text(json.dumps([{"map_col": {"a": "abc", "b": "bcd"}}]))

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN CAST(map_col, "STRING")
            """
        )
        rows = list(result)
        assert len(rows) == 1
        assert isinstance(rows[0][0], str)

    @extension_test
    def test_load_from_return_cast_with_json(self):
        self.conn.execute("LOAD JSON")
        json_path = self.data_dir / "cast.json"
        json_path.write_text(
            json.dumps(
                [
                    {
                        "age": 35,
                        "s_num": "123",
                        "date_str": "2012-01-02",
                        "ts_str": "2012-01-02 09:30:21",
                    }
                ]
            )
        )

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN
                CAST(age, "DOUBLE"),
                CAST(s_num, "INT64"),
                CAST(age, "STRING"),
                CAST(date_str, "DATE"),
                CAST(ts_str, "TIMESTAMP")
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]
        assert row[0] == 35.0
        assert row[1] == 123
        assert row[2] == "35"
        self._assert_date(row[3], date(2012, 1, 2))
        self._assert_datetime(row[4], datetime(2012, 1, 2, 9, 30, 21))

    @extension_test
    def test_parquet_type_inference_basic(self):
        pyarrow = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        self.conn.execute("LOAD PARQUET")
        parquet_path = self.data_dir / "parquet_basic.parquet"

        table = pyarrow.table(
            {
                "i_col": pyarrow.array([42], type=pyarrow.int64()),
                "f_col": pyarrow.array([3.14], type=pyarrow.float64()),
                "b_col": pyarrow.array([True], type=pyarrow.bool_()),
                "s_col": pyarrow.array(["hello"], type=pyarrow.string()),
            }
        )
        pq.write_table(table, parquet_path)

        result = self.conn.execute(
            f"""
            LOAD FROM "{parquet_path}"
            RETURN i_col, f_col, b_col, s_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]

        assert isinstance(row[0], int)
        assert isinstance(row[1], float)
        assert isinstance(row[2], bool)
        assert isinstance(row[3], str)
    @extension_test
    def test_parquet_type_inference_temporal(self):
        pyarrow = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        self.conn.execute("LOAD PARQUET")
        parquet_path = self.data_dir / "parquet_temporal.parquet"

        table = pyarrow.table(
            {
                "d_col": pyarrow.array(["2012-01-02"], type=pyarrow.string()),
                "ts_col": pyarrow.array(
                    [datetime(2012, 1, 2, 9, 30, 21)], type=pyarrow.timestamp("ms")
                ),
                "iv_col": pyarrow.array(["1 year 2 month"], type=pyarrow.string()),
            }
        )
        pq.write_table(table, parquet_path)

        result = self.conn.execute(
            f"""
            LOAD FROM "{parquet_path}"
            RETURN CAST(d_col, "DATE"), ts_col, CAST(iv_col, "INTERVAL")
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]
        self._assert_date(row[0], date(2012, 1, 2))
        self._assert_datetime(row[1], datetime(2012, 1, 2, 9, 30, 21))
        assert self._is_expected_interval(row[2])

    @extension_test
    def test_parquet_type_inference_list(self):
        pyarrow = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        self.conn.execute("LOAD PARQUET")
        parquet_path = self.data_dir / "parquet_list.parquet"

        table = pyarrow.table(
            {
                "list_col": pyarrow.array([[1, 2, 3]], type=pyarrow.list_(pyarrow.int64())),
            }
        )
        pq.write_table(table, parquet_path)

        result = self.conn.execute(
            f"""
            LOAD FROM "{parquet_path}"
            RETURN list_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        assert isinstance(rows[0][0], list)
        assert rows[0][0] == [1, 2, 3]

    @extension_test
    def test_parquet_type_inference_map(self):
        pyarrow = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        self.conn.execute("LOAD PARQUET")
        parquet_path = self.data_dir / "parquet_map.parquet"

        table = pyarrow.table(
            {
                "map_col": pyarrow.array(
                    [[("a", "abc"), ("b", "bcd")]],
                    type=pyarrow.map_(pyarrow.string(), pyarrow.string()),
                ),
            }
        )
        pq.write_table(table, parquet_path)

        result = self.conn.execute(
            f"""
            LOAD FROM "{parquet_path}"
            RETURN CAST(map_col, "STRING")
            """
        )
        rows = list(result)
        assert len(rows) == 1
        assert isinstance(rows[0][0], str)
