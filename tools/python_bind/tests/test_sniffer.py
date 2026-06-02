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
from datetime import date
from datetime import datetime

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

    @staticmethod
    def _get_comprehensive_parquet_path(filename):
        current_file = os.path.abspath(__file__)
        tests_dir = os.path.dirname(current_file)
        python_bind_dir = os.path.dirname(tests_dir)
        tools_dir = os.path.dirname(python_bind_dir)
        workspace_root = os.path.dirname(tools_dir)
        parquet_dir = os.path.join(
            workspace_root, "example_dataset", "comprehensive_graph", "parquet"
        )
        parquet_path = os.path.join(parquet_dir, filename)
        if not os.path.exists(parquet_path):
            pytest.skip(f"Parquet file not found: {parquet_path}")
        return parquet_path

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
        csv_path.write_text("i_col,f_col,b_col,s_col\n" "42,3.14,true,hello\n")

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
            "d_col,ts_col,iv_col\n" "2012-01-02,2012-01-02 09:30:21,1 year 2 month\n"
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
        csv_path.write_text("map_col\n\"{'a': 'abc', 'b': 'bcd'}\"\n")

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

    def test_csv_cast_numeric_conversions(self):
        csv_path = self.data_dir / "csv_cast_numeric.csv"
        csv_path.write_text("i_col,d_col\n42,3.25\n")

        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true, delim=",")
            RETURN CAST(i_col, "INT32"), CAST(d_col, "FLOAT")
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]
        assert isinstance(row[0], int) and row[0] == 42
        assert isinstance(row[1], float) and abs(row[1] - 3.25) < 1e-6

    @pytest.mark.xfail(
        reason="TODO: support casting ISO datetime string to DATE in LOAD FROM."
    )
    def test_csv_cast_temporal_conversions(self):
        csv_path = self.data_dir / "csv_cast_temporal.csv"
        csv_path.write_text("d_col,ts_col\n" "2012-01-02,2012-01-02 09:30:21\n")

        result = self.conn.execute(
            f"""
            LOAD FROM "{csv_path}" (header=true, delim=",")
            RETURN CAST(d_col, "TIMESTAMP"), CAST(ts_col, "DATE")
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]
        self._assert_datetime(row[0], datetime(2012, 1, 2, 0, 0, 0))
        self._assert_date(row[1], date(2012, 1, 2))

    def test_json_type_inference_basic(self):
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

    @pytest.mark.xfail(
        reason="TODO: align JSON date inference behavior (DATE vs DATETIME)."
    )
    def test_json_type_inference_date(self):
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

    def test_json_type_inference_datetime(self):
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

    def test_json_type_inference_interval(self):
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

    @pytest.mark.xfail(reason="TODO: support JSON list type inference in LOAD FROM.")
    def test_json_type_inference_list(self):
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

    @pytest.mark.xfail(
        reason="TODO: support JSON map/object type inference in LOAD FROM."
    )
    def test_json_type_inference_map(self):
        json_path = self.data_dir / "json_map.json"
        json_path.write_text(json.dumps([{"map_col": {"a": "abc", "b": "bcd"}}]))

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN map_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        assert isinstance(rows[0][0], dict)
        assert rows[0][0] == {"a": "abc", "b": "bcd"}

    def test_json_cast_numeric_conversions(self):
        json_path = self.data_dir / "json_cast_numeric.json"
        json_path.write_text(json.dumps([{"i_col": 42, "d_col": 3.25}]))

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN CAST(i_col, "INT32"), CAST(d_col, "FLOAT")
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]
        assert isinstance(row[0], int) and row[0] == 42
        assert isinstance(row[1], float) and abs(row[1] - 3.25) < 1e-6

    @pytest.mark.xfail(
        reason="TODO: support casting ISO datetime string to DATE in LOAD FROM."
    )
    def test_json_cast_temporal_conversions(self):
        json_path = self.data_dir / "json_cast_temporal.json"
        json_path.write_text(
            json.dumps([{"d_col": "2012-01-02", "ts_col": "2012-01-02 09:30:21"}])
        )

        result = self.conn.execute(
            f"""
            LOAD FROM "{json_path}"
            RETURN CAST(d_col, "TIMESTAMP"), CAST(ts_col, "DATE")
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]
        self._assert_datetime(row[0], datetime(2012, 1, 2, 0, 0, 0))
        self._assert_date(row[1], date(2012, 1, 2))

    @extension_test
    def test_parquet_type_inference_basic(self):
        self.conn.execute("LOAD PARQUET")
        parquet_path = self._get_comprehensive_parquet_path("node_a.parquet")

        result = self.conn.execute(
            f"""
            LOAD FROM "{parquet_path}"
            RETURN id, i32_property, f64_property, str_property
            LIMIT 1
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]

        assert isinstance(row[0], int)
        assert isinstance(row[1], int)
        assert isinstance(row[2], float)
        assert isinstance(row[3], str)

    @extension_test
    def test_parquet_type_inference_temporal(self):
        self.conn.execute("LOAD PARQUET")
        parquet_path = self._get_comprehensive_parquet_path("node_a.parquet")

        result = self.conn.execute(
            f"""
            LOAD FROM "{parquet_path}"
            RETURN date_property, datetime_property, CAST(interval_property, "INTERVAL")
            LIMIT 1
            """
        )
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]
        assert isinstance(row[0], date)
        assert isinstance(row[1], datetime)
        assert isinstance(row[2], object)

    @extension_test
    @pytest.mark.xfail(reason="TODO: support Parquet list arrow type in LOAD FROM.")
    def test_parquet_type_inference_list(self):
        self.conn.execute("LOAD PARQUET")
        parquet_path = self._get_comprehensive_parquet_path("parquet_list.parquet")

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
    @pytest.mark.xfail(reason="TODO: support Parquet map type inference in LOAD FROM.")
    def test_parquet_type_inference_map(self):
        self.conn.execute("LOAD PARQUET")
        parquet_path = self._get_comprehensive_parquet_path("parquet_map.parquet")

        result = self.conn.execute(
            f"""
            LOAD FROM "{parquet_path}"
            RETURN map_col
            """
        )
        rows = list(result)
        assert len(rows) == 1
        assert isinstance(rows[0][0], dict)
        assert rows[0][0] == {"a": "abc", "b": "bcd"}

    @extension_test
    def test_copy_edge_from_parquet_non_standard_columns(self):
        """Issue #409: COPY edge FROM parquet should work when columns are
        named src_id/dst_id instead of from/to."""
        self.conn.execute("LOAD PARQUET")
        node_path = self._get_comprehensive_parquet_path("node_a.parquet")
        rel_path = self._get_comprehensive_parquet_path("rel_a.parquet")

        self.conn.execute(
            """
            CREATE NODE TABLE node_a(
                id INT64, i32_property INT32, i64_property INT64,
                u32_property UINT32, u64_property UINT64,
                f32_property FLOAT, f64_property DOUBLE,
                str_property STRING, date_property DATE,
                datetime_property TIMESTAMP, interval_property STRING,
                PRIMARY KEY(id)
            );
            """
        )
        self.conn.execute(
            """
            CREATE REL TABLE rel_a(
                FROM node_a TO node_a,
                double_weight DOUBLE, i32_weight INT32,
                i64_weight INT64, datetime_weight TIMESTAMP
            );
            """
        )
        self.conn.execute(f'COPY node_a FROM "{node_path}";')
        self.conn.execute(
            f'COPY rel_a FROM "{rel_path}" (from="node_a", to="node_a");'
        )

        result = self.conn.execute(
            "MATCH ()-[r:rel_a]->() RETURN count(r);"
        )
        count = next(result)[0]
        assert count == 10, f"Expected 10 edges, got {count}"

    @extension_test
    def test_copy_edge_from_parquet_custom_src_dst(self):
        """Issue #409: COPY edge FROM parquet with custom src/dst column names
        should load all rows, not silently return 0."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        self.conn.execute("LOAD PARQUET")

        node_path = self.data_dir / "nodes.parquet"
        pq.write_table(
            pa.table({
                "id": pa.array([1, 2, 3, 4], type=pa.int64()),
                "name": pa.array(["Alice", "Bob", "Carol", "Dave"]),
            }),
            str(node_path),
        )

        edge_path = self.data_dir / "edges_src_dst.parquet"
        pq.write_table(
            pa.table({
                "src": pa.array([1, 2, 3, 1], type=pa.int64()),
                "dst": pa.array([2, 3, 4, 4], type=pa.int64()),
                "weight": pa.array([1.0, 2.0, 3.0, 4.0], type=pa.float64()),
            }),
            str(edge_path),
        )

        self.conn.execute(
            "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"
        )
        self.conn.execute(
            "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"
        )
        self.conn.execute(f'COPY person FROM "{node_path}";')
        self.conn.execute(
            f'COPY knows FROM "{edge_path}" (from="person", to="person");'
        )

        result = self.conn.execute(
            "MATCH ()-[k:knows]->() RETURN count(k);"
        )
        count = next(result)[0]
        assert count == 4, f"Expected 4 edges, got {count}"

        result = self.conn.execute(
            "MATCH (a:person)-[k:knows]->(b:person) "
            "RETURN a.name, b.name, k.weight ORDER BY k.weight;"
        )
        rows = list(result)
        assert len(rows) == 4
        assert str(rows[0][0]) == "Alice" and str(rows[0][1]) == "Bob"
        assert str(rows[1][0]) == "Bob" and str(rows[1][1]) == "Carol"
        assert str(rows[2][0]) == "Carol" and str(rows[2][1]) == "Dave"
        assert str(rows[3][0]) == "Alice" and str(rows[3][1]) == "Dave"

    def test_copy_edge_from_csv_non_standard_columns(self):
        """Regression: CSV COPY edge with non-standard column names should
        still work via positional mapping."""
        csv_node = self.data_dir / "nodes.csv"
        csv_node.write_text("id,name\n1,Alice\n2,Bob\n3,Carol\n")

        csv_edge = self.data_dir / "edges.csv"
        csv_edge.write_text("src,dst,weight\n1,2,1.0\n2,3,2.0\n1,3,3.0\n")

        self.conn.execute(
            "CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));"
        )
        self.conn.execute(
            "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);"
        )
        self.conn.execute(f'COPY person FROM "{csv_node}" (HEADER=true);')
        self.conn.execute(
            f'COPY knows FROM "{csv_edge}" (from="person", to="person", HEADER=true);'
        )

        result = self.conn.execute(
            "MATCH ()-[k:knows]->() RETURN count(k);"
        )
        count = next(result)[0]
        assert count == 3, f"Expected 3 edges, got {count}"
