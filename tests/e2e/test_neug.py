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

import math
import re

import pytest
import yaml
from neug.query_result import QueryResult

from base_test import BaseTest
from utils.utils import Query
from utils.utils import ResultType


class TestNeug(BaseTest):
    @pytest.mark.neug_ap_test
    def test_queries(self, neug_conn, query_object):
        self.run_test(neug_conn, query_object)

    @pytest.mark.neug_tp_test
    def test_queries_tp(self, neug_sess, query_object):
        self.run_test(neug_sess, query_object)

    @pytest.mark.neug_benchmark
    def test_benchmark_queries(self, request, benchmark, neug_sess, query_object):
        self.run_benchmark(neug_sess, query_object, request, benchmark)

    def _parse_datetime_in_dict_str(self, val):
        import datetime

        def repl_dt(m):
            parts = [int(g) if g else 0 for g in m.groups()]
            y, mo, d, h, mi, s = parts
            return f"'{y:04d}-{mo:02d}-{d:02d} {h:02d}:{mi:02d}:{s:02d}'"

        val = re.sub(
            r"datetime\.datetime\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)"
            r"(?:\s*,\s*(\d+))?(?:\s*,\s*(\d+))?(?:\s*,\s*(\d+))?\s*\)",
            repl_dt,
            val,
        )

        def repl_d(m):
            y, mo, d = map(int, m.groups())
            return f"'{y:04d}-{mo:02d}-{d:02d}'"

        val = re.sub(
            r"datetime\.date\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", repl_d, val
        )
        return val

    def _parse_possible_dict(self, val):
        if isinstance(val, dict):
            return val
        if isinstance(val, str):
            # if vertex is none, return an empty dict
            if val == "":
                return {}
            try:
                val = self._parse_datetime_in_dict_str(val)
                return yaml.safe_load(val)
            except Exception:
                pass
        return val

    def _extract_edge_dict(self, val):
        d = self._parse_possible_dict(val)
        if isinstance(d, dict):
            # neug edge is a dict
            return d
        if isinstance(val, str):
            # if edge is none, return an empty dict
            # kuzu edge is a string that contains the edge part as a dict
            if val == "":
                return {}
            m = re.search(r"\{.*\}", val)
            if m:
                return self._parse_possible_dict(m.group(0))
        return val

    def _filter_vertex_props(self, d):
        # only keep _LABEL and properties for comparison
        return {k: v for k, v in d.items() if k != "_ID"}

    def _filter_edge_props(self, d):
        # only keep _LABEL and properties for comparison
        return {k: v for k, v in d.items() if k == "_LABEL" or not k.startswith("_")}

    def _value_equal(self, v1, v2, float_tol=1e-5):
        """Compare two values with float tolerance if possible, else as string."""
        try:
            f1 = float(v1)
            f2 = float(v2)
            return math.isclose(f1, f2, rel_tol=float_tol)
        except Exception:
            if v1 is None:
                v1 = ""
            if v2 is None:
                v2 = ""
            if v1 == v2:
                return True
            import datetime

            # Try datetime comparison. One may be datetime, the other string.
            try:
                v1 = str(v1)
                v2 = str(v2)
                dt1 = datetime.datetime.fromisoformat(v1)
                dt2 = datetime.datetime.fromisoformat(v2)
                return dt1 == dt2
            except Exception:
                return False

    def _dict_equal(self, d1, d2, float_tol=1e-5):
        """Compare two dictionaries for equality, allowing for float tolerance."""
        if d1.keys() != d2.keys():
            return False
        for k in d1:
            if not self._values_equal(d1[k], d2[k], float_tol):
                return False
        return True

    def _normalize_value_for_compare(self, value):
        if isinstance(value, list):
            return [self._normalize_value_for_compare(v) for v in value]
        parsed = self._parse_possible_dict(value)
        if isinstance(parsed, dict):
            if "_LABEL" in parsed:
                if "_SRC_LABEL" in parsed or "_DST_LABEL" in parsed:
                    return self._filter_edge_props(parsed)
                return self._filter_vertex_props(parsed)
            return {k: self._normalize_value_for_compare(v) for k, v in parsed.items()}
        return parsed

    def _values_equal(self, v1, v2, float_tol=1e-5):
        v1_norm = self._normalize_value_for_compare(v1)
        v2_norm = self._normalize_value_for_compare(v2)
        if isinstance(v1_norm, list) and isinstance(v2_norm, list):
            if len(v1_norm) != len(v2_norm):
                return False
            return all(
                self._values_equal(i1, i2, float_tol)
                for i1, i2 in zip(v1_norm, v2_norm)
            )
        if isinstance(v1_norm, dict) and isinstance(v2_norm, dict):
            return self._dict_equal(v1_norm, v2_norm, float_tol)
        return self._value_equal(v1_norm, v2_norm, float_tol)

    def _row_equal(self, row1, row2, float_tol=1e-5):
        """
        1. Support float-tolerant row comparison. Accepts 1.0 == 1.00000, etc.
        2. Handles vertex/edge comparisons by neglecting specific fields like `_ID` (inner_id).
        """
        if len(row1) != len(row2):
            return False
        for v1, v2 in zip(row1, row2):
            if not self._values_equal(v1, v2, float_tol):
                return False
        return True

    def run_test(self, connection, query_object: Query):
        print("Get query: " + str(query_object))
        query = query_object.query
        parameters = query_object.parameters
        try:
            result = connection.execute(query, parameters=parameters)
            if query_object.result_type == ResultType.ERROR:
                # the query is expected to fail
                assert (
                    False
                ), f"Query {query_object.name} should have failed but passed."
            elif query_object.result_type == ResultType.OK:
                # the query is expected to succeed and no validation is needed
                assert True
            elif query_object.result_type == ResultType.EXACT:
                # the query is expected to succeed and the result should be validated
                self.validate_result(query_object, result)
            else:
                raise ValueError(f"Unknown result type: {query_object.result_type}")
        except Exception as e:
            # TODO: if the query is expected to fail, we may need to further confirm the error message.
            # currently, we just check if it raises an exception.
            if query_object.result_type == ResultType.ERROR:
                assert True
            else:
                # Not expected to fail
                raise e

    def validate_result(self, query: Query, result: QueryResult):
        """Validate the results against expected output."""
        result_list = self.process_results(result)
        expected = query.expected_result
        if query.check_order:
            assert len(result_list) == len(
                expected
            ), f"Query {query.name} failed: Expected {len(expected)} rows, but got {len(result_list)}"
            for i, (r, e) in enumerate(zip(result_list, expected)):
                assert self._row_equal(
                    r, e
                ), f"Query {query.name} failed at row {i}: Expected {e}, but got {r}"
        else:
            # Unordered: match each expected to a result, and track extras
            unmatched_expected = []
            unmatched_result = result_list.copy()
            for e in expected:
                for i, r in enumerate(unmatched_result):
                    if self._row_equal(e, r):
                        unmatched_result.pop(i)
                        break
                else:
                    unmatched_expected.append(e)
            assert (
                not unmatched_expected and not unmatched_result
            ), f"Query {query.name} failed: Unmatched rows: {unmatched_expected}. Extra rows: {unmatched_result}"
