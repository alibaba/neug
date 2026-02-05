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

    def _extract_result_headers(self, schema_yaml: str):
        """
        Extracts column type information from the result schema YAML.

        The schema YAML typically looks like:
        returns:
          - name: n
            type: {primitive_type: DT_SIGNED_INT64}
          - name: $f1
            type: {graph_type: {element_opt: VERTEX, graph_data_type: [{label: {id: 0, name: person}}]}}

        This method returns a list of type dicts, e.g.:
        [
            {'primitive_type': 'DT_SIGNED_INT64'},
            {'graph_type': {'element_opt': 'VERTEX', 'graph_data_type': [{'label': {'id': 0, 'name': 'person'}}]}}
        ]
        """
        result_schema = yaml.safe_load(schema_yaml) if schema_yaml else None
        headers = []
        if "returns" in result_schema:
            headers = [col["type"] for col in result_schema["returns"]]

        return headers

    def _parse_possible_dict(self, val):
        if isinstance(val, dict):
            return val
        if isinstance(val, str):
            # if vertex is none, return an empty dict
            if val == "":
                return {}
            try:
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
            return v1 == v2

    def _dict_equal(self, d1, d2, float_tol=1e-5):
        """Compare two dictionaries for equality, allowing for float tolerance."""
        if d1.keys() != d2.keys():
            return False
        for k in d1:
            if not self._value_equal(d1[k], d2[k], float_tol):
                return False
        return True

    def _row_equal(self, row1, row2, result_headers, float_tol=1e-5):
        """
        1. Support float-tolerant row comparison. Accepts 1.0 == 1.00000, etc.
        2. Handles vertex/edge comparisons by neglecting specific fields like `_ID` (inner_id).
        """
        if len(row1) != len(row2):
            return False
        for idx, (v1, v2) in enumerate(zip(row1, row2)):
            if result_headers and idx < len(result_headers):
                col_graph_type = result_headers[idx].get("graph_type", {})
                if col_graph_type and col_graph_type.get("element_opt") == "VERTEX":
                    # Handle vertex comparison:
                    # Kuzu vertex formats like: {_ID: 1:0, _LABEL: person, id: 1, name: marko, age: 29}, while
                    # Neug vertex formats like: {_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}
                    # We only compare the relevant fields like _LABEL and other properties.
                    d1 = self._parse_possible_dict(v1)
                    d2 = self._parse_possible_dict(v2)
                    if isinstance(d1, dict) and isinstance(d2, dict):
                        d1 = self._filter_vertex_props(d1)
                        d2 = self._filter_vertex_props(d2)
                        if not self._dict_equal(d1, d2, float_tol):
                            return False
                        continue
                    else:
                        return False
                elif col_graph_type and col_graph_type.get("element_opt") == "EDGE":
                    # Handle edge comparison:
                    # Kuzu edge formats like:
                    # (0:0)-{_LABEL: knows, _ID: 3:0, weight: 0.5}->(0:1),
                    # while Neug edge formats like:
                    # {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}.
                    # We only compare the relevant fields like _LABEL and other properties.
                    # Note: Edge comparison may be imprecise since edges may lack properties as primary keys.
                    # However, given the current edge formats, it is hard to determine the source and destination vertices
                    # as they represent with inner IDs, so we simplify the edge comparison.
                    d1 = self._extract_edge_dict(v1)
                    d2 = self._extract_edge_dict(v2)
                    if isinstance(d1, dict) and isinstance(d2, dict):
                        d1 = self._filter_edge_props(d1)
                        d2 = self._filter_edge_props(d2)
                        if not self._dict_equal(d1, d2, float_tol):
                            return False
                        continue
                    else:
                        return False
                else:
                    if not self._value_equal(v1, v2, float_tol):
                        return False
            else:
                if not self._value_equal(v1, v2, float_tol):
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
        # extract the result schema first to help with row comparison
        headers = self._extract_result_headers(result.get_result_schema())
        result_list = self.process_results(result)
        expected = query.expected_result
        if query.check_order:
            assert len(result_list) == len(
                expected
            ), f"Query {query.name} failed: Expected {len(expected)} rows, but got {len(result_list)}"
            for i, (r, e) in enumerate(zip(result_list, expected)):
                assert self._row_equal(
                    r, e, headers
                ), f"Query {query.name} failed at row {i}: Expected {e}, but got {r}"
        else:
            # Unordered: match each expected to a result, and track extras
            unmatched_expected = []
            unmatched_result = result_list.copy()
            for e in expected:
                for i, r in enumerate(unmatched_result):
                    if self._row_equal(e, r, headers):
                        unmatched_result.pop(i)
                        break
                else:
                    unmatched_expected.append(e)
            assert (
                not unmatched_expected and not unmatched_result
            ), f"Query {query.name} failed: Unmatched rows: {unmatched_expected}. Extra rows: {unmatched_result}"
