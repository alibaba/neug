#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2025 Alibaba Group Holding Limited. All Rights Reserved.
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

from utils.utils import Query
from utils.utils import ResultType


class BaseTest:
    def run_test(self, connection, query_object: Query):
        query = self.prepare_query(query_object)
        try:
            result = connection.execute(query)
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

    def run_benchmark(self, connection, query_object, request, benchmark):
        iterations = request.config.getoption("iterations")
        rounds = request.config.getoption("rounds")
        warmup_rounds = request.config.getoption("warmup_rounds")

        query = self.prepare_query(query_object)
        # Benchmark the query
        benchmark.pedantic(
            connection.execute,
            args=(query,),
            iterations=iterations,
            rounds=rounds,
            warmup_rounds=warmup_rounds,
        )

    def prepare_query(self, query_object: Query):
        """prepare the query for execution."""
        return query_object.query

    def replace_none_with_empty_string(self, record):
        """Replace None values in the record with empty strings."""
        if isinstance(record, list):
            return [self.replace_none_with_empty_string(item) for item in record]
        elif isinstance(record, dict):
            return {
                k: self.replace_none_with_empty_string(v) for k, v in record.items()
            }
        elif record is None:
            return ""
        else:
            return record

    def process_results(self, result):
        """Process the results for validation."""
        return [
            list(map(str, self.replace_none_with_empty_string(record)))
            for record in result
        ]

    def result_to_tuple(self, result):
        """Convert the result to a tuple for easier comparison."""
        # If the result contains None, replace it with ''
        result = self.replace_none_with_empty_string(result)
        return tuple(result)

    def validate_result(self, query: Query, result):
        """Validate the results against expected output."""
        result_list = self.process_results(result)
        if query.check_order:
            assert (
                result_list == query.expected_result
            ), f"Query {query.name} failed: Expected {query.expected_result}, but got {result}"
        else:
            assert set(self.result_to_tuple(r) for r in result_list) == set(
                tuple(r) for r in query.expected_result
            ), (
                f"Query {query.name} failed: Expected {set(tuple(r) for r in query.expected_result)},"
                f"but got {set(tuple(r) for r in result_list)}"
            )
