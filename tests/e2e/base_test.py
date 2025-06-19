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
from utils.utils import validate_result


class BaseTest:
    def run_test(self, connection, query_object: Query):
        query_name = query_object.name
        query = self.prepare_query(query_object)
        expected_output = query_object.expected_result
        check_order = query_object.check_order
        result = connection.execute(query)
        result_str = self.process_results(result)
        validate_result(query_name, result_str, expected_output, check_order)

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

    def prepare_query(self, query_object):
        """prepare the query for execution."""
        return query_object.query

    def process_results(self, result):
        """Process the results for validation."""
        return [list(map(str, record)) for record in result]
