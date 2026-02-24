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

import pytest

from base_test import BaseTest


class TestKuzu(BaseTest):

    def process_results(self, result):
        result_str = []
        while result.has_next():
            record = result.get_next()
            result_str.append(list(map(str, record)))
        return result_str

    @pytest.mark.kuzu_test
    def test_queries(self, kuzu_conn, query_object):
        self.run_test(kuzu_conn, query_object)

    @pytest.mark.kuzu_benchmark
    def test_benchmark_queries(self, request, benchmark, kuzu_conn, query_object):
        self.run_benchmark(kuzu_conn, query_object, request, benchmark)
