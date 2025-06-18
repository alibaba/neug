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
from utils.utils import preprocess_query


class TestNeug(BaseTest):
    @pytest.mark.neug_test
    def test_queries(self, neug_conn, query_object):
        self.run_test(neug_conn, query_object)

    @pytest.mark.neug_benchmark
    def test_benchmark_queries(self, request, benchmark, neug_conn, query_object):
        self.run_benchmark(neug_conn, query_object, request, benchmark)
