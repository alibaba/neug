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


def ensure_result_cnt_gt_zero(result):
    """
    Ensure that the result of a query contains at least one record.
    """
    if not result:
        raise ValueError("Query returned no results.")

    cnt = 0
    for record in result:
        cnt += 1
    if cnt == 0:
        raise ValueError("Query returned no records.")


def ensure_result_cnt_eq(result, expected_count):
    """
    Ensure that the result of a query contains exactly the expected number of records.
    """
    if not result:
        raise ValueError("Query returned no results.")

    cnt = 0
    for record in result:
        cnt += 1
    if cnt != expected_count:
        raise ValueError(f"Query returned {cnt} records, expected {expected_count}.")


def submit_cypher_query(conn, query, lambda_func=None):
    """
    Fixture to submit a Cypher query and return the result.
    """
    if not conn:
        raise RuntimeError("Database connection is not established.")

    if not query:
        raise ValueError("Query cannot be empty.")

    result = conn.execute(query)
    if result is None:
        raise RuntimeError("Query execution returned no result.")

    if lambda_func:
        lambda_func(result)
