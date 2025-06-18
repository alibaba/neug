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
import re
from enum import Enum


class FileType(Enum):
    TEST = 1
    CYPHER = 2
    BENCHMARK = 3


class Query:
    def __init__(self, name, query, expected_result=None, check_order=False):
        self.name = name
        self.query = query
        self.expected_result = expected_result
        self.check_order = check_order

    def __repr__(self):
        return f"Query(name={self.name}, query={self.query}, expected_result={self.expected_result}, check_order={self.check_order})"


def collect_test_files(root_dir):
    test_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            test_files.append(os.path.join(dirpath, filename))
    return test_files


# Collect query_names, queries, and expected results from files
def collect_tests_from_files(test_files, dataset=None):
    all_tests = []
    for file in test_files:
        file_suffix = os.path.splitext(file)[1]
        if file_suffix == ".test":
            tests = parse_test_file(file, FileType.TEST, dataset)
            all_tests.extend(tests)
        elif file_suffix == ".benchmark":
            tests = parse_test_file(file, FileType.BENCHMARK, dataset)
            all_tests.extend(tests)
        elif file_suffix == ".cypher":
            # .cypher files only contain queries, we could find expected results from json files
            # with the same name
            # e.g., test.cypher and test.json
            tests = parse_cypher_file(file)
            all_tests.extend(tests)
        else:
            print(f"Unsupported file type: {file_suffix}. Skipping file: {file}")
            continue
    return all_tests


def parse_test_file(file_path, file_type=FileType.TEST, dataset=None):
    DATASET = "-DATASET"
    # SKIP is used to mark the cases that we want to skip, usually because they are failed but will be fixed later.
    SKIP = "-SKIP"
    # UNSUPPORTED marks the cases that we won't support yet.
    UNSUPPORTED = "-UNSUPPORTED"
    CHECK_ORDER = "-CHECK_ORDER"
    RESULT_PREFIX = "----"
    if file_type == FileType.TEST:
        NAME = "-LOG"
        STATEMENT = "-STATEMENT"
    elif file_type == FileType.BENCHMARK:
        NAME = "-NAME"
        STATEMENT = "-QUERY"
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    with open(file_path, "r") as f:
        lines = iter(f.readlines())

    tests = []
    current_dataset = None
    current_test_name = None
    current_query = None
    expected_result = []
    check_order = False
    skip = False
    query_lines = []
    within_statement = False

    for line in lines:
        line = line.strip()

        if line.startswith((NAME, CHECK_ORDER, RESULT_PREFIX)):
            if within_statement and query_lines:
                current_query = " ".join(query_lines)
                within_statement = False
                query_lines.clear()

        if line.startswith(DATASET):
            current_dataset = line.split()[2]
            if dataset and current_dataset != dataset:
                print(f"Skip dataset: {current_dataset}")
                return []
        elif line.startswith(NAME):
            current_test_name = line.split()[1]
            check_order = False
            skip = False
        elif line.startswith(STATEMENT):
            within_statement = True
            query_lines = [line[len(STATEMENT) :].strip()]
        elif line.startswith(CHECK_ORDER):
            check_order = True
        elif line.startswith(SKIP) or line.startswith(UNSUPPORTED):
            # If the test is marked as SKIP or UNSUPPORTED, we skip it
            skip = True
        elif line.startswith(RESULT_PREFIX):
            n = line.split()[1]
            if not n.isdigit():
                # TODO: process expected results such as "ok" or "error", which usually for ddl/dml statements
                # currently, we just skip them
                print(f"Skip invalid result line: {line}")
                continue
            n = int(n)
            # Sometimes, the expected result is not available, so we need to handle it
            expected_result = []
            try:
                if n > 0:
                    expected_result = [next(lines).strip().split("|") for _ in range(n)]
            except StopIteration:
                print(f"Warning: Expected {n} result lines but file ended prematurely.")

            if skip:
                print(
                    f"Skipping test: {current_test_name} due to skip or unsupported flag."
                )
                continue
            else:
                tests.append(
                    Query(
                        name=current_test_name,
                        query=current_query,
                        expected_result=expected_result,
                        check_order=check_order,
                    )
                )
        else:
            if within_statement:
                query_lines.append(line)

    return tests


def parse_cypher_file(file_path):
    with open(file_path, "r") as f:
        queries = f.read().strip().split(";")
    file_name = os.path.basename(file_path).split(".")[0]
    # Attempt to find JSON file for expected results
    json_file_path = os.path.splitext(file_path)[0] + ".json"
    if os.path.exists(json_file_path):
        expected_result = parse_expected_results_from_json(json_file_path)
    else:
        expected_result = None
    queries = [
        Query(name=file_name, query=q.strip(), expected_result=expected_result)
        for q in queries
        if q.strip()
    ]
    return queries


def parse_expected_results_from_json(json_file_path):
    with open(json_file_path, "r") as f:
        data = json.load(f)
    # Extract the 'result'
    result_data = data.get("result", None)
    expected_results = []
    if isinstance(result_data, list):
        for item in result_data:
            if isinstance(item, dict):
                expected_results.append(tuple(map(str, item.values())))
    elif isinstance(result_data, dict):
        expected_results.append(tuple(map(str, result_data.values())))

    return expected_results


# Process queries to make them compatible with NeuG
def process_queries(queries):
    processed_queries = []
    for query in queries:
        if not skip_query(query.query):
            query.query = preprocess_query(query.query)
            processed_queries.append(query)
    return processed_queries


# Preprocess the query to make it compatible with NeuG
def preprocess_query(query):
    # e.g., Match (a:person:user) should be Match (a:person|user)
    query = re.sub(r":([a-zA-Z0-9_]+):([a-zA-Z0-9_]+)", r":\1|\2", query)
    # e.g., Match (a)-[:knows|:marries]->(b) should be Match (a)-[:knows|marries]->(b)
    query = re.sub(r":\s*([a-zA-Z0-9_]+)\s*\|:", r":\1|", query)
    query = re.sub(r"\|\s*:\s*([a-zA-Z0-9_]+\s*)", r"|\1", query)
    # e.g., id(a)<>id(b) should be a<>b, as we have not supported id() yet
    query = re.sub(
        r"[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?<> ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)",
        r"\1 <> \2",
        query,
    )
    query = re.sub(
        r"[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?>= ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)",
        r"\1 >= \2",
        query,
    )
    query = re.sub(
        r"[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?<= ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)",
        r"\1 <= \2",
        query,
    )
    query = re.sub(
        r"[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?= ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)",
        r"\1 = \2",
        query,
    )
    query = re.sub(
        r"[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?> ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)",
        r"\1 > \2",
        query,
    )
    query = re.sub(
        r"[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?< ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)",
        r"\1 < \2",
        query,
    )

    # Transform path length constraints
    def path_length_transform(match):
        if match.group(1) and match.group(2):
            # Case of "*x..y", e.g., "*1..2" to "*1..3"
            return f"*{match.group(1)}..{int(match.group(2)) + 1}"
        elif match.group(2):
            # Case of "*..y", e.g., "*..2" to "*1..3"
            return f"*1..{int(match.group(2)) + 1}"
        elif match.group(1):
            # Case of "*x..", e.g., "*1.." to "*1..10". Here we assume the max length as 10,
            # Besides, Kuzu assumes the max length as 30, and can be configured by CALL var_length_extend_max_depth=10;
            return f"*{match.group(1)}..10"
        else:
            return match.group(0)

    def single_path_length_transform(match):
        if match.group(1):
            # Case of "*x", e.g., "*2" to "*2..3"
            x = int(match.group(1))
            return f"*{x}..{x + 1}"
        else:
            return match.group(0)

    # Apply transformations for ranged constraints
    query = re.sub(r"\*(\d*)\.\.(\d*)", path_length_transform, query)
    # Apply transformations for single jump constraints
    query = re.sub(r"\*(\d+)(?!\.\.)", single_path_length_transform, query)

    return query


# Skip queries that are not supported by NeuG
def skip_query(query):
    # list of keywords to skip, as we have not supported them yet
    skip_keywords = ["CREATE", "DELETE", "SET", "CALL", "p ="]
    # check if any of the skip_keywords exist in query
    return any(keyword in query for keyword in skip_keywords)


def validate_result(query_name, result, expected_result, check_order):
    if check_order:
        assert (
            result == expected_result
        ), f"Query {query_name} failed: Expected {expected_result}, but got {result}"
    else:
        assert set(tuple(r) for r in result) == set(
            tuple(r) for r in expected_result
        ), f"Query {query_name} failed: Expected {set(tuple(r) for r in expected_result)}, but got {set(tuple(r) for r in result)}"
