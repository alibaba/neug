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

import os
import re
import pytest

def preprocess_query(query_lines):
    # omit HINT clause
    query_lines = [line for line in query_lines if not line.strip().startswith('HINT')]
    # join the lines into a single query
    query = " ".join(query_lines)
    # e.g., Match (a:person:user) should be Match (a:person|user)
    query = re.sub(r':([a-zA-Z0-9_]+):([a-zA-Z0-9_]+)', r':\1|\2', query)
    # e.g., Match (a)-[:knows|:marries]->(b) should be Match (a)-[:knows|marries]->(b)
    query = re.sub(r':\s*([a-zA-Z0-9_]+)\s*\|:', r':\1|', query)
    query = re.sub(r'\|\s*:\s*([a-zA-Z0-9_]+\s*)', r'|\1', query)
    # e.g., id(a)<>id(b) should be a<>b, as we have not supported id() yet
    query = re.sub(r'[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?<> ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)', r'\1 <> \2', query)
    query = re.sub(r'[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?>= ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)', r'\1 >= \2', query)
    query = re.sub(r'[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?<= ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)', r'\1 <= \2', query)
    query = re.sub(r'[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?= ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)', r'\1 = \2', query)
    query = re.sub(r'[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?> ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)', r'\1 > \2', query)
    query = re.sub(r'[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\) ?< ?[Ii][Dd]\(\s*([a-zA-Z0-9_]+)\s*\)', r'\1 < \2', query)
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
    query = re.sub(r'\*(\d*)\.\.(\d*)', path_length_transform, query)
    # Apply transformations for single jump constraints
    query = re.sub(r'\*(\d+)(?!\.\.)', single_path_length_transform, query)
    
    
    return query

def skip_query(query):
    # list of keywords to skip, as we have not supported them yet
    skip_keywords = ["CREATE", "DELETE", "SET", "CALL", "p ="]
    # check if any of the skip_keywords exist in query
    return any(keyword in query for keyword in skip_keywords)

def collect_test_files(root_dir):
    test_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.test'):
                test_files.append(os.path.join(dirpath, filename))
    return test_files

def collect_tests_from_files(test_files, dataset, cases):
    all_tests = []
    for file in test_files:
        tests = parse_test_file(file, dataset, cases)
        all_tests.extend(tests)
    return all_tests

def pytest_addoption(parser):
    parser.addoption("--query_dir", action="store", default="queries", help="Root directory to search for test files")
    parser.addoption("--cases", action="store", default=None, help="Comma separated case names to run")
    parser.addoption("--dataset", action="store", default=None, help="Specific dataset to run tests on")

def pytest_generate_tests(metafunc):
    query_dir = metafunc.config.getoption("query_dir")
    cases_to_run = metafunc.config.getoption("cases")
    dataset_to_run = metafunc.config.getoption("dataset")
    
    all_test_files = collect_test_files(query_dir)

    cases_to_run_set = set(cases_to_run.split(",")) if cases_to_run else None
    
    all_tests = collect_tests_from_files(all_test_files, dataset_to_run, cases_to_run_set)
    print(f"tests: {all_tests}")

    if "dataset" in metafunc.fixturenames and "case" in metafunc.fixturenames and "log" in metafunc.fixturenames and "query" in metafunc.fixturenames and "expected_output" in metafunc.fixturenames and "check_order" in metafunc.fixturenames:
        metafunc.parametrize("dataset, case, log, query, expected_output, check_order", all_tests)

# Parse test file and extract queries and expected results
def parse_test_file(file_path, dataset=None, cases=None):
    DATASET = '-DATASET'
    CASE = '-CASE'
    LOG = '-LOG'
    STATEMENT = '-STATEMENT'
    CHECK_ORDER = '-CHECK_ORDER'
    RESULT_PREFIX = '----'

    with open(file_path, 'r') as f:
        lines = iter(f.readlines())

    tests = []
    current_dataset = None
    current_case = None
    current_test_name = None
    current_query = None
    expected_result = []
    check_order = False
    query_lines = [] 
    within_statement = False  

    for line in lines:
        line = line.strip()

        # Process the statement to get the query
        if line.startswith(DATASET) or line.startswith(CASE) or line.startswith(LOG) or line.startswith(CHECK_ORDER) or line.startswith(RESULT_PREFIX):
            if within_statement and query_lines:
                current_query = preprocess_query(query_lines)
                within_statement = False  
                query_lines.clear()  

        if line.startswith(DATASET):
            current_dataset = line.split()[2]
            if dataset and current_dataset != dataset:
                print(f"Skip dataset: {current_dataset}")
                return []
        elif line.startswith(CASE):
            current_case = line.split()[1]
            if cases and current_case not in cases:
                print(f"Skip case: {current_case}")
                return []
        elif line.startswith(LOG):
            current_test_name = line.split()[1]
            check_order = False  
        elif line.startswith(STATEMENT):
            within_statement = True  
            query_lines = [line[len(STATEMENT):].strip()]
        elif line.startswith(CHECK_ORDER):
            check_order = True
        elif line.startswith(RESULT_PREFIX):
            n  = line.split()[1]
            if not n.isdigit():
                # TODO: process expected results such as "ok" or "error", which usually for ddl/dml statements
                # currently, we just skip them
                print(f"Skip invalid result line: {line}")
                continue
            n = int(n)
            if n == 0:
                expected_result = []
            else:
                expected_result = []
                for _ in range(n):
                    result_line = next(lines).strip()
                    expected_result.append(result_line.split('|'))
            # TODO: we skip several queries that are not supported by the engine
            if skip_query(current_query):
                print(f"Skip query due to unsupported: {current_query}")
                continue
            tests.append((current_dataset, current_case, current_test_name, current_query, expected_result, check_order))
        else:
            if within_statement:
                query_lines.append(line) 

    return tests
