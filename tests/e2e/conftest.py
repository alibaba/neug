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

import os
import re
import sys

import pytest

from utils.cypher_client import CypherClient
from utils.utils import collect_test_files
from utils.utils import collect_tests_from_files

sys.path.append(os.path.join(os.path.dirname(__file__), "../../tools/python_bind"))

from neug import Session

_CACHED_TESTS = {}


def pytest_addoption(parser):
    # database options
    parser.addoption(
        "--service_uri",
        action="store",
        default="bolt://localhost:7687",
        help="URI for the database service",
    )
    parser.addoption(
        "--db_dir",
        action="store",
        help="Directory for the database",
    )
    parser.addoption(
        "--read_only",
        action="store_true",
        default=False,
        help="Open the database in read-only mode or not",
    )
    # query options
    parser.addoption(
        "--query_dir",
        action="store",
        default="queries",
        help="Root directory to search for test files",
    )
    parser.addoption(
        "--dataset",
        action="store",
        default=None,
        help="Specific dataset to run tests or benchmarks on",
    )
    parser.addoption(
        "--test_names",
        action="store",
        default=None,
        help="Comma-separated list of test names to run",
    )
    parser.addoption(
        "--include_skip_tests",
        action="store_true",
        default=False,
        help="Include tests that are marked as skip to run",
    )
    # benchmark options
    parser.addoption(
        "--iterations",
        action="store",
        type=int,
        default=1,
        help="Number of iterations for each benchmark",
    )
    parser.addoption(
        "--rounds",
        action="store",
        type=int,
        default=5,
        help="Number of rounds for each benchmark",
    )
    parser.addoption(
        "--warmup_rounds",
        action="store",
        type=int,
        default=1,
        help="Number of warmup rounds for each benchmark",
    )


def _load_tests(query_dir, dataset, test_names, include_skip_tests):
    cache_key = (query_dir, dataset, test_names, include_skip_tests)
    if cache_key not in _CACHED_TESTS:
        test_files = collect_test_files(query_dir)
        all_tests = collect_tests_from_files(
            test_files, dataset, test_names, include_skip_tests
        )
        skip_keywords = ["call", "p =", "rels(", "nodes(", "unwind", "skip"]
        all_tests = [
            query
            for query in all_tests
            if not any(kw in query.query.lower() for kw in skip_keywords)
        ]
        _CACHED_TESTS[cache_key] = all_tests
    return _CACHED_TESTS[cache_key]


def pytest_generate_tests(metafunc):
    query_dir = metafunc.config.getoption("query_dir")
    dataset_to_run = metafunc.config.getoption("dataset")
    test_names = metafunc.config.getoption("test_names")
    include_skip_tests = metafunc.config.getoption("include_skip_tests")

    if "query_object" in metafunc.fixturenames:
        all_tests = _load_tests(
            query_dir, dataset_to_run, test_names, include_skip_tests
        )
        test_ids = [query.name for query in all_tests]  # use query name as test id
        metafunc.parametrize("query_object", all_tests, ids=test_ids)


@pytest.fixture(scope="module")
def neo4j_client(pytestconfig):
    service_uri = pytestconfig.getoption("service_uri")
    neo4j_user = "neo4j"
    neo4j_password = "password"
    with CypherClient(
        service_uri, neo4j_user=neo4j_user, neo4j_password=neo4j_password
    ) as client:
        yield client


@pytest.fixture(scope="module")
def neug_conn(pytestconfig):
    from neug.database import Database

    db_dir = pytestconfig.getoption("db_dir")
    read_only = pytestconfig.getoption("read_only")
    if read_only:
        db = Database(db_path=str(db_dir), mode="r")
    else:
        db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    yield conn
    conn.close()
    db.close()


@pytest.fixture(scope="module")
def neug_sess(pytestconfig):
    from neug.database import Database

    db_dir = pytestconfig.getoption("db_dir")
    read_only = pytestconfig.getoption("read_only")
    if read_only:
        db = Database(db_path=str(db_dir), mode="r")
    else:
        db = Database(db_path=str(db_dir), mode="w")
    # conn = db.connect()
    endpoint = db.serve(port=10000, host="localhost", blocking=False)
    sess = Session.open(endpoint=endpoint, timeout="60s", num_threads=5)
    yield sess
    sess.close()
    db.close()


@pytest.fixture(scope="module")
def kuzu_conn(pytestconfig):
    from pathlib import Path

    import kuzu

    db_dir = pytestconfig.getoption("db_dir")
    read_only = pytestconfig.getoption("read_only")
    num_threads = 1  # to compare with NeuG

    db_path = Path(db_dir)
    db = kuzu.Database(db_path, read_only=read_only)
    conn = kuzu.Connection(db, num_threads)

    yield conn
    conn.close()
    db.close()
