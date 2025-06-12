import pytest
import os
import sys
from utils.utils import collect_test_files, collect_tests_from_files
from utils.cypher_client import CypherClient


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


def pytest_generate_tests(metafunc):
    query_dir = metafunc.config.getoption("query_dir")
    dataset_to_run = metafunc.config.getoption("dataset")

    test_files = collect_test_files(query_dir)
    all_tests = collect_tests_from_files(test_files, dataset_to_run)

    if "query_object" in metafunc.fixturenames:
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
    import nexg
    from nexg.database import Database

    db_dir = pytestconfig.getoption("db_dir")
    read_only = pytestconfig.getoption("read_only")
    if read_only:
        db = Database(db_dir, "r")
    else:
        db = Database(db_dir, "rw")
    conn = db.connect()
    yield conn
    conn.close()
    db.close()


@pytest.fixture(scope="module")
def kuzu_conn(pytestconfig):
    import kuzu
    from pathlib import Path

    db_dir = pytestconfig.getoption("db_dir")
    read_only = pytestconfig.getoption("read_only")
    num_threads = 1  # to compare with NeuG

    db_path = Path(db_dir)
    db = kuzu.Database(db_path, read_only=read_only)
    conn = kuzu.Connection(db, num_threads)

    yield conn
    conn.close()
    db.close()
