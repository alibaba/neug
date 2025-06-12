#!/bin/bash

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)

# test markers
CYTHER_TEST_MARKER="cypher_test"
NEUG_TEST_MARKER="neug_test"

REPORT_BASE_DIR="$SCRIPT_DIR/report"

function test_lsqb_queries_embedded {
    DB_DIR="/tmp/csr-data-lsqb"
    QUERY_DIR="queries/lsqb"
    FILTERED_DATASET="lsqb-sf01"
    REPORT_DIR="$REPORT_BASE_DIR/lsqb"

    # run lsqb queries
    cmd="python3 -m pytest -m $NEUG_TEST_MARKER --db_dir=$DB_DIR --query_dir=$QUERY_DIR --dataset $FILTERED_DATASET --read_only --html=\"$REPORT_DIR/test_report.html\" --alluredir=\"$REPORT_DIR/allure-results\""
    printf "Running command: %s\n" "$cmd"
    eval "$cmd"
}

function test_tinysnb_queries_embedded {
    DB_DIR="/tmp/csr-data-tinysnb"
    QUERY_DIR="queries/"
    FILTERED_DATASET="tinysnb"
    REPORT_DIR="$REPORT_BASE_DIR/tinysnb"

    # run tinysnb queries
    cmd="python3 -m pytest -m $NEUG_TEST_MARKER --db_dir=$DB_DIR --query_dir=$QUERY_DIR --dataset $FILTERED_DATASET --read_only --html=\"$REPORT_DIR/test_report.html\" --alluredir=\"$REPORT_DIR/allure-results\""
    printf "Running command: %s\n" "$cmd"
    eval "$cmd"
}

# test_tinysnb_queries_embedded
test_lsqb_queries_embedded
