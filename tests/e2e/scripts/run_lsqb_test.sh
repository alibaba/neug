#!/bin/bash
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TEST_DIR="$SCRIPT_DIR/.."
# query directory
QUERY_DIR="$TEST_DIR/queries/lsqb"
# report directory
REPORT_DIR="$TEST_DIR/report/lsqb"
# database directory
DB_DIR="${LSQB_DATA_DIR:-/tmp/lsqb}"
# test markers
TEST_MARKER="neug_test"
# filtered dataset name
FILTERED_DATASET="lsqb-sf01"

function test_lsqb_queries_embedded {

    pushd "$TEST_DIR" >/dev/null

    # run lsqb queries
    cmd="python3 -m pytest -m $TEST_MARKER --db_dir=$DB_DIR --query_dir=$QUERY_DIR --dataset $FILTERED_DATASET --read_only --html=\"$REPORT_DIR/test_report.html\" --alluredir=\"$REPORT_DIR/allure-results\""
    printf "Running command: %s\n" "$cmd"
    eval "$cmd"

    popd >/dev/null
}

test_lsqb_queries_embedded
