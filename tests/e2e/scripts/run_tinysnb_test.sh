#!/bin/bash
set -e
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TEST_DIR="$SCRIPT_DIR/.."
# query directory
BASE_QUERY_DIR="$TEST_DIR/queries"
# report directory
BASE_REPORT_DIR="$TEST_DIR/report/tinysnb"
# database directory
BASE_DB_DIR="${TINYSNB_DATA_DIR:-/tmp/tinysnb}"
# test markers
TEST_MARKER="neug_test"
# filtered dataset name
FILTERED_DATASET="tinysnb"

function test_tinysnb_queries_embedded {
    # Parse command-line arguments
    if [ $# -eq 0 ]; then
        # No args: use all subdirs under queries/
        SUBQUERY_DIRS=()
        for d in "$BASE_QUERY_DIR"/*/; do
            [ -d "$d" ] && SUBQUERY_DIRS+=("$(basename "$d")")
        done
    else
        # Args given: split by comma or use as list
        IFS=',' read -ra SUBQUERY_DIRS <<<"$*"
    fi

    pushd "$TEST_DIR" >/dev/null

    for SUBQUERY_DIR in "${SUBQUERY_DIRS[@]}"; do
        QUERY_DIR="$BASE_QUERY_DIR/$SUBQUERY_DIR"
        REPORT_DIR="$BASE_REPORT_DIR/$SUBQUERY_DIR"
        # if subquery involves ddl/dml operations, open database in read-write mode, and copy the dataset for testing
        if [[ "$SUBQUERY_DIR" == *"ddl"* || "$SUBQUERY_DIR" == *"dml"* ]]; then
            READ_ONLY_FLAG=false
            cp -r "$BASE_DB_DIR" "$BASE_DB_DIR"_rw
            DB_DIR="$BASE_DB_DIR"_rw
        else
            READ_ONLY_FLAG=true
            DB_DIR="$BASE_DB_DIR"
        fi
        cmd="python3 -m pytest -sv -m $TEST_MARKER --db_dir=$DB_DIR --query_dir=$QUERY_DIR --dataset $FILTERED_DATASET"
        if [ "$READ_ONLY_FLAG" = true ]; then
            cmd+=" --read_only"
        fi
        # by default, the test will generate an HTML report and Allure results
        cmd+=" --html=\"$REPORT_DIR/test_report.html\" --alluredir=\"$REPORT_DIR/allure-results\""
        printf "Running command: %s\n" "$cmd"
        eval "$cmd"
        # if rw mode was used, remove the copied database directory
        if [ "$READ_ONLY_FLAG" = false ]; then
            rm -rf "$DB_DIR"
        fi
    done

    popd >/dev/null
}

test_tinysnb_queries_embedded "$@"
