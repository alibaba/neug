#!/bin/bash
set -e

usage() {
    echo "Usage: $0 <test_mode> <dataset_name> [db_dir] [subquery_dirs] [rw_flag]"
    echo "  <test_mode>      (required) Test mode, neug_ap_test or neug_tp_test"
    echo "  <dataset_name>   (required) Dataset name, e.g. tinysnb or modern-graph"
    echo "  [db_dir]         (optional) Database directory, default: /tmp/<dataset_name>"
    echo "  [subquery_dirs]  (optional) Comma-separated list of subquery dirs, default: all"
    echo "  [rw_flag]        (optional) If set, run in read-write mode, default: false"
    echo "Example: $0 tinysnb /tmp/tinysnb basic_test,projection,filter"
    echo "Example: $0 modern-graph /tmp/modern_graph ddl true"
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

# neug_ap_test for ap test, neug_tp_test for tp test
TEST_MARKER="$1"
DATASET_NAME="$2"
DB_DIR="${3:-/tmp/${DATASET_NAME}}"
SUBQUERY_ARG="${4:-}"
READ_WRITE_FLAG="${5:-false}"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TEST_DIR="$SCRIPT_DIR/.."
BASE_QUERY_DIR="$TEST_DIR/queries"
BASE_REPORT_DIR="$TEST_DIR/report/$DATASET_NAME"


function test_queries_embedded {
    all_dirs=()
    for d in "$BASE_QUERY_DIR"/*/; do
        [[ -d "$d" ]] && all_dirs+=("$(basename "$d")")
    done
    if [ -z "$SUBQUERY_ARG" ]; then
        SUBQUERY_DIRS=("${all_dirs[@]}")
    else
        IFS=',' read -ra SUBQUERY_DIRS <<<"$SUBQUERY_ARG"
        IFS=',' read -ra parts <<< "$SUBQUERY_ARG"
        if [[ "${parts[0]}" == !* ]]; then
            excludes=()
            for item in "${parts[@]}"; do
            if [[ "$item" == !* ]]; then
                excludes+=("${item#!}")
            else
                echo "Warning: Non-exclusion items should not appear in exclusion mode. '$item'" >&2
            fi
        done
        for dir in "${all_dirs[@]}"; do
            skip=false
            for excl in "${excludes[@]}"; do
                if [[ "$dir" == "$excl" ]]; then
                    skip=true
                    break
                fi
            done
            if ! $skip; then
                SUBQUERY_DIRS+=("$dir")
            fi
        done
        else
          SUBQUERY_DIRS=("${parts[@]}")
        fi
    fi

    pushd "$TEST_DIR" >/dev/null

    for SUBQUERY_DIR in "${SUBQUERY_DIRS[@]}"; do
        QUERY_DIR="$BASE_QUERY_DIR/$SUBQUERY_DIR"
        REPORT_DIR="$BASE_REPORT_DIR/$SUBQUERY_DIR"
        cmd="python3 -m pytest -sv -m $TEST_MARKER --query_dir=$QUERY_DIR --dataset $DATASET_NAME"
        if [[ "$READ_WRITE_FLAG" == "true" ]]; then
            # If read-write mode is requested, copy the database directory
            if [ -d "${DB_DIR}_rw" ]; then
                rm -rf "${DB_DIR}_rw"
            fi
            cp -r "$DB_DIR" "${DB_DIR}_rw"
            cmd+=" --db_dir=${DB_DIR}_rw"
        else
            cmd+=" --db_dir=$DB_DIR --read_only"
        fi
        cmd+=" --html=\"$REPORT_DIR/test_report.html\" --alluredir=\"$REPORT_DIR/allure-results\""
        printf "Running command: %s\n" "$cmd"
        eval "$cmd"
    done

    popd >/dev/null
}

test_queries_embedded