#!/bin/bash
set -e
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)

# test markers
CYTHER_TEST_MARKER="cypher_test"
NEUG_TEST_MARKER="neug_test"

REPORT_BASE_DIR="$SCRIPT_DIR/report"


function start_services {
    local db_dir=$1
    local graph_schema=$2
    local interactive_config="$ROOT_DIR/tests/engines/interactive_config_test_cbo.yaml"
    local statistics_json="$db_dir/statistics.json"

    echo "Starting interactive server and compiler for dataset: $db_dir"
    
    # start interactive server
    GLOG_minloglevel=1 $ROOT_DIR/build/src/bin/interactive_server -c $interactive_config -g $graph_schema --data-path $db_dir &
    # start compiler
    # TODO: a tricky for tinysnb, as the some types in graph.yaml are not supported by the compiler
    # so we need to use the graph-modify.yaml
    if [[ "$db_dir" == *tinysnb* ]]; then
        graph_schema="$ROOT_DIR/example_dataset/tinysnb/graph.yaml"
    fi
    java -Dgraph.schema=$graph_schema -Dgraph.statistics=$statistics_json -jar $ROOT_DIR/compiler-0.0.1-SNAPSHOT-shade.jar $interactive_config &
    sleep 10  # wait for the compiler to start

    echo "Services started for dataset: $db_dir"
}

function run_queries {
    local db_dir=$1
    local query_subdir=$2
    local dataset=$3
    local test_marker=$4

    
    cd $SCRIPT_DIR || exit
    echo "Running queries for subdir: $query_subdir"

    # Add base command with common options
    local base_command="python3 -m pytest -m \"$test_marker\" --query_dir \"$query_subdir\""

    # If db_dir is provided, include it
    if [[ -n "$db_dir" ]]; then
        base_command="$base_command --db_dir \"$db_dir\""
    fi

    if [[ -n "$dataset" ]]; then
        base_command="$base_command --dataset \"$dataset\""
    fi

    # Run the appropriate test or benchmark based on the marker
    eval "$base_command --html=\"${test_marker}_on_${query_subdir}.html\""
    
    echo "Queries completed for subdir: $query_subdir"
}

function stop_services {
    echo "Stopping interactive server and compiler"
    kill $(ps aux | grep '[i]nteractive_server' | awk '{print $2}')
    kill $(ps aux | grep '[c]ompiler-0.0.1-SNAPSHOT-shade.jar' | awk '{print $2}')
    echo "Services stopped"
    sleep 10
}

trap stop_services EXIT    # Ensure stop_services is called on script exit
trap stop_services INT     # Ensure stop_services is called when receiving Ctrl+C (SIGINT)
trap stop_services TERM    # Ensure stop_services is called when receiving SIGTERM

function test_ldbc_queries {
    DB_DIR="/tmp/csr-data-ldbc"
    SCHEMA_FILE="$ROOT_DIR/example_dataset/ldbc/graph.yaml"
    QUERY_DIR="benchmark_queries/neug-ldbc-sf01"
    REPORT_DIR="$REPORT_BASE_DIR/ldbc"

    # start ldbc services
    start_services "$DB_DIR" "$SCHEMA_FILE"
    # run ldbc queries
    cmd="python3 -m pytest -m $CYTHER_TEST_MARKER --query_dir=$QUERY_DIR --html=\"$REPORT_DIR/test_report.html\" --alluredir=\"$REPORT_DIR/allure-results\""
    eval "$cmd"
    # stop ldbc services
    stop_services
}

function test_ldbc_queries_kuzu {
    DB_DIR="/tmp/kuzu-data-ldbc"
    QUERY_DIR="benchmark_queries/kuzu-ldbc-sf01"
    REPORT_DIR="$REPORT_BASE_DIR/ldbc-kuzu"

    # run ldbc queries
    cmd="python3 -m pytest -m kuzu_test --db_dir=$DB_DIR --query_dir=$QUERY_DIR --html=\"$REPORT_DIR/test_report.html\" --alluredir=\"$REPORT_DIR/allure-results\""
    eval "$cmd"
}

function test_lsqb_queries {
    DB_DIR="/tmp/csr-data-lsqb"
    SCHEMA_FILE="$ROOT_DIR/example_dataset/lsqb-sf01/graph.yaml"
    QUERY_DIR="queries/lsqb"
    FILTERED_DATASET="lsqb-sf01"
    REPORT_DIR="$REPORT_BASE_DIR/lsqb"

    # start lsqb services
    start_services "$DB_DIR" "$SCHEMA_FILE"
    # run lsqb queries
    cmd="python3 -m pytest -m $CYTHER_TEST_MARKER --query_dir=$QUERY_DIR --dataset $FILTERED_DATASET --html=\"$REPORT_DIR/test_report.html\" --alluredir=\"$REPORT_DIR/allure-results\""
    eval "$cmd"
    # stop lsqb services
    stop_services
}

function test_tinysnb_queries {
    DB_DIR="/tmp/csr-data-tinysnb"
    SCHEMA_FILE="$ROOT_DIR/example_dataset/tinysnb/graph-modify.yaml"
    QUERY_DIR="queries/"
    FILTERED_DATASET="tinysnb"
    REPORT_DIR="$REPORT_BASE_DIR/tinysnb"

    # start tinysnb services
    start_services "$DB_DIR" "$SCHEMA_FILE"
    # run tinysnb queries
    cmd="python3 -m pytest -m $CYTHER_TEST_MARKER --query_dir=$QUERY_DIR --dataset $FILTERED_DATASET --html=\"$REPORT_DIR/test_report.html\" --alluredir=\"$REPORT_DIR/allure-results\""
    eval "$cmd"
    # stop tinysnb services
    stop_services
}


# Test queries
# test_ldbc_queries
# test_tinysnb_queries
test_lsqb_queries
