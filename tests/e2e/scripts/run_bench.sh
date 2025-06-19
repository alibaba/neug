#!/bin/bash
set -e
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)

# benchmark markers
CYTHER_BENCHMARK_MARKER="cypher_benchmark"
KUZU_BENCHMARK_MARKER="kuzu_benchmark"
NEUG_BENCHMARK_MARKER="neug_benchmark"


function start_services {
    local db_dir=$1
    local graph_schema="$db_dir/graph.yaml"
    local interactive_config="$ROOT_DIR/tests/engines/interactive_config_test_cbo.yaml"
    local statistics_json="$db_dir/statistics.json"

    echo "Starting interactive server and compiler for dataset: $db_dir"
    
    # start interactive server
    GLOG_minloglevel=1 $ROOT_DIR/build/src/bin/interactive_server -c $interactive_config -g $graph_schema --data-path $db_dir &
    # start compiler
    java -Dgraph.schema=$graph_schema -Dgraph.statistics=$statistics_json -jar $ROOT_DIR/compiler-0.0.1-SNAPSHOT-shade.jar $interactive_config &

    sleep 10  # wait for the interactive server to start

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
    sleep 5
}

function bench_ldbc_queries {
    DB_DIR="/tmp/csr-data-ldbc"
    QUERY_DIR="benchmark_queries/neug-ldbc-sf01"
    
    # start ldbc services
    start_services "$DB_DIR"
    # run ldbc queries
    cmd="python3 -m pytest -m $CYTHER_BENCHMARK_MARKER --query_dir=$QUERY_DIR --benchmark-save=benchmark-ldbc-neug.json"
    eval "$cmd"
    # stop ldbc services
    stop_services

    # TODO: benchmark via embedded db
    # eval "$cmd --db_dir=$DB_DIR"
}

function bench_ldbc_queries_kuzu {
    DB_DIR="/tmp/kuzu-data-ldbc"
    QUERY_DIR="benchmark_queries/kuzu-ldbc-sf01"
    
    # run kuzu queries
    cmd="python3 -m pytest -m $KUZU_BENCHMARK_MARKER --db_dir=$DB_DIR --query_dir=$QUERY_DIR --benchmark-save=benchmark-ldbc-kuzu.json"
    eval "$cmd"
}

# TODO: currently, building lsqb dataset on neug occurs error
function bench_lsqb_queries {
    DB_DIR="/tmp/csr-data-lsqb"
    QUERY_DIR="queries/lsqb"
    REPORT_DIR="$REPORT_BASE_DIR/lsqb"

    # start lsqb services
    start_services "$DB_DIR"
    # run lsqb queries
    cmd="python3 -m pytest -m $CYTHER_BENCHMARK_MARKER --query_dir=$QUERY_DIR --benchmark-save=benchmark-lsqb-neug.json"
    eval "$cmd"
    # stop lsqb services
    stop_services

    # TODO: test via embedded db
    # eval "$cmd --db_dir=$DB_DIR"
}

function bench_lsqb_queries_kuzu {
    DB_DIR="/tmp/kuzu-data-lsqb"
    QUERY_DIR="queries/lsqb"
    
    # run kuzu queries
    cmd="python3 -m pytest -m $KUZU_BENCHMARK_MARKER --db_dir=$DB_DIR --query_dir=$QUERY_DIR --benchmark-save=benchmark-lsqb-kuzu.json"
    eval "$cmd"
}

# Test queries
bench_ldbc_queries
bench_ldbc_queries_kuzu
