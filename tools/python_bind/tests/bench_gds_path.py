#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Benchmark: BFS/SSSP with and without path return.
# Runs against the LDBC Graphalytics validation datasets and compares:
#   1. Correctness (distance values must match reference output)
#   2. Performance (wall-clock time with vs. without path YIELDed)
#
# Usage:
#   python3 bench_gds_path.py [--rounds N]

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from neug.database import Database

RESOURCE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "resources", "graphalytics"
)

LONG_MAX = 9223372036854775807
FLOAT_EPSILON = 1e-4

BFS_SSSP_DATASETS = [
    "test-bfs-directed",
    "test-bfs-undirected",
    "test-sssp-directed",
    "test-sssp-undirected",
    "example-directed",
    "example-undirected",
]


def _parse_properties(path, name):
    props = {}
    prefix = "graph.{}.".format(name)
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip()
            if key.startswith(prefix):
                props[key[len(prefix) :]] = value
    return props


def _read_vertices(path):
    ids = []
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if line:
                ids.append(int(line.split()[0]))
    return ids


def _read_edges(path):
    edges = []
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            src, dst = int(parts[0]), int(parts[1])
            weight = float(parts[2]) if len(parts) >= 3 else 1.0
            edges.append((src, dst, weight))
    return edges


def _read_reference(path):
    expected = {}
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            expected[int(parts[0])] = parts[1]
    return expected


def _load_graph(conn, vertices, edges):
    conn.execute("CREATE NODE TABLE v(id INT64 PRIMARY KEY);")
    conn.execute("CREATE REL TABLE e(FROM v TO v, weight DOUBLE);")
    for vid in vertices:
        conn.execute("CREATE (:v {{id: {}}});".format(vid))
    for src, dst, weight in edges:
        conn.execute(
            "MATCH (a:v), (b:v) WHERE a.id = {} AND b.id = {} "
            "CREATE (a)-[:e {{weight: {!r}}}]->(b);".format(src, dst, weight)
        )
    conn.execute("CALL project_graph('g', ['v'], {'[v, e, v]': ''});")
    conn.execute("LOAD gds;")


def _floats_close(a, b, eps=FLOAT_EPSILON):
    diff = abs(a - b)
    if diff <= eps:
        return True
    return b != 0.0 and diff / abs(b) <= eps


# ---- BFS queries ----

BFS_NO_PATH = (
    "CALL bfs('g', {{source: '{src}', directed: {directed}}}) "
    "YIELD node, distance RETURN node.id, distance;"
)
BFS_WITH_PATH = (
    "CALL bfs('g', {{source: '{src}', directed: {directed}}}) "
    "YIELD node, distance, path RETURN node.id, distance, path;"
)

# ---- SSSP queries ----

SSSP_NO_PATH = (
    "CALL sssp('g', {{source: '{src}', weight: '{weight}', directed: {directed}}}) "
    "YIELD node, distance RETURN node.id, distance;"
)
SSSP_WITH_PATH = (
    "CALL sssp('g', {{source: '{src}', weight: '{weight}', directed: {directed}}}) "
    "YIELD node, distance, path RETURN node.id, distance, path;"
)


def _time_query(conn, query, rounds):
    """Run a query `rounds` times and return (avg_seconds, rows_of_last_run)."""
    rows = None
    times = []
    for _ in range(rounds):
        t0 = time.perf_counter()
        rows = list(conn.execute(query))
        t1 = time.perf_counter()
        times.append(t1 - t0)
    avg = sum(times) / len(times)
    return avg, rows


def _validate_bfs(rows, expected, has_path):
    actual = {}
    for r in rows:
        actual[int(r[0])] = int(r[1])
    errors = 0
    for vid, raw in expected.items():
        exp = int(raw)
        got = actual.get(vid)
        if exp >= LONG_MAX:
            if got is not None and 0 <= got < LONG_MAX:
                errors += 1
        else:
            if got != exp:
                errors += 1
    return errors


def _validate_sssp(rows, expected, has_path):
    actual = {}
    for r in rows:
        actual[int(r[0])] = float(r[1])
    errors = 0
    for vid, raw in expected.items():
        got = actual.get(vid)
        if raw.lower() == "infinity":
            if got is not None and got >= 0:
                errors += 1
        else:
            exp = float(raw)
            if got is None or got < 0 or not _floats_close(got, exp):
                errors += 1
    return errors


def _check_path_column(rows, algo):
    """Verify path column is present and well-formed."""
    issues = 0
    for r in rows:
        dist = r[1]
        path = r[2]
        if algo == "bfs":
            if dist >= 0 and path is None:
                issues += 1
            if dist < 0 and path is not None:
                issues += 1
        else:  # sssp
            if isinstance(dist, float) and dist >= 0 and path is None:
                issues += 1
            if isinstance(dist, float) and dist < 0 and path is not None:
                issues += 1
    return issues


def run_benchmark(rounds, tmp_dir):
    print("=" * 78)
    print("GDS Path Return Benchmark: BFS & SSSP")
    print("  Rounds per query: {}".format(rounds))
    print(
        "  Datasets: {} ({})".format(
            len(BFS_SSSP_DATASETS), ", ".join(BFS_SSSP_DATASETS)
        )
    )
    print("=" * 78)
    print()

    summary_rows = []

    for ds_name in BFS_SSSP_DATASETS:
        ds_dir = os.path.join(RESOURCE_DIR, ds_name)
        if not os.path.isdir(ds_dir):
            print("SKIP: {} (directory not found)".format(ds_name))
            continue

        is_bfs = "bfs" in ds_name or ds_name.startswith("example")
        is_sssp = "sssp" in ds_name or ds_name.startswith("example")

        algos_to_run = []
        if is_bfs:
            algos_to_run.append("bfs")
        if is_sssp:
            algos_to_run.append("sssp")

        props_file = os.path.join(ds_dir, ds_name + ".properties")
        if not os.path.exists(props_file):
            print("SKIP: {} (no properties file)".format(ds_name))
            continue

        # Load dataset
        vertices = _read_vertices(os.path.join(ds_dir, ds_name + ".v"))
        edges = _read_edges(os.path.join(ds_dir, ds_name + ".e"))
        n_vertices = len(vertices)
        n_edges = len(edges)

        db_path = os.path.join(tmp_dir, ds_name)
        db = Database(db_path=db_path, mode="w")
        conn = db.connect()
        try:
            _load_graph(conn, vertices, edges)

            # Parse properties using dataset name as prefix
            # (format: graph.<dataset-name>.key = value)
            props = _parse_properties(props_file, ds_name)
            directed = props.get("directed", "false").lower() == "true"

            for algo in algos_to_run:
                source = props.get("{}.source-vertex".format(algo), "1")

                ref_file = os.path.join(
                    ds_dir, ds_name + "-" + ("BFS" if algo == "bfs" else "SSSP")
                )
                if not os.path.exists(ref_file):
                    continue
                expected = _read_reference(ref_file)

                if algo == "bfs":
                    q_no = BFS_NO_PATH.format(
                        src=source, directed=str(directed).lower()
                    )
                    q_yes = BFS_WITH_PATH.format(
                        src=source, directed=str(directed).lower()
                    )
                else:
                    weight = props.get("sssp.weight-property", "weight")
                    q_no = SSSP_NO_PATH.format(
                        src=source,
                        weight=weight,
                        directed=str(directed).lower(),
                    )
                    q_yes = SSSP_WITH_PATH.format(
                        src=source,
                        weight=weight,
                        directed=str(directed).lower(),
                    )

                # --- Run without path ---
                t_no, rows_no = _time_query(conn, q_no, rounds)

                # --- Run with path ---
                t_yes, rows_yes = _time_query(conn, q_yes, rounds)

                # --- Validate ---
                if algo == "bfs":
                    err_no = _validate_bfs(rows_no, expected, False)
                    err_yes = _validate_bfs(rows_yes, expected, True)
                else:
                    err_no = _validate_sssp(rows_no, expected, False)
                    err_yes = _validate_sssp(rows_yes, expected, True)

                path_issues = _check_path_column(rows_yes, algo)

                overhead_pct = ((t_yes - t_no) / t_no * 100) if t_no > 0 else 0.0

                status_no = "PASS" if err_no == 0 else "FAIL({})".format(err_no)
                status_yes = "PASS" if err_yes == 0 else "FAIL({})".format(err_yes)
                status_path = (
                    "OK" if path_issues == 0 else "ISSUES({})".format(path_issues)
                )

                row_summary = (
                    "{:<28s} {:>4s}  V={:<4d} E={:<5d}  "
                    "no_path={:.4f}s [{:>4s}]  "
                    "with_path={:.4f}s [{:>4s}] path_col={:>4s}  "
                    "overhead={:+.1f}%"
                ).format(
                    ds_name,
                    algo.upper(),
                    n_vertices,
                    n_edges,
                    t_no,
                    status_no,
                    t_yes,
                    status_yes,
                    status_path,
                    overhead_pct,
                )
                print(row_summary)
                summary_rows.append(row_summary)

        finally:
            conn.close()
            db.close()

    print()
    print("=" * 78)
    print("Summary")
    print("=" * 78)
    for row in summary_rows:
        print(row)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rounds", type=int, default=5, help="Number of rounds per query (default: 5)"
    )
    parser.add_argument(
        "--tmp-dir",
        type=str,
        default="/tmp/neug_gds_bench",
        help="Temporary directory for databases",
    )
    args = parser.parse_args()

    import shutil

    if os.path.exists(args.tmp_dir):
        shutil.rmtree(args.tmp_dir)
    os.makedirs(args.tmp_dir, exist_ok=True)

    run_benchmark(args.rounds, args.tmp_dir)

    shutil.rmtree(args.tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
