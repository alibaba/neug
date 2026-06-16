#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LDBC Graphalytics Benchmark for NeuG GDS Extension

Supports: graph500-23, datagen-8_0-fb
Algorithms: WCC, BFS, PageRank, CDLP, LCC, Leiden, Louvain

Usage:
  python3 run_gds_benchmark.py [--dataset graph500-23|datagen-8_0-fb] [--skip-lcc]
"""

import argparse
import os
import shutil
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../tools/python_bind"))

from neug.database import Database

BENCHMARK_DIR = os.path.expanduser("~/Documents/git/neug/benchmark")

DATASETS = {
    "graph500-23": {
        "vertices": 4610222,
        "edges": 129333677,
        "bfs_source": "7348998",
        "pr_iters": 10,
        "cdlp_iters": 10,
    },
    "datagen-8_0-fb": {
        "vertices": 1706561,
        "edges": 107507376,
        "bfs_source": "6",
        "pr_iters": 10,
        "cdlp_iters": 10,
    },
}

GRAPH_NAME = "benchmark_graph"
DB_PATH_PREFIX = "/tmp/gds_benchmark_"


def load_data(dataset_name, cfg):
    """Load Graphalytics dataset into NeuG."""
    print("=" * 70)
    print(f"Loading Dataset: {dataset_name}")
    print(f"  Vertices: {cfg['vertices']:,}  Edges: {cfg['edges']:,}")
    print("=" * 70)

    db_path = DB_PATH_PREFIX + dataset_name.replace("-", "_")
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    db = Database(db_path, "w")
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Node (id INT64 PRIMARY KEY);")
    conn.execute("CREATE REL TABLE Edge (FROM Node TO Node);")

    # Load vertices
    print("Loading vertices...")
    t0 = time.time()
    vertex_file = os.path.join(BENCHMARK_DIR, f"{dataset_name}.v")
    vertex_csv = f"/tmp/{dataset_name}_vertices.csv"
    with open(vertex_file, "r") as f_in, open(vertex_csv, "w") as f_out:
        f_out.write("id\n")
        for line in f_in:
            vid = line.strip()
            if vid:
                f_out.write(f"{vid}\n")
    conn.execute(f"COPY Node FROM '{vertex_csv}' (header=true);")
    v_time = time.time() - t0
    print(f"  Vertices loaded in {v_time:.2f}s")

    # Load edges
    print("Loading edges...")
    t0 = time.time()
    edge_file = os.path.join(BENCHMARK_DIR, f"{dataset_name}.e")
    edge_csv = f"/tmp/{dataset_name}_edges.csv"
    with open(edge_file, "r") as f_in, open(edge_csv, "w") as f_out:
        for line in f_in:
            parts = line.strip().split()
            if len(parts) >= 2:
                f_out.write(f"{parts[0]},{parts[1]}\n")
    conn.execute(
        f"COPY Edge FROM '{edge_csv}' (header=false, delimiter=',');"
    )
    e_time = time.time() - t0
    print(f"  Edges loaded in {e_time:.2f}s")

    # Verify
    v_count = list(conn.execute("MATCH (n) RETURN count(n);"))[0][0]
    e_count = list(conn.execute("MATCH ()-[e]->() RETURN count(e);"))[0][0]
    print(f"\n  Verified: {v_count:,} vertices, {e_count:,} edges")

    # Create projected graph
    conn.execute("LOAD gds;")
    conn.execute(
        f"CALL project_graph('{GRAPH_NAME}', ['Node'], "
        f"{{'[Node, Edge, Node]': ''}});"
    )
    print(f"  Projected graph '{GRAPH_NAME}' created")

    conn.close()
    db.close()

    for f in [vertex_csv, edge_csv]:
        if os.path.exists(f):
            os.remove(f)

    return v_count, e_count, db_path


def load_reference(dataset_name, algo):
    """Load reference output for comparison."""
    ref_file = os.path.join(BENCHMARK_DIR, f"{dataset_name}-{algo}")
    if not os.path.exists(ref_file):
        return None
    result = {}
    with open(ref_file, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2:
                result[int(parts[0])] = parts[1]
    return result


def compare(ours, ref, algo, tol=1e-6):
    """Compare our results with reference, return accuracy."""
    if ref is None:
        return -1.0
    total = len(ref)
    correct = 0
    for vid, ref_val in ref.items():
        if vid not in ours:
            continue
        try:
            if algo in ["BFS", "WCC", "CDLP"]:
                if int(ours[vid]) == int(float(ref_val)):
                    correct += 1
            else:
                if abs(float(ours[vid]) - float(ref_val)) < tol:
                    correct += 1
        except (ValueError, TypeError):
            pass
    return correct / total if total > 0 else 0.0


def open_db(db_path):
    """Open a DB connection with GDS loaded and projected graph created."""
    db = Database(db_path, "w")
    conn = db.connect()
    conn.execute("LOAD gds;")
    conn.execute(
        f"CALL project_graph('{GRAPH_NAME}', ['Node'], "
        f"{{'[Node, Edge, Node]': ''}});"
    )
    return db, conn


def run_algo(conn, query, algo_name):
    """Run a single algorithm and time it."""
    print(f"\n{'─' * 50}")
    print(f"  {algo_name}")
    print(f"{'─' * 50}")
    t0 = time.time()
    result = conn.execute(query)
    rows = list(result)
    elapsed = time.time() - t0
    print(f"    Time:    {elapsed:.3f}s")
    print(f"    Rows:    {len(rows):,}")
    return rows, elapsed


def run_benchmarks(db_path, dataset_name, cfg, skip_lcc=False):
    """Run all GDS algorithm benchmarks."""
    num_edges = cfg["edges"]
    results = {}

    # ── WCC ──
    db, conn = open_db(db_path)
    rows, t = run_algo(
        conn,
        f"CALL wcc('{GRAPH_NAME}', {{concurrency: 4}}) YIELD node, comp "
        f"RETURN node.id, comp;",
        "WCC",
    )
    ours = {int(r[0]): int(r[1]) for r in rows}
    ref = load_reference(dataset_name, "WCC")
    acc = compare(ours, ref, "WCC")
    comps = defaultdict(int)
    for c in ours.values():
        comps[c] += 1
    print(f"    Accuracy: {acc:.2%}" if acc >= 0 else "    Accuracy: N/A (no ref)")
    print(f"    Components: {len(comps)}")
    results["WCC"] = (t, acc, len(rows), num_edges / t)
    conn.close()
    db.close()

    # ── BFS ──
    db, conn = open_db(db_path)
    rows, t = run_algo(
        conn,
        f"CALL bfs('{GRAPH_NAME}', {{source: '{cfg['bfs_source']}'}}) "
        f"YIELD node, distance RETURN node.id, distance;",
        f"BFS (source={cfg['bfs_source']})",
    )
    ours = {int(r[0]): int(r[1]) for r in rows}
    ref = load_reference(dataset_name, "BFS")
    acc = compare(ours, ref, "BFS")
    dists = defaultdict(int)
    for d in ours.values():
        dists[d] += 1
    print(f"    Accuracy: {acc:.2%}" if acc >= 0 else "    Accuracy: N/A (no ref)")
    print(f"    Max distance: {max(dists.keys()) if dists else 'N/A'}")
    results["BFS"] = (t, acc, len(rows), num_edges / t)
    conn.close()
    db.close()

    # ── PageRank ──
    db, conn = open_db(db_path)
    rows, t = run_algo(
        conn,
        f"CALL page_rank('{GRAPH_NAME}', "
        f"{{max_iterations: {cfg['pr_iters']}}}) "
        f"YIELD node, rank RETURN node.id, rank;",
        f"PageRank (iters={cfg['pr_iters']})",
    )
    ours = {int(r[0]): float(r[1]) for r in rows}
    ref = load_reference(dataset_name, "PR")
    acc = compare(ours, ref, "PR", tol=1e-10)
    top5 = sorted(ours.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"    Accuracy: {acc:.2%}" if acc >= 0 else "    Accuracy: N/A (no ref)")
    print(f"    Top-5:")
    for vid, rank in top5:
        print(f"      {vid}: {rank:.6e}")
    results["PageRank"] = (t, acc, len(rows), num_edges / t)
    conn.close()
    db.close()

    # ── CDLP (Label Propagation) ──
    db, conn = open_db(db_path)
    rows, t = run_algo(
        conn,
        f"CALL label_propagation('{GRAPH_NAME}', "
        f"{{max_iterations: {cfg['cdlp_iters']}}}) "
        f"YIELD node, label RETURN node.id, label;",
        f"CDLP/LabelProp (iters={cfg['cdlp_iters']})",
    )
    ours = {int(r[0]): int(r[1]) for r in rows}
    ref = load_reference(dataset_name, "CDLP")
    acc = compare(ours, ref, "CDLP")
    comms = defaultdict(int)
    for c in ours.values():
        comms[c] += 1
    print(f"    Accuracy: {acc:.2%}" if acc >= 0 else "    Accuracy: N/A (no ref)")
    print(f"    Communities: {len(comms)}")
    results["CDLP"] = (t, acc, len(rows), num_edges / t)
    conn.close()
    db.close()

    # ── LCC ──
    if not skip_lcc:
        db, conn = open_db(db_path)
        rows, t = run_algo(
            conn,
            f"CALL lcc('{GRAPH_NAME}', {{concurrency: 4}}) YIELD node, lcc "
            f"RETURN node.id, lcc;",
            "LCC",
        )
        ours = {int(r[0]): float(r[1]) for r in rows}
        ref = load_reference(dataset_name, "LCC")
        acc = compare(ours, ref, "LCC")
        avg_lcc = sum(ours.values()) / len(ours) if ours else 0
        print(f"    Accuracy: {acc:.2%}" if acc >= 0 else "    Accuracy: N/A (no ref)")
        print(f"    Avg LCC: {avg_lcc:.6f}")
        results["LCC"] = (t, acc, len(rows), num_edges / t)
        conn.close()
        db.close()

    # ── Leiden (no reference output) ──
    db, conn = open_db(db_path)
    rows, t = run_algo(
        conn,
        f"CALL leiden('{GRAPH_NAME}', {{resolution: 1.0}}) "
        f"YIELD node, community RETURN node.id, community;",
        "Leiden (resolution=1.0)",
    )
    ours = {int(r[0]): int(r[1]) for r in rows}
    comms = defaultdict(int)
    for c in ours.values():
        comms[c] += 1
    print(f"    Accuracy: N/A (no LDBC ref for community detection)")
    print(f"    Communities: {len(comms)}")
    results["Leiden"] = (t, -1.0, len(rows), num_edges / t)
    conn.close()
    db.close()

    # ── Louvain (no reference output) ──
    db, conn = open_db(db_path)
    rows, t = run_algo(
        conn,
        f"CALL louvain('{GRAPH_NAME}', {{resolution: 1.0}}) "
        f"YIELD node, community RETURN node.id, community;",
        "Louvain (resolution=1.0)",
    )
    ours = {int(r[0]): int(r[1]) for r in rows}
    comms = defaultdict(int)
    for c in ours.values():
        comms[c] += 1
    print(f"    Accuracy: N/A (no LDBC ref for community detection)")
    print(f"    Communities: {len(comms)}")
    results["Louvain"] = (t, -1.0, len(rows), num_edges / t)
    conn.close()
    db.close()

    return results


def print_summary(dataset_name, cfg, results):
    """Print benchmark summary table."""
    v = cfg["vertices"]
    e = cfg["edges"]
    print(f"\n{'=' * 70}")
    print("BENCHMARK SUMMARY")
    print(f"{'=' * 70}")
    print(f"Dataset: {dataset_name} ({v:,} vertices, {e:,} edges)")
    print()
    hdr = f"{'Algorithm':<15} {'Time(s)':<10} {'Accuracy':<12} {'MEdges/s':<12} {'Rows':<12}"
    print(hdr)
    print("─" * len(hdr))
    for algo, (t, acc, rows, throughput) in results.items():
        acc_str = f"{acc:.2%}" if acc >= 0 else "N/A"
        print(
            f"{algo:<15} {t:<10.3f} {acc_str:<12} "
            f"{throughput / 1e6:<12.2f} {rows:<12,}"
        )
    print("─" * len(hdr))


def main():
    parser = argparse.ArgumentParser(description="NeuG GDS Benchmark")
    parser.add_argument(
        "--dataset",
        default="graph500-23",
        choices=list(DATASETS.keys()),
        help="Dataset to benchmark",
    )
    parser.add_argument(
        "--skip-lcc",
        action="store_true",
        help="Skip LCC (slow on large graphs)",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip data loading (reuse existing DB)",
    )
    args = parser.parse_args()

    cfg = DATASETS[args.dataset]

    db_path = DB_PATH_PREFIX + args.dataset.replace("-", "_")

    if not args.skip_load:
        v_count, e_count, db_path = load_data(args.dataset, cfg)
    else:
        if not os.path.exists(db_path):
            print(f"ERROR: DB not found at {db_path}. Run without --skip-load first.")
            return 1
        v_count, e_count = cfg["vertices"], cfg["edges"]

    results = run_benchmarks(db_path, args.dataset, cfg, skip_lcc=args.skip_lcc)
    print_summary(args.dataset, cfg, results)

    return 0


if __name__ == "__main__":
    sys.exit(main())
