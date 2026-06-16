#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Competitor comparison benchmark: NeuG GDS vs NetworkX
Uses datagen-8_0-fb dataset (1.7M vertices, 107M edges).
Runs: WCC, BFS, PageRank, CDLP (Label Propagation), Leiden, Louvain.
"""

import os
import sys
import time
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../tools/python_bind"))

from neug.database import Database
import networkx as nx

BENCHMARK_DIR = os.path.expanduser("~/Documents/git/neug/benchmark")
DATASET_NAME = "datagen-8_0-fb"
GRAPH_NAME = "benchmark_graph"
DB_PATH = "/tmp/competitor_benchmark_db"
BFS_SOURCE = 6


def load_neug_graph():
    """Load data into NeuG, return (db, conn)."""
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)

    db = Database(DB_PATH, "w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Node (id INT64 PRIMARY KEY);")
    conn.execute("CREATE REL TABLE Edge (FROM Node TO Node);")

    print("Loading NeuG data...")
    t0 = time.time()
    vfile = os.path.join(BENCHMARK_DIR, f"{DATASET_NAME}.v")
    vcsv = "/tmp/comp_vertices.csv"
    with open(vfile) as fin, open(vcsv, "w") as fout:
        fout.write("id\n")
        for line in fin:
            v = line.strip()
            if v:
                fout.write(f"{v}\n")
    conn.execute(f"COPY Node FROM '{vcsv}' (header=true);")

    efile = os.path.join(BENCHMARK_DIR, f"{DATASET_NAME}.e")
    ecsv = "/tmp/comp_edges.csv"
    with open(efile) as fin, open(ecsv, "w") as fout:
        for line in fin:
            parts = line.strip().split()
            if len(parts) >= 2:
                fout.write(f"{parts[0]},{parts[1]}\n")
    conn.execute(f"COPY Edge FROM '{ecsv}' (header=false, delimiter=',');")
    print(f"  NeuG data loaded in {time.time() - t0:.1f}s")

    conn.execute("LOAD gds;")
    conn.execute(
        f"CALL project_graph('{GRAPH_NAME}', ['Node'], "
        f"{{'[Node, Edge, Node]': ''}});"
    )
    os.remove(vcsv)
    os.remove(ecsv)
    return db, conn


def load_networkx_graph():
    """Load same data into NetworkX."""
    print("Loading NetworkX graph...")
    t0 = time.time()
    G = nx.Graph()
    vfile = os.path.join(BENCHMARK_DIR, f"{DATASET_NAME}.v")
    with open(vfile) as f:
        for line in f:
            v = line.strip()
            if v:
                G.add_node(int(v))
    efile = os.path.join(BENCHMARK_DIR, f"{DATASET_NAME}.e")
    with open(efile) as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2:
                G.add_edge(int(parts[0]), int(parts[1]))
    print(f"  NetworkX loaded in {time.time() - t0:.1f}s "
          f"({G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges)")
    return G


def bench_neug(conn, algo_name, query):
    """Time a NeuG GDS query."""
    t0 = time.time()
    rows = list(conn.execute(query))
    elapsed = time.time() - t0
    return elapsed, len(rows)


def bench_nx(G, algo_name, func, *args, **kwargs):
    """Time a NetworkX function."""
    t0 = time.time()
    result = func(G, *args, **kwargs)
    elapsed = time.time() - t0
    # Consume generators if needed
    if hasattr(result, '__len__'):
        n = len(result)
    else:
        n = -1
    return elapsed, n


def main():
    print("=" * 70)
    print("NeuG GDS vs NetworkX Competitor Benchmark")
    print(f"Dataset: {DATASET_NAME}")
    print("=" * 70)

    # Load both
    db, conn = load_neug_graph()
    G = load_networkx_graph()

    results = {}  # algo -> (neug_time, nx_time)

    # ── WCC ──
    print("\n── WCC ──")
    t_neug, n = bench_neug(conn, "WCC",
        f"CALL wcc('{GRAPH_NAME}', {{concurrency: 4}}) "
        f"YIELD node, comp RETURN node.id, comp;")
    t_nx, _ = bench_nx(G, "WCC", nx.connected_components)
    results["WCC"] = (t_neug, t_nx, n)
    print(f"  NeuG: {t_neug:.3f}s  |  NetworkX: {t_nx:.3f}s  |  "
          f"Speedup: {t_nx/t_neug:.1f}x")

    # ── BFS ──
    print(f"\n── BFS (source={BFS_SOURCE}) ──")
    t_neug, n = bench_neug(conn, "BFS",
        f"CALL bfs('{GRAPH_NAME}', {{source: '{BFS_SOURCE}'}}) "
        f"YIELD node, distance RETURN node.id, distance;")
    t_nx, _ = bench_nx(G, "BFS", nx.single_source_shortest_path_length,
                        BFS_SOURCE)
    results["BFS"] = (t_neug, t_nx, n)
    print(f"  NeuG: {t_neug:.3f}s  |  NetworkX: {t_nx:.3f}s  |  "
          f"Speedup: {t_nx/t_neug:.1f}x")

    # ── PageRank ──
    print("\n── PageRank (iters=10) ──")
    t_neug, n = bench_neug(conn, "PageRank",
        f"CALL page_rank('{GRAPH_NAME}', {{max_iterations: 10}}) "
        f"YIELD node, rank RETURN node.id, rank;")
    t_nx, _ = bench_nx(G, "PageRank", nx.pagerank, max_iter=10, tol=1e-6)
    results["PageRank"] = (t_neug, t_nx, n)
    print(f"  NeuG: {t_neug:.3f}s  |  NetworkX: {t_nx:.3f}s  |  "
          f"Speedup: {t_nx/t_neug:.1f}x")

    # ── CDLP (Label Propagation) ──
    print("\n── CDLP / Label Propagation (iters=10) ──")
    t_neug, n = bench_neug(conn, "CDLP",
        f"CALL label_propagation('{GRAPH_NAME}', {{max_iterations: 10}}) "
        f"YIELD node, label RETURN node.id, label;")
    # NetworkX label_propagation_communities returns a generator
    t0 = time.time()
    nx_comms = list(nx.label_propagation_communities(G))
    t_nx = time.time() - t0
    results["CDLP"] = (t_neug, t_nx, n)
    print(f"  NeuG: {t_neug:.3f}s  |  NetworkX: {t_nx:.3f}s  |  "
          f"Speedup: {t_nx/t_neug:.1f}x")

    # ── Leiden (no NetworkX equivalent, use Louvain) ──
    print("\n── Community Detection: NeuG Leiden vs NetworkX Louvain ──")
    t_neug_leiden, n = bench_neug(conn, "Leiden",
        f"CALL leiden('{GRAPH_NAME}', {{resolution: 1.0}}) "
        f"YIELD node, community RETURN node.id, community;")
    try:
        t0 = time.time()
        nx_comms = nx.community.louvain_communities(G, resolution=1.0, seed=42)
        t_nx_louvain = time.time() - t0
    except Exception as e:
        t_nx_louvain = -1
        print(f"  NetworkX Louvain error: {e}")

    # ── NeuG Louvain ──
    print("\n── NeuG Louvain ──")
    t_neug_louvain, n2 = bench_neug(conn, "Louvain",
        f"CALL louvain('{GRAPH_NAME}', {{resolution: 1.0}}) "
        f"YIELD node, community RETURN node.id, community;")
    results["Leiden"] = (t_neug_leiden, t_nx_louvain, n)
    results["Louvain"] = (t_neug_louvain, t_nx_louvain, n2)
    if t_nx_louvain > 0:
        print(f"  NeuG Leiden: {t_neug_leiden:.3f}s")
        print(f"  NeuG Louvain: {t_neug_louvain:.3f}s")
        print(f"  NetworkX Louvain: {t_nx_louvain:.3f}s")

    # Cleanup
    conn.close()
    db.close()
    shutil.rmtree(DB_PATH, ignore_errors=True)

    # ── Summary ──
    print(f"\n{'=' * 70}")
    print("COMPETITOR BENCHMARK SUMMARY")
    print(f"{'=' * 70}")
    print(f"Dataset: {DATASET_NAME}")
    print(f"  NeuG (parallel C++, concurrency=4)")
    print(f"  NetworkX (single-threaded Python)")
    print()
    hdr = f"{'Algorithm':<15} {'NeuG(s)':<12} {'NetworkX(s)':<14} {'Speedup':<10}"
    print(hdr)
    print("─" * len(hdr))
    for algo, (t_neug, t_nx, _) in results.items():
        nx_str = f"{t_nx:.3f}" if t_nx > 0 else "N/A"
        speedup = f"{t_nx/t_neug:.1f}x" if t_nx > 0 and t_neug > 0 else "N/A"
        print(f"{algo:<15} {t_neug:<12.3f} {nx_str:<14} {speedup:<10}")
    print("─" * len(hdr))
    print("\nNote: NeuG uses multi-threaded C++ with CSR storage;")
    print("      NetworkX is single-threaded Python with dict-based adjacency.")


if __name__ == "__main__":
    main()
