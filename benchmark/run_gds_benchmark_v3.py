#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LDBC Graphalytics GDS Benchmark: NeuG vs NetworkX

Dataset: datagen-8_0-fb (1.7M vertices, 107M edges, undirected)
Algorithms: WCC, BFS, PageRank, CDLP, Leiden, Louvain
"""

import os
import sys
import time
import shutil
import logging

logging.getLogger("neug").setLevel(logging.CRITICAL)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../tools/python_bind"))

from neug.database import Database
import networkx as nx

BENCHMARK_DIR = os.path.expanduser("~/Documents/git/neug/benchmark")
DATASET_NAME = "datagen-8_0-fb"
GRAPH_NAME = "benchmark_graph"
DB_PATH = "/tmp/gds_bench_v3_db"
BFS_SOURCE = 6
NUM_EDGES = 107507376


def load_neug():
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    db = Database(DB_PATH, "w")
    conn = db.connect()
    conn.execute("CREATE NODE TABLE Node (id INT64 PRIMARY KEY);")
    conn.execute("CREATE REL TABLE Edge (FROM Node TO Node);")
    print("  Loading NeuG...", end=" ", flush=True)
    t0 = time.time()
    vf = os.path.join(BENCHMARK_DIR, f"{DATASET_NAME}.v")
    vc = "/tmp/_bv3.csv"
    with open(vf) as fi, open(vc, "w") as fo:
        fo.write("id\n")
        for line in fi:
            v = line.strip()
            if v:
                fo.write(f"{v}\n")
    conn.execute(f"COPY Node FROM '{vc}' (header=true);")
    ef = os.path.join(BENCHMARK_DIR, f"{DATASET_NAME}.e")
    ec = "/tmp/_be3.csv"
    with open(ef) as fi, open(ec, "w") as fo:
        for line in fi:
            p = line.strip().split()
            if len(p) >= 2:
                fo.write(f"{p[0]},{p[1]}\n")
    conn.execute(f"COPY Edge FROM '{ec}' (header=false, delimiter=',');")
    os.remove(vc)
    os.remove(ec)
    conn.execute("LOAD gds;")
    conn.execute(
        f"CALL project_graph('{GRAPH_NAME}', ['Node'], "
        f"{{'[Node, Edge, Node]': ''}});"
    )
    print(f"{time.time() - t0:.1f}s")
    return db, conn


def load_nx():
    print("  Loading NetworkX...", end=" ", flush=True)
    t0 = time.time()
    G = nx.Graph()
    vf = os.path.join(BENCHMARK_DIR, f"{DATASET_NAME}.v")
    with open(vf) as f:
        for line in f:
            v = line.strip()
            if v:
                G.add_node(int(v))
    ef = os.path.join(BENCHMARK_DIR, f"{DATASET_NAME}.e")
    with open(ef) as f:
        for line in f:
            p = line.strip().split()
            if len(p) >= 2:
                G.add_edge(int(p[0]), int(p[1]))
    print(
        f"{time.time() - t0:.1f}s "
        f"({G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges)"
    )
    return G


def time_neug(conn, query):
    """Execute query and return (wall_time, row_count)."""
    t0 = time.time()
    rows = list(conn.execute(query))
    return time.time() - t0, len(rows)


def main():
    print("=" * 72)
    print("  NeuG GDS vs NetworkX — LDBC Graphalytics Benchmark")
    print(f"  Dataset: {DATASET_NAME} ({NUM_EDGES:,} edges)")
    print("=" * 72)
    print()

    db, conn = load_neug()
    G = load_nx()
    print()

    results = []  # (algo, neug_total, nx_time, notes)

    # ── WCC ──
    print("── WCC ──")
    t_n, n = time_neug(
        conn,
        f"CALL wcc('{GRAPH_NAME}', {{concurrency: 4}}) "
        f"YIELD node, comp RETURN node.id, comp;",
    )
    t0 = time.time()
    comps = list(nx.connected_components(G))
    t_nx = time.time() - t0
    results.append(("WCC", t_n, t_nx, f"{len(comps)} components"))
    print(f"  NeuG: {t_n:.3f}s  |  NetworkX: {t_nx:.3f}s  |  "
          f"Speedup: {t_nx / t_n:.1f}x")

    # ── BFS ──
    print(f"\n── BFS (source={BFS_SOURCE}) ──")
    t_n, n = time_neug(
        conn,
        f"CALL bfs('{GRAPH_NAME}', {{source: {BFS_SOURCE}}}) "
        f"YIELD node, distance RETURN node.id, distance;",
    )
    t0 = time.time()
    _ = sum(1 for _ in nx.single_source_shortest_path_length(G, BFS_SOURCE))
    t_nx = time.time() - t0
    results.append(("BFS", t_n, t_nx, f"source={BFS_SOURCE}"))
    print(f"  NeuG: {t_n:.3f}s  |  NetworkX: {t_nx:.3f}s  |  "
          f"Speedup: {t_nx / t_n:.1f}x")

    # ── PageRank ──
    print("\n── PageRank (10 iterations) ──")
    t_n, n = time_neug(
        conn,
        f"CALL page_rank('{GRAPH_NAME}', "
        f"{{max_iterations: 10, damping_factor: 0.85}}) "
        f"YIELD node, rank RETURN node.id, rank;",
    )
    t0 = time.time()
    _ = nx.pagerank(G, max_iter=10, tol=1e-6, alpha=0.85)
    t_nx = time.time() - t0
    results.append(("PageRank", t_n, t_nx, "iters=10, α=0.85"))
    print(f"  NeuG: {t_n:.3f}s  |  NetworkX: {t_nx:.3f}s  |  "
          f"Speedup: {t_nx / t_n:.1f}x")

    # ── CDLP (Label Propagation) ──
    print("\n── CDLP / Label Propagation (10 iterations) ──")
    t_n, n = time_neug(
        conn,
        f"CALL label_propagation('{GRAPH_NAME}', {{max_iterations: 10}}) "
        f"YIELD node, label RETURN node.id, label;",
    )
    t0 = time.time()
    nx_lp = list(nx.label_propagation_communities(G))
    t_nx = time.time() - t0
    results.append(("CDLP", t_n, t_nx, f"{len(nx_lp)} communities (NX)"))
    print(f"  NeuG: {t_n:.3f}s  |  NetworkX: {t_nx:.3f}s  |  "
          f"Speedup: {t_nx / t_n:.1f}x")

    # ── Leiden (NeuG only) ──
    print("\n── Leiden (NeuG only) ──")
    t_n, n = time_neug(
        conn,
        f"CALL leiden('{GRAPH_NAME}', {{resolution: 1.0}}) "
        f"YIELD node, community RETURN node.id, community;",
    )
    results.append(("Leiden", t_n, -1, f"{n} vertices"))
    print(f"  NeuG: {t_n:.3f}s  |  NetworkX: N/A (no built-in Leiden)")

    # ── Louvain ──
    print("\n── Louvain ──")
    t_n, n = time_neug(
        conn,
        f"CALL louvain('{GRAPH_NAME}', {{resolution: 1.0}}) "
        f"YIELD node, community RETURN node.id, community;",
    )
    t0 = time.time()
    try:
        nx_louvain = nx.community.louvain_communities(G, resolution=1.0,
                                                       seed=42)
        t_nx = time.time() - t0
        results.append(("Louvain", t_n, t_nx,
                        f"{len(nx_louvain)} communities (NX)"))
        print(f"  NeuG: {t_n:.3f}s  |  NetworkX: {t_nx:.3f}s  |  "
              f"Speedup: {t_nx / t_n:.1f}x")
    except Exception as e:
        results.append(("Louvain", t_n, -1, f"NX error: {e}"))
        print(f"  NeuG: {t_n:.3f}s  |  NetworkX: error ({e})")

    # Cleanup
    conn.close()
    db.close()
    shutil.rmtree(DB_PATH, ignore_errors=True)

    # ── Summary ──
    print(f"\n{'=' * 72}")
    print("  BENCHMARK SUMMARY")
    print(f"{'=' * 72}")
    print(f"  Dataset: {DATASET_NAME} (1,706,561 vertices, 107,507,376 edges)")
    print(f"  NeuG:      C++ parallel (concurrency=4), CSR + project_graph view")
    print(f"  NetworkX:  Python single-threaded, dict-based adjacency")
    print(f"  LadybugDB: N/A (no GDS algorithm support in v0.15.3)")
    print()
    print(f"  {'Algorithm':<15} {'NeuG(s)':<12} {'NetworkX(s)':<14} "
          f"{'Speedup':<10} {'Notes'}")
    print(f"  {'─' * 68}")
    for algo, t_n, t_nx, notes in results:
        nx_s = f"{t_nx:.3f}" if t_nx > 0 else "N/A"
        sp = f"{t_nx / t_n:.1f}x" if t_nx > 0 else "—"
        print(f"  {algo:<15} {t_n:<12.3f} {nx_s:<14} {sp:<10} {notes}")
    print(f"  {'─' * 68}")
    print()
    print("  Note: Times include Python-side result materialization")
    print("        (iterating ~1.7M rows). C++ algo-only times from logs:")


if __name__ == "__main__":
    main()
