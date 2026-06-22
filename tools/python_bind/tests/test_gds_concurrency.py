#!/usr/bin/env python3
"""Comprehensive concurrency tests for BFS/SSSP path return.

Tests the concurrency fix (commit 3dbc7588) that:
1. BFS dense pull mode: uses atomic CAS for distance update
2. SSSP: routes path return through single-threaded SSSPPred

Key insight: when multiple shortest paths exist, parallel BFS may return
different valid paths across runs (the CAS winner is non-deterministic).
This is EXPECTED — what we verify is:
  - Distances are always deterministic and correct
  - Every returned path is a valid shortest path (correct length, valid
    edges, no cycles, correct endpoints)
  - No crashes or corruption under high-concurrency stress
  - SSSP (routed through single-threaded SSSPPred) IS fully deterministic
"""

import csv
import os
import random
import sys
from collections import defaultdict

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from neug.database import Database

# Graph parameters — must be > 4096 to trigger multi-threaded parallel_for
# (chunk_size = 4096 in ParallelUtils).  We use 20000 vertices to get ~5
# threads, and structure edges so that large BFS frontiers trigger the
# dense pull mode (frontier > 5% of vertex count).
NUM_VERTICES = 20000
HUB_COUNT = 1000  # vertices at BFS level 1 from source
EDGES_PER_HUB = 5  # edges from each hub to layer 2
LAYER2_START = HUB_COUNT + 1
LAYER2_COUNT = 4000


def generate_graph_csv(tmp_dir):
    """Generate CSV files for a synthetic graph designed to trigger
    dense pull mode and stress-test the CAS in BFS.

    Structure:
      - Source (id=0) connected to 1000 hubs (ids 1..1000)  → level 1
      - Each hub connected to 5 vertices in 1001..5000        → level 2
      - Each layer-2 vertex connected to 3 in 5001..10000     → level 3
      - Random cross-edges to create multiple paths (races)
    """
    v_path = os.path.join(tmp_dir, "vertices.csv")
    e_path = os.path.join(tmp_dir, "edges.csv")

    with open(v_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id"])
        for i in range(NUM_VERTICES):
            w.writerow([i])

    rng = random.Random(42)  # deterministic graph generation

    with open(e_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["src", "dst"])
        # Source → hubs
        for i in range(1, HUB_COUNT + 1):
            w.writerow([0, i])
        # Hubs → layer 2 (multiple hubs point to same layer-2 vertex,
        # creating the race condition in the predecessor array)
        for i in range(1, HUB_COUNT + 1):
            for _ in range(EDGES_PER_HUB):
                dst = rng.randint(LAYER2_START, LAYER2_START + LAYER2_COUNT - 1)
                w.writerow([i, dst])
        # Layer 2 → layer 3
        for i in range(LAYER2_START, LAYER2_START + LAYER2_COUNT):
            for _ in range(3):
                dst = rng.randint(5001, 10000)
                w.writerow([i, dst])
        # Some cross-edges within hubs to create alternative paths
        for _ in range(2000):
            src = rng.randint(1, HUB_COUNT)
            dst = rng.randint(1, HUB_COUNT)
            if src != dst:
                w.writerow([src, dst])

    return v_path, e_path


@pytest.fixture(scope="module")
def large_graph_db(tmp_path_factory):
    """Create a large graph DB for concurrency testing."""
    tmp_dir = tmp_path_factory.mktemp("concurrency_graph")
    v_path, e_path = generate_graph_csv(str(tmp_dir))

    db_dir = str(tmp_path_factory.mktemp("concurrency_db"))
    db = Database(db_path=db_dir, mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE v(id INT64, PRIMARY KEY(id));")
        conn.execute("CREATE REL TABLE e(FROM v TO v);")
        conn.execute(f"COPY v FROM '{v_path}' (DELIMITER=',', HEADER=true);")
        conn.execute(f"COPY e FROM '{e_path}' (DELIMITER=',', HEADER=true);")
        conn.execute("CALL project_graph('g', ['v'], {'[v, e, v]': ''});")
        conn.execute("LOAD gds;")
        yield conn
    finally:
        conn.close()
        db.close()


def extract_path_vertices(path):
    """Extract the list of vertex IDs from a path dict."""
    if path is None:
        return None
    nodes = path.get("nodes", [])
    ids = []
    for node in nodes:
        vid = node.get("id")
        if vid is not None:
            ids.append(vid)
    return ids


def build_adjacency_map(conn):
    """Build an adjacency map from the graph for path validation."""
    rows = list(conn.execute("MATCH (a:v)-[:e]->(b:v) RETURN a.id, b.id;"))
    adj = defaultdict(set)
    for src, dst in rows:
        adj[src].add(dst)
    return adj


def validate_bfs_path(nid, dist, path, adj, dist_map, source=0):
    """Validate a single BFS path. Returns error message or None."""
    if dist < 0:
        if path is not None:
            return f"node {nid}: unreachable but path is not None"
        return None
    if dist == 0:
        if path is None:
            return f"node {nid}: source but path is None"
        path_ids = extract_path_vertices(path)
        if path_ids != [nid]:
            return f"node {nid}: source path should be [{nid}], got {path_ids}"
        return None

    if path is None:
        return f"node {nid}: dist={dist} but path is None"

    path_ids = extract_path_vertices(path)
    if path_ids is None:
        return f"node {nid}: path has no vertices"

    # Check endpoints
    if path_ids[0] != source:
        return f"node {nid}: path starts at {path_ids[0]}, expected {source}"
    if path_ids[-1] != nid:
        return f"node {nid}: path ends at {path_ids[-1]}, expected {nid}"

    # Check length
    if len(path_ids) - 1 != dist:
        return f"node {nid}: path length {len(path_ids) - 1} != distance {dist}"

    # Check no cycles
    if len(path_ids) != len(set(path_ids)):
        return f"node {nid}: path has cycles: {path_ids}"

    # Check valid edges (undirected: either direction OK)
    for i in range(len(path_ids) - 1):
        src, dst = path_ids[i], path_ids[i + 1]
        if dst not in adj.get(src, set()) and src not in adj.get(dst, set()):
            return f"node {nid}: invalid edge {src}->{dst} in path"

    # Check distance monotonicity (each vertex at position i has distance i)
    for i, vid in enumerate(path_ids):
        if dist_map.get(vid) != i:
            return (
                f"node {nid}: vertex {vid} at position {i} has distance "
                f"{dist_map.get(vid)}, expected {i}"
            )

    return None


def validate_sssp_path(nid, dist, path, adj, source=0):
    """Validate a single SSSP path. Returns error message or None."""
    if dist < 0:
        if path is not None:
            return f"node {nid}: unreachable but path is not None"
        return None
    if dist == 0.0:
        if path is None:
            return f"node {nid}: source but path is None"
        path_ids = extract_path_vertices(path)
        if path_ids != [nid]:
            return f"node {nid}: source path should be [{nid}], got {path_ids}"
        return None

    if path is None:
        return f"node {nid}: dist={dist} but path is None"

    path_ids = extract_path_vertices(path)
    if path_ids is None:
        return f"node {nid}: path has no vertices"

    # Check endpoints
    if path_ids[0] != source:
        return f"node {nid}: path starts at {path_ids[0]}, expected {source}"
    if path_ids[-1] != nid:
        return f"node {nid}: path ends at {path_ids[-1]}, expected {nid}"

    # Check length (unit weights: distance == path edge count)
    if len(path_ids) - 1 != dist:
        return f"node {nid}: path length {len(path_ids) - 1} != distance {dist}"

    # Check no cycles
    if len(path_ids) != len(set(path_ids)):
        return f"node {nid}: path has cycles: {path_ids}"

    # Check valid edges
    for i in range(len(path_ids) - 1):
        src, dst = path_ids[i], path_ids[i + 1]
        if dst not in adj.get(src, set()) and src not in adj.get(dst, set()):
            return f"node {nid}: invalid edge {src}->{dst} in SSSP path"

    return None


# ---------------------------------------------------------------------------
# BFS tests
# ---------------------------------------------------------------------------


class TestBFSConcurrency:
    """BFS path return concurrency tests."""

    def test_bfs_distance_determinism(self, large_graph_db):
        """BFS distances must be deterministic across 50 runs with max threads.

        While specific paths may vary (multiple valid shortest paths exist),
        the distance to each vertex must always be the same.
        """
        conn = large_graph_db
        baseline = None
        ITERATIONS = 50

        for i in range(ITERATIONS):
            rows = list(
                conn.execute(
                    f"CALL bfs('g', {{source: '0', concurrency: {os.cpu_count()}}}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            dist_map = {nid: dist for nid, dist, _ in rows}
            sorted_dists = sorted(dist_map.items())

            if baseline is None:
                baseline = sorted_dists
            else:
                assert sorted_dists == baseline, (
                    f"Non-deterministic BFS distance at iteration {i}! "
                    f"Distances differ from baseline."
                )

    def test_bfs_path_validity_stress(self, large_graph_db):
        """Run BFS 50 times and validate every path each time.

        Each path must be a valid shortest path: correct length, valid edges,
        no cycles, correct endpoints, distance-monotonic.
        """
        conn = large_graph_db
        adj = build_adjacency_map(conn)
        ITERATIONS = 50
        errors_total = 0

        for i in range(ITERATIONS):
            rows = list(
                conn.execute(
                    f"CALL bfs('g', {{source: '0', concurrency: {os.cpu_count()}}}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            dist_map = {nid: dist for nid, dist, _ in rows}

            for nid, dist, path in rows:
                err = validate_bfs_path(nid, dist, path, adj, dist_map)
                if err:
                    errors_total += 1
                    if errors_total <= 5:
                        print(f"[iter {i}] {err}")

        assert (
            errors_total == 0
        ), f"{errors_total} path validation errors across {ITERATIONS} iterations"

    def test_bfs_path_length_equals_distance(self, large_graph_db):
        """For every reachable vertex, path length (edges) must equal distance."""
        conn = large_graph_db
        rows = list(
            conn.execute(
                "CALL bfs('g', {source: '0'}) "
                "YIELD node, distance, path "
                "RETURN node.id, distance, path;"
            )
        )
        for nid, dist, path in rows:
            if dist <= 0:
                continue
            path_ids = extract_path_vertices(path)
            assert path_ids is not None, f"node {nid} dist={dist} has null path"
            assert (
                len(path_ids) - 1 == dist
            ), f"node {nid}: path length {len(path_ids) - 1} != distance {dist}"

    def test_bfs_path_source_correct(self, large_graph_db):
        """Every path must start at the source vertex (id=0)."""
        conn = large_graph_db
        rows = list(
            conn.execute(
                "CALL bfs('g', {source: '0'}) "
                "YIELD node, distance, path "
                "RETURN node.id, distance, path;"
            )
        )
        for nid, dist, path in rows:
            if dist < 0:
                assert path is None, f"unreachable node {nid} should have null path"
                continue
            path_ids = extract_path_vertices(path)
            assert path_ids is not None, f"node {nid} has null path"
            assert (
                path_ids[0] == 0
            ), f"node {nid}: path starts at {path_ids[0]}, expected 0"
            assert (
                path_ids[-1] == nid
            ), f"node {nid}: path ends at {path_ids[-1]}, expected {nid}"

    def test_bfs_path_no_cycles(self, large_graph_db):
        """No path should contain repeated vertices (cycles)."""
        conn = large_graph_db
        rows = list(
            conn.execute(
                "CALL bfs('g', {source: '0'}) "
                "YIELD node, distance, path "
                "RETURN node.id, distance, path;"
            )
        )
        for nid, dist, path in rows:
            if dist <= 0:
                continue
            path_ids = extract_path_vertices(path)
            assert path_ids is not None
            assert len(path_ids) == len(
                set(path_ids)
            ), f"node {nid}: path has cycles: {path_ids}"

    def test_bfs_path_valid_edges(self, large_graph_db):
        """Every consecutive pair in a path must be connected by an edge."""
        conn = large_graph_db
        adj = build_adjacency_map(conn)
        rows = list(
            conn.execute(
                "CALL bfs('g', {source: '0'}) "
                "YIELD node, distance, path "
                "RETURN node.id, distance, path;"
            )
        )
        for nid, dist, path in rows:
            if dist <= 0:
                continue
            path_ids = extract_path_vertices(path)
            assert path_ids is not None
            for i in range(len(path_ids) - 1):
                src, dst = path_ids[i], path_ids[i + 1]
                assert dst in adj.get(src, set()) or src in adj.get(
                    dst, set()
                ), f"node {nid}: invalid edge {src}->{dst} in path"

    def test_bfs_path_distance_monotonic(self, large_graph_db):
        """Predecessor in path must be at distance-1 from successor."""
        conn = large_graph_db
        rows = list(
            conn.execute(
                "CALL bfs('g', {source: '0'}) "
                "YIELD node, distance, path "
                "RETURN node.id, distance, path;"
            )
        )
        dist_map = {nid: dist for nid, dist, _ in rows}
        for nid, dist, path in rows:
            if dist <= 0:
                continue
            path_ids = extract_path_vertices(path)
            assert path_ids is not None
            for i, vid in enumerate(path_ids):
                expected_dist = i
                assert dist_map.get(vid) == expected_dist, (
                    f"node {nid}: vertex {vid} at position {i} has distance "
                    f"{dist_map.get(vid)}, expected {expected_dist}"
                )

    def test_bfs_directed_path_validity(self, large_graph_db):
        """BFS with directed=true: validate all paths across 30 iterations."""
        conn = large_graph_db
        adj = build_adjacency_map(conn)
        ITERATIONS = 30
        errors_total = 0

        for i in range(ITERATIONS):
            rows = list(
                conn.execute(
                    f"CALL bfs('g', {{source: '0', directed: true, "
                    f"concurrency: {os.cpu_count()}}}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            dist_map = {nid: dist for nid, dist, _ in rows}
            for nid, dist, path in rows:
                err = validate_bfs_path(nid, dist, path, adj, dist_map)
                if err:
                    errors_total += 1
                    if errors_total <= 5:
                        print(f"[directed iter {i}] {err}")
        assert (
            errors_total == 0
        ), f"{errors_total} directed BFS path errors across {ITERATIONS} iterations"

    def test_bfs_directed_distance_determinism(self, large_graph_db):
        """BFS directed distances must be deterministic across 30 runs."""
        conn = large_graph_db
        baseline = None
        for i in range(30):
            rows = list(
                conn.execute(
                    f"CALL bfs('g', {{source: '0', directed: true, "
                    f"concurrency: {os.cpu_count()}}}) "
                    "YIELD node, distance "
                    "RETURN node.id, distance;"
                )
            )
            dist_map = sorted((nid, dist) for nid, dist in rows)
            if baseline is None:
                baseline = dist_map
            else:
                assert (
                    dist_map == baseline
                ), f"Non-deterministic directed BFS distance at iteration {i}!"


# ---------------------------------------------------------------------------
# SSSP tests
# ---------------------------------------------------------------------------


class TestSSSPConcurrency:
    """SSSP path return concurrency tests.

    SSSP with return_path=true routes through SSSPPred (single-threaded
    Dijkstra), so results should be fully deterministic AND correct.
    """

    def test_sssp_full_determinism(self, large_graph_db):
        """SSSP with path return must be fully deterministic (single-threaded)."""
        conn = large_graph_db
        baseline = None
        ITERATIONS = 50

        for i in range(ITERATIONS):
            rows = list(
                conn.execute(
                    f"CALL sssp('g', {{source: '0', concurrency: {os.cpu_count()}}}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            result = sorted(
                (nid, dist, tuple(extract_path_vertices(path) or []))
                for nid, dist, path in rows
            )
            if baseline is None:
                baseline = result
            else:
                assert result == baseline, (
                    f"Non-deterministic SSSP at iteration {i}! "
                    f"SSSPPred should be single-threaded and deterministic."
                )

    def test_sssp_path_validity_stress(self, large_graph_db):
        """Run SSSP 50 times and validate every path each time."""
        conn = large_graph_db
        adj = build_adjacency_map(conn)
        ITERATIONS = 50
        errors_total = 0

        for i in range(ITERATIONS):
            rows = list(
                conn.execute(
                    f"CALL sssp('g', {{source: '0', concurrency: {os.cpu_count()}}}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            for nid, dist, path in rows:
                err = validate_sssp_path(nid, dist, path, adj)
                if err:
                    errors_total += 1
                    if errors_total <= 5:
                        print(f"[iter {i}] {err}")
        assert (
            errors_total == 0
        ), f"{errors_total} SSSP path errors across {ITERATIONS} iterations"

    def test_sssp_path_length_equals_distance(self, large_graph_db):
        """With unit weights, SSSP distance must equal path edge count."""
        conn = large_graph_db
        rows = list(
            conn.execute(
                "CALL sssp('g', {source: '0'}) "
                "YIELD node, distance, path "
                "RETURN node.id, distance, path;"
            )
        )
        for nid, dist, path in rows:
            if dist < 0:
                continue
            path_ids = extract_path_vertices(path)
            assert path_ids is not None
            assert (
                len(path_ids) - 1 == dist
            ), f"node {nid}: SSSP path length {len(path_ids) - 1} != distance {dist}"

    def test_sssp_path_source_correct(self, large_graph_db):
        """Every path must start at source and end at the target vertex."""
        conn = large_graph_db
        rows = list(
            conn.execute(
                "CALL sssp('g', {source: '0'}) "
                "YIELD node, distance, path "
                "RETURN node.id, distance, path;"
            )
        )
        for nid, dist, path in rows:
            if dist < 0:
                assert path is None
                continue
            path_ids = extract_path_vertices(path)
            assert path_ids is not None, f"node {nid} has null path"
            assert path_ids[0] == 0, f"node {nid}: path starts at {path_ids[0]}"
            assert path_ids[-1] == nid, f"node {nid}: path ends at {path_ids[-1]}"

    def test_sssp_path_no_cycles(self, large_graph_db):
        """No path should contain repeated vertices."""
        conn = large_graph_db
        rows = list(
            conn.execute(
                "CALL sssp('g', {source: '0'}) "
                "YIELD node, distance, path "
                "RETURN node.id, distance, path;"
            )
        )
        for nid, dist, path in rows:
            if dist < 0:
                continue
            path_ids = extract_path_vertices(path)
            assert path_ids is not None
            assert len(path_ids) == len(
                set(path_ids)
            ), f"node {nid}: SSSP path has cycles: {path_ids}"

    def test_sssp_path_valid_edges(self, large_graph_db):
        """Every consecutive pair in a path must be connected by an edge."""
        conn = large_graph_db
        adj = build_adjacency_map(conn)
        rows = list(
            conn.execute(
                "CALL sssp('g', {source: '0'}) "
                "YIELD node, distance, path "
                "RETURN node.id, distance, path;"
            )
        )
        for nid, dist, path in rows:
            if dist < 0:
                continue
            path_ids = extract_path_vertices(path)
            assert path_ids is not None
            for i in range(len(path_ids) - 1):
                src, dst = path_ids[i], path_ids[i + 1]
                assert dst in adj.get(src, set()) or src in adj.get(
                    dst, set()
                ), f"node {nid}: invalid edge {src}->{dst} in SSSP path"

    def test_sssp_directed_full_determinism(self, large_graph_db):
        """SSSP directed must be fully deterministic (single-threaded)."""
        conn = large_graph_db
        baseline = None
        for i in range(30):
            rows = list(
                conn.execute(
                    f"CALL sssp('g', {{source: '0', directed: true, "
                    f"concurrency: {os.cpu_count()}}}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            result = sorted(
                (nid, dist, tuple(extract_path_vertices(path) or []))
                for nid, dist, path in rows
            )
            if baseline is None:
                baseline = result
            else:
                assert (
                    result == baseline
                ), f"Non-deterministic directed SSSP at iteration {i}!"


# ---------------------------------------------------------------------------
# Cross-validation: BFS vs SSSP
# ---------------------------------------------------------------------------


class TestBFSvsSSSPConsistency:
    """Verify BFS and SSSP agree on unit-weight graphs."""

    def test_bfs_sssp_same_distances(self, large_graph_db):
        """BFS and SSSP must return identical distances on unit-weight graph."""
        conn = large_graph_db
        bfs_rows = list(
            conn.execute(
                "CALL bfs('g', {source: '0'}) "
                "YIELD node, distance "
                "RETURN node.id, distance;"
            )
        )
        sssp_rows = list(
            conn.execute(
                "CALL sssp('g', {source: '0'}) "
                "YIELD node, distance "
                "RETURN node.id, distance;"
            )
        )
        bfs_dist = {nid: d for nid, d in bfs_rows}
        sssp_dist = {nid: d for nid, d in sssp_rows}

        for nid in bfs_dist:
            bfs_d = bfs_dist[nid]
            sssp_d = sssp_dist.get(nid)
            if bfs_d == -1:
                assert (
                    sssp_d is not None and sssp_d < 0
                ), f"node {nid}: BFS unreachable but SSSP distance={sssp_d}"
            else:
                assert sssp_d == float(
                    bfs_d
                ), f"node {nid}: BFS distance={bfs_d} but SSSP distance={sssp_d}"


# ---------------------------------------------------------------------------
# Multi-source stress tests
# ---------------------------------------------------------------------------


class TestStressMultipleSources:
    """Stress test with multiple source vertices."""

    def test_bfs_multiple_sources_validity(self, large_graph_db):
        """Run BFS from different sources; validate all paths."""
        conn = large_graph_db
        adj = build_adjacency_map(conn)
        sources = [0, 1, 500, 1000, 5000]

        for src in sources:
            for i in range(15):
                rows = list(
                    conn.execute(
                        f"CALL bfs('g', {{source: '{src}'}}) "
                        "YIELD node, distance, path "
                        "RETURN node.id, distance, path;"
                    )
                )
                dist_map = {nid: dist for nid, dist, _ in rows}
                for nid, dist, path in rows:
                    err = validate_bfs_path(nid, dist, path, adj, dist_map, source=src)
                    assert err is None, f"[BFS src={src} iter={i}] {err}"

    def test_sssp_multiple_sources_determinism(self, large_graph_db):
        """Run SSSP from different sources; verify full determinism."""
        conn = large_graph_db
        sources = [0, 1, 500, 1000, 5000]

        for src in sources:
            baseline = None
            for i in range(15):
                rows = list(
                    conn.execute(
                        f"CALL sssp('g', {{source: '{src}'}}) "
                        "YIELD node, distance, path "
                        "RETURN node.id, distance, path;"
                    )
                )
                result = sorted(
                    (nid, dist, tuple(extract_path_vertices(path) or []))
                    for nid, dist, path in rows
                )
                if baseline is None:
                    baseline = result
                else:
                    assert (
                        result == baseline
                    ), f"Non-deterministic SSSP from source {src} at iteration {i}!"
