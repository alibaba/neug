#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OF ANY KIND, either express or implied. See the License for
# the specific language governing permissions and limitations under the License.
#
# Conformance tests for the GDS extension against the official LDBC Graphalytics
# validation datasets (the tiny ``test-*`` graphs published at
# https://ldbcouncil.org/benchmarks/graphalytics/datasets/).
#
# Each dataset bundles a graph (``.v`` / ``.e``), a ``.properties`` file with the
# algorithm parameters, and a reference output file (``<name>-<ALGO>``).  We load
# the graph into NeuG, run the matching GDS algorithm with the parameters from the
# ``.properties`` file, and validate the result against the reference output using
# the same rules as the LDBC Graphalytics validator:
#
#   * BFS         -- exact match (unreachable == LONG_MAX in the reference).
#   * SSSP        -- floating point within epsilon ("infinity" == unreachable).
#   * PageRank    -- floating point within epsilon.
#   * LCC         -- floating point within epsilon.
#   * WCC / CDLP  -- partition equivalence (label values are arbitrary; only the
#                    induced grouping of vertices must match).

import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database

RESOURCE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "resources", "graphalytics"
)

# LDBC encodes "unreachable" in the BFS reference output as 2^63 - 1.
LONG_MAX = 9223372036854775807

# Tolerance used for floating point algorithms, matching the default epsilon of
# the LDBC Graphalytics validator.
FLOAT_EPSILON = 1e-4

# (dataset directory name, algorithm) for every vendored test graph.  The
# single-algorithm ``test-*`` graphs each validate one algorithm, while the
# ``example-*`` graphs carry reference outputs for all six algorithms.
DATASETS = [
    ("test-bfs-directed", "bfs"),
    ("test-bfs-undirected", "bfs"),
    ("test-sssp-directed", "sssp"),
    ("test-sssp-undirected", "sssp"),
    ("test-pr-directed", "pr"),
    ("test-pr-undirected", "pr"),
    ("test-wcc-directed", "wcc"),
    ("test-wcc-undirected", "wcc"),
    ("test-cdlp-directed", "cdlp"),
    ("test-cdlp-undirected", "cdlp"),
    ("test-lcc-directed", "lcc"),
    ("test-lcc-undirected", "lcc"),
] + [
    (graph, algo)
    for graph in ("example-directed", "example-undirected")
    for algo in ("bfs", "sssp", "pr", "wcc", "cdlp", "lcc")
]

ALGO_OUTPUT_SUFFIX = {
    "bfs": "BFS",
    "sssp": "SSSP",
    "pr": "PR",
    "wcc": "WCC",
    "cdlp": "CDLP",
    "lcc": "LCC",
}


# ---------------------------------------------------------------------------
# Parsing helpers for the Graphalytics on-disk format
# ---------------------------------------------------------------------------


def _parse_properties(path, name):
    """Read a ``<name>.properties`` file, stripping the ``graph.<name>.`` prefix."""
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
    """Return a list of ``(src, dst, weight)``; weight defaults to 1.0."""
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
    """Return ``{vertex_id: raw_value_string}`` from a reference output file."""
    expected = {}
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            expected[int(parts[0])] = parts[1]
    return expected


# ---------------------------------------------------------------------------
# Graph loading
# ---------------------------------------------------------------------------


def _load_graph(conn, vertices, edges):
    """Create a single-label graph (v)-[:e]->(v) and project it as 'g'."""
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


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def _floats_close(actual, expected, eps=FLOAT_EPSILON):
    diff = abs(actual - expected)
    if diff <= eps:
        return True
    return expected != 0.0 and diff / abs(expected) <= eps


def _partition(mapping):
    """Group keys by their assigned value into a comparable set-of-frozensets."""
    groups = {}
    for key, value in mapping.items():
        groups.setdefault(value, set()).add(key)
    return frozenset(frozenset(members) for members in groups.values())


# ---------------------------------------------------------------------------
# Per-algorithm validation
# ---------------------------------------------------------------------------


def _check_bfs(conn, props, directed, expected):
    source = props["bfs.source-vertex"]
    rows = conn.execute(
        "CALL bfs('g', {{source: '{}', directed: {}}}) "
        "YIELD node, distance RETURN node.id, distance;".format(
            source, str(directed).lower()
        )
    )
    actual = {int(r[0]): int(r[1]) for r in rows}

    def unreachable(val):
        return val is None or val < 0 or val >= LONG_MAX

    for vid, raw in expected.items():
        exp = int(raw)
        got = actual.get(vid)
        if exp >= LONG_MAX:
            assert unreachable(got), "vertex {} expected unreachable, got {}".format(
                vid, got
            )
        else:
            assert got == exp, "vertex {} expected distance {}, got {}".format(
                vid, exp, got
            )


def _check_sssp(conn, props, directed, expected):
    source = props["sssp.source-vertex"]
    weight = props.get("sssp.weight-property", "weight")
    rows = conn.execute(
        "CALL sssp('g', {{source: '{}', weight: '{}', directed: {}}}) "
        "YIELD node, distance RETURN node.id, distance;".format(
            source, weight, str(directed).lower()
        )
    )
    actual = {int(r[0]): float(r[1]) for r in rows}

    def unreachable(val):
        return val is None or val < 0

    for vid, raw in expected.items():
        got = actual.get(vid)
        if raw.lower() == "infinity":
            assert unreachable(got), "vertex {} expected infinity, got {}".format(
                vid, got
            )
        else:
            exp = float(raw)
            assert (
                got is not None and not unreachable(got) and _floats_close(got, exp)
            ), "vertex {} expected distance {}, got {}".format(vid, exp, got)


def _check_pr(conn, props, directed, expected):
    damping = float(props["pr.damping-factor"])
    iterations = int(props["pr.num-iterations"])
    rows = conn.execute(
        "CALL page_rank('g', {{damping_factor: {!r}, max_iterations: {}, "
        "directed: {}}}) YIELD node, rank RETURN node.id, rank;".format(
            damping, iterations, str(directed).lower()
        )
    )
    actual = {int(r[0]): float(r[1]) for r in rows}

    for vid, raw in expected.items():
        exp = float(raw)
        got = actual.get(vid)
        assert got is not None and _floats_close(
            got, exp
        ), "vertex {} expected rank {}, got {}".format(vid, exp, got)


def _check_lcc(conn, props, directed, expected):
    rows = conn.execute(
        "CALL lcc('g', {{directed: {}}}) YIELD node, lcc "
        "RETURN node.id, lcc;".format(str(directed).lower())
    )
    actual = {int(r[0]): float(r[1]) for r in rows}

    for vid, raw in expected.items():
        exp = float(raw)
        got = actual.get(vid)
        assert got is not None and _floats_close(
            got, exp
        ), "vertex {} expected lcc {}, got {}".format(vid, exp, got)


def _check_wcc(conn, props, directed, expected):
    rows = conn.execute(
        "CALL wcc('g', {concurrency: 1}) YIELD node, comp RETURN node.id, comp;"
    )
    actual = {int(r[0]): int(r[1]) for r in rows}
    expected_int = {vid: int(raw) for vid, raw in expected.items()}
    assert set(actual) == set(expected_int), "WCC vertex set mismatch"
    assert _partition(actual) == _partition(
        expected_int
    ), "WCC component partitioning does not match the reference output"


def _check_cdlp(conn, props, directed, expected):
    iterations = int(props.get("cdlp.max-iterations", "5"))
    rows = conn.execute(
        "CALL cdlp('g', {{max_iterations: {}}}) YIELD node, label "
        "RETURN node.id, label;".format(iterations)
    )
    actual = {int(r[0]): int(r[1]) for r in rows}
    expected_int = {vid: int(raw) for vid, raw in expected.items()}
    assert set(actual) == set(expected_int), "CDLP vertex set mismatch"
    assert _partition(actual) == _partition(
        expected_int
    ), "CDLP community partitioning does not match the reference output"


_CHECKERS = {
    "bfs": _check_bfs,
    "sssp": _check_sssp,
    "pr": _check_pr,
    "lcc": _check_lcc,
    "wcc": _check_wcc,
    "cdlp": _check_cdlp,
}


# ---------------------------------------------------------------------------
# GDS availability gate
# ---------------------------------------------------------------------------


def _gds_available(tmp_path):
    """Best-effort probe: skip the suite if the gds extension cannot be loaded."""
    try:
        db = Database(db_path=str(tmp_path / "gds_probe"), mode="w")
        conn = db.connect()
        try:
            conn.execute("LOAD gds;")
        finally:
            conn.close()
            db.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Test entry point
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dataset,algorithm",
    DATASETS,
    ids=["{}[{}]".format(name, algo) for name, algo in DATASETS],
)
def test_graphalytics_conformance(tmp_path, dataset, algorithm):
    if not _gds_available(tmp_path):
        pytest.skip("gds extension is not available in this build")

    dataset_dir = os.path.join(RESOURCE_DIR, dataset)
    assert os.path.isdir(dataset_dir), "missing dataset directory: {}".format(
        dataset_dir
    )

    props = _parse_properties(
        os.path.join(dataset_dir, dataset + ".properties"), dataset
    )
    directed = props.get("directed", "false").lower() == "true"
    vertices = _read_vertices(os.path.join(dataset_dir, dataset + ".v"))
    edges = _read_edges(os.path.join(dataset_dir, dataset + ".e"))
    expected = _read_reference(
        os.path.join(dataset_dir, dataset + "-" + ALGO_OUTPUT_SUFFIX[algorithm])
    )

    db = Database(db_path=str(tmp_path / dataset), mode="w")
    conn = db.connect()
    try:
        _load_graph(conn, vertices, edges)
        _CHECKERS[algorithm](conn, props, directed, expected)
    finally:
        conn.close()
        db.close()
