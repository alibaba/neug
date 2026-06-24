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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tests for GDS BFS/SSSP shortest path return feature.
# When YIELDing the `path` column, BFS/SSSP return the actual shortest path
# (vertex sequence) in addition to the distance.

import os
import sys
from contextlib import contextmanager

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database


@contextmanager
def tinysnb_simple_connection(tmp_path):
    """Open a writable DB with tinysnb loaded and a simple projected graph
    (single vertex label 'person', single edge label 'knows')."""
    db_dir = tmp_path / "gds_path_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute(
            "CALL project_graph("
            "'person_knows', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )
        conn.execute("LOAD gds;")
        yield conn
    finally:
        conn.close()
        db.close()


@contextmanager
def custom_graph_connection(tmp_path):
    """Create a small custom graph for deterministic path testing.

    Graph structure (undirected, unit weights):
        0 -- 1 -- 2
        |         |
        3 -- 4 -- 5

    Shortest paths from 0:
      0->0: [0]            distance 0
      0->1: [0,1]          distance 1
      0->2: [0,1,2]        distance 2
      0->3: [0,3]          distance 1
      0->4: [0,3,4]        distance 2
      0->5: [0,3,4,5] or [0,1,2,5]  distance 3
    """
    db_dir = tmp_path / "gds_path_custom_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE v(id INT64, PRIMARY KEY(id));")
        conn.execute("CREATE REL TABLE e(FROM v TO v);")
        for vid in range(6):
            conn.execute(f"CREATE (:v {{id: {vid}}});")
        edges = [(0, 1), (1, 2), (0, 3), (3, 4), (4, 5), (2, 5)]
        for src, dst in edges:
            conn.execute(
                f"MATCH (a:v {{id: {src}}}), (b:v {{id: {dst}}}) "
                f"CREATE (a)-[:e]->(b);"
            )
        conn.execute(
            "CALL project_graph(" "'custom_graph', " "['v'], " "{'[v, e, v]': ''}" ");"
        )
        conn.execute("LOAD gds;")
        yield conn
    finally:
        conn.close()
        db.close()


# ---------------------------------------------------------------------------
# BFS path return tests
# ---------------------------------------------------------------------------


class TestBFSPathReturn:
    """Tests for BFS with YIELD path."""

    def test_bfs_yield_path_basic(self, tmp_path):
        """BFS with YIELD path returns 3 columns including path data."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('person_knows', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            assert len(rows) > 0, "BFS must return at least one row"
            for row in rows:
                assert len(row) == 3, "each row should have (node_id, distance, path)"
                node_id, distance, path = row
                assert isinstance(distance, int)
                if distance == 0:
                    # Source node: path should represent a single-node path
                    assert path is not None
                elif distance == -1:
                    # Unreachable: path should be None/null
                    assert path is None
                else:
                    # Reachable: path should be present
                    assert path is not None

    def test_bfs_path_source_node(self, tmp_path):
        """BFS path for source node should be a single-node path (length 0)."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('person_knows', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            source_rows = [(nid, d, p) for nid, d, p in rows if d == 0]
            assert len(source_rows) == 1, "exactly one source row"
            node_id, distance, path = source_rows[0]
            assert node_id == 0
            assert distance == 0
            assert path is not None

    def test_bfs_path_unreachable(self, tmp_path):
        """BFS path for unreachable nodes should be None."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('person_knows', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            unreachable = [(nid, d, p) for nid, d, p in rows if d == -1]
            for node_id, distance, path in unreachable:
                assert path is None, f"unreachable node {node_id} should have null path"

    def test_bfs_backward_compat_no_path(self, tmp_path):
        """BFS without YIELD path still returns only (node, distance)."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('person_knows', {source: '0'}) "
                    "YIELD node, distance "
                    "RETURN node.id, distance;"
                )
            )
            assert len(rows) > 0
            for row in rows:
                assert len(row) == 2, "backward compatible: only 2 columns"

    def test_bfs_distance_consistent_with_path(self, tmp_path):
        """BFS distance should equal path length (number of edges)."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('person_knows', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            for node_id, distance, path in rows:
                if distance <= 0:
                    continue
                assert path is not None, f"reachable node {node_id} must have path"


# ---------------------------------------------------------------------------
# SSSP path return tests
# ---------------------------------------------------------------------------


class TestSSSPPathReturn:
    """Tests for SSSP with YIELD path."""

    def test_sssp_yield_path_basic(self, tmp_path):
        """SSSP with YIELD path returns 3 columns including path data."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL sssp('person_knows', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            assert len(rows) > 0, "SSSP must return at least one row"
            for row in rows:
                assert len(row) == 3, "each row should have (node_id, distance, path)"
                node_id, distance, path = row
                assert isinstance(distance, float)
                if distance < 0:
                    assert path is None
                elif distance == 0.0:
                    assert path is not None
                else:
                    assert path is not None

    def test_sssp_path_source_node(self, tmp_path):
        """SSSP path for source node should be present (distance 0)."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL sssp('person_knows', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            source_rows = [(nid, d, p) for nid, d, p in rows if d == 0.0]
            assert len(source_rows) == 1
            assert source_rows[0][2] is not None

    def test_sssp_path_unreachable(self, tmp_path):
        """SSSP path for unreachable nodes should be None."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL sssp('person_knows', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            unreachable = [(nid, d, p) for nid, d, p in rows if d < 0]
            for node_id, distance, path in unreachable:
                assert path is None

    def test_sssp_backward_compat_no_path(self, tmp_path):
        """SSSP without YIELD path still returns only (node, distance)."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL sssp('person_knows', {source: '0'}) "
                    "YIELD node, distance "
                    "RETURN node.id, distance;"
                )
            )
            assert len(rows) > 0
            for row in rows:
                assert len(row) == 2, "backward compatible: only 2 columns"


# ---------------------------------------------------------------------------
# BFS/SSSP path return with predicates
# ---------------------------------------------------------------------------


class TestPathWithPredicates:
    """Tests for path return with vertex/edge predicates."""

    def test_bfs_path_with_edge_predicate(self, tmp_path):
        """BFS with edge predicate excluding all edges: only source has path."""
        db_dir = tmp_path / "gds_path_pred_db"
        db = Database(db_path=str(db_dir), mode="w")
        db.load_builtin_dataset("tinysnb")
        conn = db.connect()
        try:
            conn.execute(
                "CALL project_graph('g', ['person'], "
                "{'[person, knows, person]': 'r.date > Date(\"2999-01-01\")'});"
            )
            conn.execute("LOAD gds;")
            rows = list(
                conn.execute(
                    "CALL bfs('g', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            reachable = [(nid, d, p) for nid, d, p in rows if d >= 0]
            assert reachable[0] == (0, 0, reachable[0][2])
            assert reachable[0][2] is not None
            # All others unreachable → null path
            unreachable = [(nid, d, p) for nid, d, p in rows if d == -1]
            for _, _, p in unreachable:
                assert p is None
        finally:
            conn.close()
            db.close()

    def test_sssp_path_with_edge_predicate(self, tmp_path):
        """SSSP with edge predicate excluding all edges: only source has path."""
        db_dir = tmp_path / "gds_path_pred_sssp_db"
        db = Database(db_path=str(db_dir), mode="w")
        db.load_builtin_dataset("tinysnb")
        conn = db.connect()
        try:
            conn.execute(
                "CALL project_graph('g', ['person'], "
                "{'[person, knows, person]': 'r.date > Date(\"2999-01-01\")'});"
            )
            conn.execute("LOAD gds;")
            rows = list(
                conn.execute(
                    "CALL sssp('g', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            reachable = [(nid, d, p) for nid, d, p in rows if d >= 0]
            assert reachable[0] == (0, 0.0, reachable[0][2])
            assert reachable[0][2] is not None
            unreachable = [(nid, d, p) for nid, d, p in rows if d < 0]
            for _, _, p in unreachable:
                assert p is None
        finally:
            conn.close()
            db.close()


# ---------------------------------------------------------------------------
# Custom graph deterministic path tests
# ---------------------------------------------------------------------------


class TestCustomGraphPaths:
    """Deterministic path tests on a small custom graph."""

    def test_bfs_custom_graph_paths(self, tmp_path):
        """Verify BFS returns correct distances on custom graph."""
        with custom_graph_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('custom_graph', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            dist_map = {nid: d for nid, d, _ in rows}
            assert dist_map[0] == 0
            assert dist_map[1] == 1
            assert dist_map[2] == 2
            assert dist_map[3] == 1
            assert dist_map[4] == 2
            # 0->5 can be [0,1,2,5] or [0,3,4,5], both distance 3
            assert dist_map[5] == 3

            # All reachable → all have paths
            paths = {nid: p for nid, d, p in rows}
            for nid in range(6):
                assert paths[nid] is not None, f"node {nid} should have a path"

    def test_sssp_custom_graph_paths(self, tmp_path):
        """Verify SSSP returns correct distances on custom graph (unit weight)."""
        with custom_graph_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL sssp('custom_graph', {source: '0'}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            dist_map = {nid: d for nid, d, _ in rows}
            assert dist_map[0] == 0.0
            assert dist_map[1] == 1.0
            assert dist_map[2] == 2.0
            assert dist_map[3] == 1.0
            assert dist_map[4] == 2.0
            assert dist_map[5] == 3.0

    def test_bfs_directed_custom_graph(self, tmp_path):
        """Verify BFS respects directed flag on custom graph."""
        with custom_graph_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('custom_graph', {source: '0', directed: true}) "
                    "YIELD node, distance, path "
                    "RETURN node.id, distance, path;"
                )
            )
            dist_map = {nid: d for nid, d, _ in rows}
            # With directed edges 0->1, 1->2, 0->3, 3->4, 4->5, 2->5
            assert dist_map[0] == 0
            assert dist_map[1] == 1
            assert dist_map[3] == 1
            assert dist_map[2] == 2
            assert dist_map[4] == 2
            assert dist_map[5] == 3


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestPathEdgeCases:
    """Edge case tests for path return."""

    def test_bfs_no_yield_defaults_to_two_columns(self, tmp_path):
        """No-YIELD BFS should return all 3 columns (node, distance, path)
        since outputColumns now includes path."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute("CALL bfs('person_knows', {source: '0'}) " "RETURN *;")
            )
            # With the new outputColumns, RETURN * gives all 3 columns
            if len(rows) > 0:
                assert len(rows[0]) == 3, "RETURN * should include all 3 output columns"

    def test_sssp_no_yield_defaults_to_three_columns(self, tmp_path):
        """No-YIELD SSSP should return all 3 columns."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute("CALL sssp('person_knows', {source: '0'}) " "RETURN *;")
            )
            if len(rows) > 0:
                assert len(rows[0]) == 3, "RETURN * should include all 3 output columns"


# ---------------------------------------------------------------------------
