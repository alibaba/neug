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
# Tests for project_graph / drop_projected_graph and GDS CALL paths
# (see specs/004-gds). Prefer LIST literals for graph entries where the parser
# does not lower `{...}` struct literals consistently.

import os
import sys
from contextlib import contextmanager

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from conftest import ensure_result_cnt_gt_zero

from neug.database import Database


def _shown_projected_graph_names(conn):
    """Return graph names reported by CALL SHOW_PROJECTED_GRAPHS() RETURN *."""
    rows = list(conn.execute("CALL SHOW_PROJECTED_GRAPHS() RETURN *;"))
    return [row[0] for row in rows]


def _projected_graph_info_rows(conn, graph_name):
    """Rows from CALL PROJECTED_GRAPH_INFO(graph_name) RETURN * (label, predicate)."""
    escaped = graph_name.replace("\\", "\\\\").replace('"', '\\"')
    return list(
        conn.execute('CALL PROJECTED_GRAPH_INFO("{}") RETURN *;'.format(escaped))
    )


@contextmanager
def tinysnb_connection(tmp_path):
    """Open a writable DB with builtin tinysnb loaded; always closes conn + db."""
    db_dir = tmp_path / "gds_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        yield conn
    finally:
        conn.close()
        db.close()


def test_project_graph_and_drop_roundtrip(tmp_path):
    """Register a projected graph alias, then drop it (happy path)."""
    with tinysnb_connection(tmp_path) as conn:
        assert _shown_projected_graph_names(conn) == []

        conn.execute(
            "CALL project_graph("
            "'my_subgraph', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )
        names = _shown_projected_graph_names(conn)
        assert (
            "my_subgraph" in names
        ), "SHOW_PROJECTED_GRAPHS must list the registered alias"

        info_rows = _projected_graph_info_rows(conn, "my_subgraph")
        labels = {row[0] for row in info_rows}
        assert "person" in labels, "PROJECTED_GRAPH_INFO must expose node tables"
        assert "[person,knows,person]" in labels or any(
            "person" in lbl and "knows" in lbl for lbl in labels
        ), "PROJECTED_GRAPH_INFO must expose relationship triplets"

        conn.execute("CALL drop_projected_graph('my_subgraph');")
        assert "my_subgraph" not in _shown_projected_graph_names(
            conn
        ), "SHOW_PROJECTED_GRAPHS must not list a dropped alias"


def test_project_graph_with_predicates(tmp_path):
    """Project a graph with vertex and edge predicates."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph("
            "'my_subgraph', "
            "{'person': 'n.age > 20'}, "
            "{'[person, knows, person]': 'r.date > Date(\"2021-01-01\")'}"
            ");"
        )
        rows = _projected_graph_info_rows(conn, "my_subgraph")
        label_to_pred = {row[0]: row[1] for row in rows}
        assert label_to_pred.get("person") == "n.age > 20"
        rel_rows = [lbl for lbl in label_to_pred if "[" in lbl or "knows" in lbl]
        assert (
            len(rel_rows) >= 1
        ), "expected at least one relationship row in PROJECTED_GRAPH_INFO"
        assert any(
            ("2021" in label_to_pred[rel_lbl]) or ("Date" in label_to_pred[rel_lbl])
            for rel_lbl in rel_rows
        ), "relationship predicate must be preserved"

        conn.execute("CALL drop_projected_graph('my_subgraph');")
        assert "my_subgraph" not in _shown_projected_graph_names(conn)


def test_run_cdlp(tmp_path):
    """Load GDS extension and run cdlp on a projected subgraph."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL cdlp('person_knows', {concurrency: 10})
                YIELD node, label
                RETURN node.id, label;
                """
            )
        )
        assert len(rows) > 0, "cdlp must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (id, label)"
            assert isinstance(row[1], int), "label should be an integer"


@contextmanager
def tinysnb_simple_connection(tmp_path):
    """Open a writable DB with tinysnb loaded and a simple projected graph
    (single vertex label 'person', single edge label 'knows')."""
    db_dir = tmp_path / "gds_simple_db"
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


def test_run_louvain(tmp_path):
    """Run louvain community detection on a simple projected graph."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL louvain('person_knows', {concurrency: 4})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows) > 0, "louvain must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (node_id, community)"
            assert isinstance(row[1], int), "community should be an integer"


def test_run_leiden(tmp_path):
    """Run leiden community detection on a simple projected graph."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL leiden('person_knows', {concurrency: 4})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows) > 0, "leiden must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (node_id, community)"
            assert isinstance(row[1], int), "community should be an integer"


def test_run_louvain_with_resolution(tmp_path):
    """Run louvain with custom resolution parameter."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows_low = list(
            conn.execute(
                """
                CALL louvain('person_knows', {resolution: 0.1, concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        rows_high = list(
            conn.execute(
                """
                CALL louvain('person_knows', {resolution: 10.0, concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_low) > 0
        assert len(rows_high) > 0
        low_communities = {row[1] for row in rows_low}
        high_communities = {row[1] for row in rows_high}
        assert len(high_communities) >= len(
            low_communities
        ), "Higher resolution should produce >= communities"


def test_run_leiden_with_resolution(tmp_path):
    """Run leiden with custom resolution parameter."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL leiden('person_knows', {resolution: 1.0, concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows) > 0
        communities = {row[1] for row in rows}
        assert len(communities) >= 1, "leiden should detect at least one community"


def test_run_bfs(tmp_path):
    """Run BFS on a simple projected graph."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL bfs('person_knows', {source: '0'})
                YIELD node, distance
                RETURN node.id, distance;
                """
            )
        )
        assert len(rows) > 0, "bfs must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (node_id, distance)"
            assert isinstance(row[1], int), "distance should be an integer"
            # -1 indicates unreachable from source
            assert row[1] >= -1, "distance should be >= -1 (-1 = unreachable)"


def test_run_sssp(tmp_path):
    """Run SSSP on a simple projected graph (unit weights)."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL sssp('person_knows', {source: '0'})
                YIELD node, distance
                RETURN node.id, distance;
                """
            )
        )
        assert len(rows) > 0, "sssp must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (node_id, distance)"
            assert isinstance(row[1], float), "distance should be a float"
            # -1.0 indicates unreachable from source
            assert row[1] >= -1.0, "distance should be >= -1.0 (-1.0 = unreachable)"


def test_run_wcc(tmp_path):
    """Run WCC on a simple projected graph."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL wcc('person_knows', {concurrency: 2})
                YIELD node, comp
                RETURN node.id, comp;
                """
            )
        )
        assert len(rows) > 0, "wcc must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (node_id, comp)"
            assert isinstance(row[1], int), "comp should be an integer"


def test_run_lcc(tmp_path):
    """Run LCC on a simple projected graph."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL lcc('person_knows', {concurrency: 2})
                YIELD node, lcc
                RETURN node.id, lcc;
                """
            )
        )
        assert len(rows) > 0, "lcc must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (node_id, lcc)"
            assert isinstance(row[1], float), "lcc should be a float"
            assert 0.0 <= row[1] <= 1.0, "lcc should be between 0 and 1"


def test_run_kcore(tmp_path):
    """Run K-Core decomposition on a simple projected graph."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL kcore('person_knows', {k: 1})
                YIELD node, core
                RETURN node.id, core;
                """
            )
        )
        assert len(rows) > 0, "kcore must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (node_id, core)"
            assert isinstance(row[1], int), "core should be an integer"
            # -1 indicates a node whose core number is below the k threshold
            assert row[1] >= -1, "core should be >= -1 (-1 = below threshold)"


def test_cdlp_with_edge_predicate(tmp_path):
    """CDLP propagates labels only along edges satisfying the edge predicate.
    A predicate that excludes every edge leaves each vertex in its own
    community (no propagation), which also guards against silently ignoring
    the predicate."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph('g', ['person'], "
            "{'[person, knows, person]': 'r.date > Date(\"2999-01-01\")'});"
        )
        conn.execute("LOAD gds;")
        rows = list(
            conn.execute(
                "CALL cdlp('g', {max_iterations: 5}) "
                "YIELD node, label RETURN node.id, label;"
            )
        )
        assert len(rows) > 0
        labels = [r[1] for r in rows]
        # No edge survives the predicate, so no labels propagate and every
        # vertex keeps its own initial (distinct) community.
        assert len(set(labels)) == len(rows)


def test_wcc_with_edge_predicate(tmp_path):
    """WCC honors an edge predicate: excluding every edge leaves each vertex
    in its own component."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph('g', ['person'], "
            "{'[person, knows, person]': 'r.date > Date(\"2999-01-01\")'});"
        )
        conn.execute("LOAD gds;")
        rows = list(
            conn.execute(
                "CALL wcc('g', {concurrency: 1}) YIELD node, comp "
                "RETURN node.id, comp;"
            )
        )
        assert len(rows) > 0
        comps = [r[1] for r in rows]
        assert len(set(comps)) == len(rows)


def test_wcc_with_vertex_predicate(tmp_path):
    """WCC with a vertex predicate restricts the output to subgraph vertices."""
    with tinysnb_connection(tmp_path) as conn:
        expected = list(conn.execute("MATCH (p:person) WHERE p.age > 20 RETURN p.id;"))
        conn.execute(
            "CALL project_graph('g', {'person': 'n.age > 20'}, "
            "{'[person, knows, person]': ''});"
        )
        conn.execute("LOAD gds;")
        rows = list(
            conn.execute(
                "CALL wcc('g', {concurrency: 1}) YIELD node, comp "
                "RETURN node.id, comp;"
            )
        )
        assert len(rows) == len(expected)


def test_bfs_with_edge_predicate(tmp_path):
    """BFS honors an edge predicate: excluding every edge leaves only the
    source reachable."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph('g', ['person'], "
            "{'[person, knows, person]': 'r.date > Date(\"2999-01-01\")'});"
        )
        conn.execute("LOAD gds;")
        rows = list(
            conn.execute(
                "CALL bfs('g', {source: '0'}) YIELD node, distance "
                "RETURN node.id, distance;"
            )
        )
        assert len(rows) > 0
        reachable = [(nid, d) for nid, d in rows if d >= 0]
        assert reachable == [(0, 0)]


def test_sssp_with_edge_predicate(tmp_path):
    """SSSP honors an edge predicate: excluding every edge leaves only the
    source reachable at distance 0."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph('g', ['person'], "
            "{'[person, knows, person]': 'r.date > Date(\"2999-01-01\")'});"
        )
        conn.execute("LOAD gds;")
        rows = list(
            conn.execute(
                "CALL sssp('g', {source: '0'}) YIELD node, distance "
                "RETURN node.id, distance;"
            )
        )
        assert len(rows) > 0
        reachable = [(nid, d) for nid, d in rows if d >= 0]
        assert reachable == [(0, 0.0)]


def test_page_rank_with_vertex_predicate(tmp_path):
    """PageRank with a vertex predicate restricts the output to subgraph
    vertices and produces a valid (normalized) distribution over them."""
    with tinysnb_connection(tmp_path) as conn:
        expected = list(conn.execute("MATCH (p:person) WHERE p.age > 20 RETURN p.id;"))
        conn.execute(
            "CALL project_graph('g', {'person': 'n.age > 20'}, "
            "{'[person, knows, person]': ''});"
        )
        conn.execute("LOAD gds;")
        rows = list(
            conn.execute(
                "CALL page_rank('g', {max_iterations: 20}) "
                "YIELD node, rank RETURN node.id, rank;"
            )
        )
        assert len(rows) == len(expected)
        assert abs(sum(r[1] for r in rows) - 1.0) < 1e-6


def test_kcore_with_edge_predicate(tmp_path):
    """KCore honors an edge predicate: excluding every edge drops every degree
    to 0, so all vertices fall below k and report core -1."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph('g', ['person'], "
            "{'[person, knows, person]': 'r.date > Date(\"2999-01-01\")'});"
        )
        conn.execute("LOAD gds;")
        rows = list(
            conn.execute(
                "CALL kcore('g', {k: 1}) YIELD node, core " "RETURN node.id, core;"
            )
        )
        assert len(rows) > 0
        assert all(core == -1 for _, core in rows)


def test_lcc_with_edge_predicate(tmp_path):
    """LCC honors an edge predicate: excluding every edge leaves every vertex
    with no neighbors and a coefficient of 0."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph('g', ['person'], "
            "{'[person, knows, person]': 'r.date > Date(\"2999-01-01\")'});"
        )
        conn.execute("LOAD gds;")
        rows = list(
            conn.execute(
                "CALL lcc('g', {concurrency: 1}) YIELD node, lcc "
                "RETURN node.id, lcc;"
            )
        )
        assert len(rows) > 0
        assert all(value == 0.0 for _, value in rows)


# ---------------------------------------------------------------------------
# Boundary condition & stability tests
# ---------------------------------------------------------------------------


@contextmanager
def custom_graph_connection(
    tmp_path,
    db_name,
    create_node_ddl,
    create_rel_ddl,
    node_inserts,
    edge_inserts,
    graph_name,
    vertex_entries,
    edge_entries,
):
    """Helper: create a DB, define schema, insert data, project a graph, load GDS.

    Yields the connection.  Always closes conn + db on exit.
    """
    db_dir = tmp_path / db_name
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    try:
        conn.execute(create_node_ddl)
        conn.execute(create_rel_ddl)
        for stmt in node_inserts:
            conn.execute(stmt)
        for stmt in edge_inserts:
            conn.execute(stmt)
        conn.execute(
            "CALL project_graph("
            "'{}', {}, {});".format(graph_name, vertex_entries, edge_entries)
        )
        conn.execute("LOAD gds;")
        yield conn
    finally:
        conn.close()
        db.close()


# ---- (a) Small graph: 2 nodes, 1 edge -- all algorithms ------------------


class TestSmallGraph:
    """Create a minimal graph (2 nodes, 1 edge) and run every available
    GDS algorithm to verify it does not crash and returns non-empty results."""

    def _small_graph_ctx(self, tmp_path):
        return custom_graph_connection(
            tmp_path,
            db_name="small_graph_db",
            create_node_ddl="CREATE NODE TABLE n(id INT64 PRIMARY KEY);",
            create_rel_ddl="CREATE REL TABLE e(FROM n TO n);",
            node_inserts=[
                "CREATE (:n {id: 1});",
                "CREATE (:n {id: 2});",
            ],
            edge_inserts=[
                "MATCH (a:n), (b:n) WHERE a.id = 1 AND b.id = 2"
                " CREATE (a)-[:e]->(b);",
            ],
            graph_name="g",
            vertex_entries="['n']",
            edge_entries="{'[n, e, n]': ''}",
        )

    def test_wcc_small(self, tmp_path):
        """WCC on a 2-node, 1-edge graph should produce 2 rows in 1 component."""
        with self._small_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL wcc('g', {concurrency: 1})"
                    " YIELD node, comp RETURN node.id, comp;"
                )
            )
            assert len(rows) == 2, "WCC on 2-node graph should return 2 rows"
            comps = {r[1] for r in rows}
            assert len(comps) == 1, "Both nodes should be in the same component"

    def test_bfs_small(self, tmp_path):
        """BFS from node 1 in a 2-node graph should find the source at dist 0."""
        with self._small_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('g', {source: '1'})"
                    " YIELD node, distance RETURN node.id, distance;"
                )
            )
            assert len(rows) >= 1, "BFS should return at least the source node"
            dist_map = {r[0]: r[1] for r in rows}
            assert dist_map.get(1) == 0, "Source node distance should be 0"

    def test_page_rank_small(self, tmp_path):
        """PageRank on a 2-node graph should produce positive ranks for both."""
        with self._small_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL page_rank('g', {max_iterations: 10, concurrency: 1})"
                    " YIELD node, rank RETURN node.id, rank;"
                )
            )
            assert len(rows) == 2, "PageRank should return 2 rows"
            for row in rows:
                assert isinstance(row[1], float), "rank should be a float"
                assert row[1] > 0, "rank should be positive"

    def test_lcc_small(self, tmp_path):
        """LCC on a 2-node graph should return 2 rows."""
        with self._small_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL lcc('g', {concurrency: 1})"
                    " YIELD node, lcc RETURN node.id, lcc;"
                )
            )
            assert len(rows) == 2, "LCC should return 2 rows"

    def test_louvain_small(self, tmp_path):
        """Louvain on a 2-node graph should assign community labels to both."""
        with self._small_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL louvain('g', {concurrency: 1})"
                    " YIELD node, community RETURN node.id, community;"
                )
            )
            assert len(rows) == 2, "Louvain should return 2 rows"
            for row in rows:
                assert isinstance(row[1], int), "community should be an integer"

    def test_leiden_small(self, tmp_path):
        """Leiden on a 2-node graph should assign community labels to both."""
        with self._small_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL leiden('g', {concurrency: 1})"
                    " YIELD node, community RETURN node.id, community;"
                )
            )
            assert len(rows) == 2, "Leiden should return 2 rows"
            for row in rows:
                assert isinstance(row[1], int), "community should be an integer"

    def test_cdlp_small(self, tmp_path):
        """CDLP (Community Detection using Label Propagation) on a small graph."""
        with self._small_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL cdlp('g', {max_iterations: 5}) "
                    "YIELD node, label RETURN node.id, label;"
                )
            )
            assert len(rows) == 2, "CDLP should return 2 rows"
            for row in rows:
                assert isinstance(row[1], int), "label should be an integer"


# ---- (b) BFS with non-existent source ------------------------------------


class TestBFSNonExistentSource:
    """Calling BFS or SSSP with a source vertex that does not exist should log
    an error and return an empty result, not crash the database."""

    def test_bfs_missing_source_custom_graph(self, tmp_path):
        """BFS with source 999999 on a tiny custom graph returns no rows."""
        with custom_graph_connection(
            tmp_path,
            db_name="bfs_missing_src_db",
            create_node_ddl="CREATE NODE TABLE n(id INT64 PRIMARY KEY);",
            create_rel_ddl="CREATE REL TABLE e(FROM n TO n);",
            node_inserts=[
                "CREATE (:n {id: 1});",
                "CREATE (:n {id: 2});",
            ],
            edge_inserts=[
                "MATCH (a:n), (b:n) WHERE a.id = 1 AND b.id = 2"
                " CREATE (a)-[:e]->(b);",
            ],
            graph_name="g",
            vertex_entries="['n']",
            edge_entries="{'[n, e, n]': ''}",
        ) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('g', {source: '999999'})"
                    " YIELD node, distance RETURN node.id, distance;"
                )
            )
            assert rows == [], "missing BFS source should yield no rows"

    def test_bfs_missing_source_tinysnb(self, tmp_path):
        """BFS with source 999999 on tinysnb returns no rows, not crash."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('person_knows', {source: '999999'})"
                    " YIELD node, distance RETURN node.id, distance;"
                )
            )
            assert rows == [], "missing BFS source should yield no rows"

    def test_sssp_missing_source_tinysnb(self, tmp_path):
        """SSSP with source 999999 on tinysnb returns no rows, not crash."""
        with tinysnb_simple_connection(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL sssp('person_knows', {source: '999999'})"
                    " YIELD node, distance RETURN node.id, distance;"
                )
            )
            assert rows == [], "missing SSSP source should yield no rows"


# ---- (c) Empty graph: nodes only, no edges --------------------------------


class TestEmptyGraph:
    """A graph with nodes but zero edges should not crash.  WCC should assign
    each node its own component; PageRank should converge to a uniform
    distribution."""

    def _empty_graph_ctx(self, tmp_path):
        return custom_graph_connection(
            tmp_path,
            db_name="empty_graph_db",
            create_node_ddl="CREATE NODE TABLE n(id INT64 PRIMARY KEY);",
            create_rel_ddl="CREATE REL TABLE e(FROM n TO n);",
            node_inserts=[
                "CREATE (:n {id: 1});",
                "CREATE (:n {id: 2});",
                "CREATE (:n {id: 3});",
            ],
            edge_inserts=[],  # no edges at all
            graph_name="g",
            vertex_entries="['n']",
            edge_entries="{'[n, e, n]': ''}",
        )

    def test_wcc_empty_graph(self, tmp_path):
        """Each isolated node should form its own component."""
        with self._empty_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL wcc('g', {concurrency: 1})"
                    " YIELD node, comp RETURN node.id, comp;"
                )
            )
            assert len(rows) == 3, "WCC should return one row per node"
            comps = [r[1] for r in rows]
            assert (
                len(set(comps)) == 3
            ), "With no edges, every node should be in its own component"

    def test_page_rank_empty_graph(self, tmp_path):
        """PageRank on an edgeless graph should converge to uniform distribution."""
        with self._empty_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL page_rank('g', {max_iterations: 20, concurrency: 1})"
                    " YIELD node, rank RETURN node.id, rank;"
                )
            )
            assert len(rows) == 3, "PageRank should return 3 rows"
            ranks = [r[1] for r in rows]
            expected = 1.0 / 3.0
            for rank in ranks:
                assert abs(rank - expected) < 0.05, (
                    "PageRank on edgeless graph should be ~{:.3f},"
                    " got {:.4f}".format(expected, rank)
                )


# ---- (d) Self-loop graph --------------------------------------------------


class TestSelfLoopGraph:
    """A graph containing self-loop edges should not cause infinite loops
    or crashes in WCC or BFS."""

    def _selfloop_graph_ctx(self, tmp_path):
        return custom_graph_connection(
            tmp_path,
            db_name="selfloop_graph_db",
            create_node_ddl="CREATE NODE TABLE n(id INT64 PRIMARY KEY);",
            create_rel_ddl="CREATE REL TABLE e(FROM n TO n);",
            node_inserts=[
                "CREATE (:n {id: 1});",
                "CREATE (:n {id: 2});",
                "CREATE (:n {id: 3});",
            ],
            edge_inserts=[
                # A->A (self-loop), A->B, B->C
                "MATCH (a:n) WHERE a.id = 1 CREATE (a)-[:e]->(a);",
                "MATCH (a:n), (b:n) WHERE a.id = 1 AND b.id = 2"
                " CREATE (a)-[:e]->(b);",
                "MATCH (a:n), (b:n) WHERE a.id = 2 AND b.id = 3"
                " CREATE (a)-[:e]->(b);",
            ],
            graph_name="g",
            vertex_entries="['n']",
            edge_entries="{'[n, e, n]': ''}",
        )

    def test_wcc_selfloop(self, tmp_path):
        """WCC should complete without hanging on a graph with self-loops."""
        with self._selfloop_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL wcc('g', {concurrency: 1})"
                    " YIELD node, comp RETURN node.id, comp;"
                )
            )
            assert len(rows) == 3, "WCC should return 3 rows"
            comps = {r[1] for r in rows}
            assert len(comps) == 1, (
                "All 3 nodes connected (ignoring self-loop) should be"
                " in one component"
            )

    def test_bfs_selfloop(self, tmp_path):
        """BFS should complete without hanging on a graph with self-loops."""
        with self._selfloop_graph_ctx(tmp_path) as conn:
            rows = list(
                conn.execute(
                    "CALL bfs('g', {source: '1'})"
                    " YIELD node, distance RETURN node.id, distance;"
                )
            )
            assert len(rows) >= 1, "BFS should return at least the source"
            dist_map = {r[0]: r[1] for r in rows}
            assert dist_map.get(1) == 0, "Source node should have distance 0"
            # Node 2 should be reachable at distance 1, node 3 at distance 2
            if 2 in dist_map:
                assert dist_map[2] == 1, "Node 2 should be at distance 1 from source"
            if 3 in dist_map:
                assert dist_map[3] == 2, "Node 3 should be at distance 2 from source"


# ---- (e) Large graph cross-validation with tinysnb ------------------------


class TestCrossValidationTinysnb:
    """Cross-validate algorithm results on the tinysnb dataset:
    - WCC: nodes in the same connected component must share the same comp value
    - BFS: distances should be non-decreasing when ordered by distance
    """

    def test_wcc_consistency(self, tmp_path):
        """Nodes connected by an edge must share the same WCC component."""
        with tinysnb_simple_connection(tmp_path) as conn:
            # Run WCC
            wcc_rows = list(
                conn.execute(
                    "CALL wcc('person_knows', {concurrency: 1})"
                    " YIELD node, comp RETURN node.id, comp;"
                )
            )
            assert len(wcc_rows) > 0, "WCC must return results"

            # Build node -> comp mapping
            node_comp = {r[0]: r[1] for r in wcc_rows}

            # Query actual edges and verify connected nodes share component
            edge_rows = list(
                conn.execute(
                    "MATCH (a:person)-[:knows]->(b:person)" " RETURN a.id, b.id;"
                )
            )
            for src_id, dst_id in edge_rows:
                if src_id in node_comp and dst_id in node_comp:
                    assert node_comp[src_id] == node_comp[dst_id], (
                        "Connected nodes {} and {} should have the same WCC "
                        "component, got {} vs {}".format(
                            src_id,
                            dst_id,
                            node_comp[src_id],
                            node_comp[dst_id],
                        )
                    )

    def test_bfs_distance_non_decreasing(self, tmp_path):
        """BFS distances from a source should be non-decreasing among
        reachable nodes.  Nodes with distance == -1 are unreachable and
        are excluded from the ordering checks."""
        with tinysnb_simple_connection(tmp_path) as conn:
            # Pick the first person as source
            source_rows = list(
                conn.execute("MATCH (p:person) RETURN p.id ORDER BY p.id LIMIT 1;")
            )
            assert len(source_rows) > 0, "tinysnb should have at least one person"
            source_id = str(source_rows[0][0])

            bfs_rows = list(
                conn.execute(
                    "CALL bfs('person_knows', {{source: '{}'}})"
                    " YIELD node, distance"
                    " RETURN node.id, distance"
                    " ORDER BY distance;".format(source_id)
                )
            )
            assert len(bfs_rows) > 0, "BFS should return at least the source"

            # Separate reachable (distance >= 0) from unreachable (-1)
            reachable = [(nid, d) for nid, d in bfs_rows if d >= 0]
            unreachable = [(nid, d) for nid, d in bfs_rows if d < 0]
            assert (
                len(reachable) >= 1
            ), "BFS must return at least the source as reachable"

            # The source should have distance 0
            source_row = [r for r in reachable if r[0] == int(source_id)]
            assert len(source_row) == 1, "Source node must appear in results"
            assert source_row[0][1] == 0, "Source node distance should be 0"

            # Reachable distances must be non-decreasing (already ordered)
            prev_dist = -1
            for node_id, dist in reachable:
                assert dist > prev_dist or dist == prev_dist, (
                    "BFS distances should be non-decreasing,"
                    " but got {} after {}".format(dist, prev_dist)
                )
                prev_dist = dist

            # Each distance step should be at most +1 from the previous
            # (BFS explores level by level)
            dist_set = sorted({d for _, d in reachable})
            for i in range(1, len(dist_set)):
                assert dist_set[i] - dist_set[i - 1] == 1, (
                    "BFS distance levels should be contiguous,"
                    " gap between {} and {}".format(dist_set[i - 1], dist_set[i])
                )

            # All unreachable nodes should report distance -1
            for node_id, dist in unreachable:
                assert (
                    dist == -1
                ), "Unreachable node {} should have distance -1," " got {}".format(
                    node_id, dist
                )
