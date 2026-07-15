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


# ─── Multi-Edge & Multi-Label Leiden / Louvain ─────────────────────────────────


def _batch_writeback(conn, prop_name, comm_map):
    """Batch-write community IDs to vertex property using a single CASE WHEN query."""
    if not comm_map:
        return
    when_clauses = " ".join(
        f"WHEN n.id = {nid} THEN {comm}" for nid, comm in comm_map.items()
    )
    conn.execute(
        f"MATCH (n:person) "
        f"SET n.{prop_name} = CASE {when_clauses} ELSE n.{prop_name} END;"
    )


@contextmanager
def multi_edge_connection(tmp_path):
    """Open DB with tinysnb loaded and a multi-edge projected graph
    (single vertex label 'person', two edge labels 'knows' + 'meets')."""
    db_dir = tmp_path / "gds_multi_edge_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute(
            "CALL project_graph("
            "'person_multi', "
            "['person'], "
            "{'[person, knows, person]': '', '[person, meets, person]': ''}"
            ");"
        )
        conn.execute("LOAD gds;")
        yield conn
    finally:
        conn.close()
        db.close()


def test_leiden_basic(tmp_path):
    """leiden on a single-label graph (backward compat)."""
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


def test_louvain_basic(tmp_path):
    """louvain on a single-label graph (backward compat)."""
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
            assert len(row) == 2
            assert isinstance(row[1], int)


def test_leiden_multi_edge(tmp_path):
    """leiden on a graph with two edge types (knows + meets).
    tinysnb person vertices: 0,2,3,5,7,8,9,10.
    knows forms a K4 on {0,2,3,5} + star 7→{8,9};
    meets adds 0→2,2→5,3↔7,8→3,9→3,10→2."""
    with multi_edge_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL leiden('person_multi', {concurrency: 4})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        comm = {row[0]: row[1] for row in rows}
        # All 8 person vertices present
        assert set(comm.keys()) == {0, 2, 3, 5, 7, 8, 9, 10}, f"comm={comm}"
        # All community IDs non-negative
        assert all(c >= 0 for c in comm.values()), f"comm={comm}"
        # >= 2 communities (graph has two clearly separated clusters)
        assert len(set(comm.values())) >= 2, f"comm={comm}"
        # Not all singletons — at least one community has >= 2 members
        assert len(comm) > len(set(comm.values())), f"All singletons: {comm}"


def test_leiden_multi_edge_deterministic(tmp_path):
    """Running leiden twice with same multi-edge data gives same result."""
    with multi_edge_connection(tmp_path) as conn:
        rows1 = list(
            conn.execute(
                """
                CALL leiden('person_multi', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        rows2 = list(
            conn.execute(
                """
                CALL leiden('person_multi', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        # Sort by node id for comparison
        rows1.sort(key=lambda r: r[0])
        rows2.sort(key=lambda r: r[0])
        assert rows1 == rows2, "Same input with fixed seed should give same output"


def test_leiden_deterministic_simple(tmp_path):
    """Deterministic: run twice with same simple-graph data, verify identical results."""
    db_dir = tmp_path / "gds_incremental_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")

        # Project and run first time
        conn.execute(
            "CALL project_graph("
            "'g1', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )

        rows_r1 = list(
            conn.execute(
                """
                CALL leiden('g1', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_r1) > 0, "First run should produce results"
        r1_map = {row[0]: row[1] for row in rows_r1}

        # Run second time with same data (no initial_community_property)
        # Results should be identical due to fixed seed
        rows_r2 = list(
            conn.execute(
                """
                CALL leiden('g1', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        assert r1_map == r2_map, "Deterministic: same data should yield same result"

    finally:
        conn.close()
        db.close()


def test_leiden_incremental_with_writeback(tmp_path):
    """Incremental Leiden stability: run once, write back communities, run again
    with initial_community_property, verify results are identical."""
    db_dir = tmp_path / "gds_leiden_wb_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")

        # Step 1: Add a community property column to person table
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")

        # Step 2: Project graph
        conn.execute(
            "CALL project_graph("
            "'g_leiden', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )

        # Step 3: First run - get initial communities
        rows_r1 = list(
            conn.execute(
                """
                CALL leiden('g_leiden', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_r1) > 0, "First run should produce results"
        r1_map = {row[0]: row[1] for row in rows_r1}

        # Step 4: Write back communities to vertex property (batch UNWIND)
        _batch_writeback(conn, "leiden_comm", r1_map)

        # Step 5: Drop and re-project graph to pick up updated properties
        conn.execute("CALL drop_projected_graph('g_leiden');")
        conn.execute(
            "CALL project_graph("
            "'g_leiden', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )

        # Step 6: Second run with initial_community_property
        rows_r2 = list(
            conn.execute(
                """
                CALL leiden('g_leiden',
                    {concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_r2) > 0, "Second run should produce results"
        r2_map = {row[0]: row[1] for row in rows_r2}

        # Step 7: Verify stability - communities should be identical
        assert r1_map == r2_map, (
            f"Incremental Leiden should be stable when data is unchanged.\n"
            f"First run:  {r1_map}\n"
            f"Second run: {r2_map}"
        )

    finally:
        conn.close()
        db.close()


def test_louvain_incremental_with_writeback(tmp_path):
    """Incremental Louvain stability: run once, write back communities, run again
    with initial_community_property, verify results are identical."""
    db_dir = tmp_path / "gds_louvain_wb_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")

        # Step 1: Add a community property column to person table
        conn.execute("ALTER TABLE person ADD louvain_comm INT64;")

        # Step 2: Project graph
        conn.execute(
            "CALL project_graph("
            "'g_louvain', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )

        # Step 3: First run - get initial communities
        rows_r1 = list(
            conn.execute(
                """
                CALL louvain('g_louvain', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_r1) > 0, "First run should produce results"
        r1_map = {row[0]: row[1] for row in rows_r1}

        # Step 4: Write back communities to vertex property (batch UNWIND)
        _batch_writeback(conn, "louvain_comm", r1_map)

        # Step 5: Drop and re-project graph to pick up updated properties
        conn.execute("CALL drop_projected_graph('g_louvain');")
        conn.execute(
            "CALL project_graph("
            "'g_louvain', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )

        # Step 6: Second run with initial_community_property
        rows_r2 = list(
            conn.execute(
                """
                CALL louvain('g_louvain',
                    {concurrency: 1, initial_community_property: 'louvain_comm', allow_relocation: true})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_r2) > 0, "Second run should produce results"
        r2_map = {row[0]: row[1] for row in rows_r2}

        # Step 7: Verify stability - communities should be identical
        assert r1_map == r2_map, (
            f"Incremental Louvain should be stable when data is unchanged.\n"
            f"First run:  {r1_map}\n"
            f"Second run: {r2_map}"
        )

    finally:
        conn.close()
        db.close()


def test_leiden_multi_edge_alias(tmp_path):
    """leiden on a multi-edge graph (knows + meets) — same as test_leiden_multi_edge
    but kept as a separate test entry point for regression tracking."""
    with multi_edge_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL leiden('person_multi', {concurrency: 4})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        comm = {row[0]: row[1] for row in rows}
        assert set(comm.keys()) == {0, 2, 3, 5, 7, 8, 9, 10}, f"comm={comm}"
        assert all(c >= 0 for c in comm.values()), f"comm={comm}"
        assert len(set(comm.values())) >= 2, f"comm={comm}"
        assert len(comm) > len(set(comm.values())), f"All singletons: {comm}"


def test_louvain_multi_edge_alias(tmp_path):
    """louvain on a multi-edge graph (knows + meets).
    Same graph as test_leiden_multi_edge but using louvain algorithm."""
    with multi_edge_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL louvain('person_multi', {concurrency: 4})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        comm = {row[0]: row[1] for row in rows}
        assert set(comm.keys()) == {0, 2, 3, 5, 7, 8, 9, 10}, f"comm={comm}"
        assert all(c >= 0 for c in comm.values()), f"comm={comm}"
        assert len(set(comm.values())) >= 2, f"comm={comm}"
        assert len(comm) > len(set(comm.values())), f"All singletons: {comm}"


def test_leiden_incremental_alias(tmp_path):
    """leiden alias supports initial_community_property for incremental runs;
    stable when data is unchanged."""
    db_dir = tmp_path / "gds_leiden_alias_wb_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")
        conn.execute(
            "CALL project_graph("
            "'g_leiden_alias', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )
        rows_r1 = list(
            conn.execute(
                """
                CALL leiden('g_leiden_alias', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_r1) > 0, "First run should produce results"
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "leiden_comm", r1_map)
        conn.execute("CALL drop_projected_graph('g_leiden_alias');")
        conn.execute(
            "CALL project_graph("
            "'g_leiden_alias', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )
        rows_r2 = list(
            conn.execute(
                """
                CALL leiden('g_leiden_alias',
                    {concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_r2) > 0, "Second run should produce results"
        r2_map = {row[0]: row[1] for row in rows_r2}
        assert r1_map == r2_map, (
            f"Incremental leiden alias should be stable when data unchanged.\n"
            f"First run:  {r1_map}\n"
            f"Second run: {r2_map}"
        )
    finally:
        conn.close()
        db.close()


def test_louvain_incremental_alias(tmp_path):
    """louvain alias supports initial_community_property for incremental runs;
    stable when data is unchanged."""
    db_dir = tmp_path / "gds_louvain_alias_wb_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD louvain_comm INT64;")
        conn.execute(
            "CALL project_graph("
            "'g_louvain_alias', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )
        rows_r1 = list(
            conn.execute(
                """
                CALL louvain('g_louvain_alias', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_r1) > 0, "First run should produce results"
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "louvain_comm", r1_map)
        conn.execute("CALL drop_projected_graph('g_louvain_alias');")
        conn.execute(
            "CALL project_graph("
            "'g_louvain_alias', "
            "['person'], "
            "{'[person, knows, person]': ''}"
            ");"
        )
        rows_r2 = list(
            conn.execute(
                """
                CALL louvain('g_louvain_alias',
                    {concurrency: 1, initial_community_property: 'louvain_comm', allow_relocation: true})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows_r2) > 0, "Second run should produce results"
        r2_map = {row[0]: row[1] for row in rows_r2}
        assert r1_map == r2_map, (
            f"Incremental louvain alias should be stable when data unchanged.\n"
            f"First run:  {r1_map}\n"
            f"Second run: {r2_map}"
        )
    finally:
        conn.close()
        db.close()


# ─── Multi-Vertex-Label & Data-Change Tests ─────────────────────────────────


@contextmanager
def multi_vertex_label_connection(tmp_path):
    """Open DB with tinysnb loaded and a multi-vertex-label projected graph
    (person + organisation vertices, knows + studyAt edges)."""
    db_dir = tmp_path / "gds_multi_vlabel_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute(
            "CALL project_graph("
            "'person_org', "
            "['person', 'organisation'], "
            "{'[person, knows, person]': '', '[person, studyAt, organisation]': ''}"
            ");"
        )
        conn.execute("LOAD gds;")
        yield conn
    finally:
        conn.close()
        db.close()


def test_leiden_two_vertex_labels(tmp_path):
    """Leiden on person+organisation graph (knows+studyAt edges).
    11 vertices: person 0,2,3,5,7,8,9,10 + organisation 1,4,6.
    studyAt: 0→1, 2→1, 8→1 (all to org #1)."""
    with multi_vertex_label_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL leiden('person_org', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        comm = {row[0]: row[1] for row in rows}
        # All 11 vertices present (8 person + 3 organisation)
        assert set(comm.keys()) == {0, 2, 3, 5, 7, 8, 9, 10, 1, 4, 6}, f"comm={comm}"
        assert all(c >= 0 for c in comm.values()), f"comm={comm}"
        assert len(set(comm.values())) >= 2, f"comm={comm}"
        assert len(comm) > len(set(comm.values())), f"All singletons: {comm}"
        # Vertex 0, 2, 8 all studyAt org 1 — at least two should share a community
        study_vertices = [0, 2, 8, 1]
        study_comms = [comm.get(v) for v in study_vertices if v in comm]
        assert len(set(study_comms)) < len(
            study_comms
        ), f"studyAt-connected vertices should share a community: {comm}"


def test_louvain_two_vertex_labels(tmp_path):
    """Louvain on person+organisation graph (knows+studyAt edges).
    Same graph as test_leiden_two_vertex_labels but using louvain."""
    with multi_vertex_label_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL louvain('person_org', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        comm = {row[0]: row[1] for row in rows}
        assert set(comm.keys()) == {0, 2, 3, 5, 7, 8, 9, 10, 1, 4, 6}, f"comm={comm}"
        assert all(c >= 0 for c in comm.values()), f"comm={comm}"
        assert len(set(comm.values())) >= 2, f"comm={comm}"
        assert len(comm) > len(set(comm.values())), f"All singletons: {comm}"
        # Vertex 0, 2, 8 all studyAt org 1 — at least two should share a community
        study_vertices = [0, 2, 8, 1]
        study_comms = [comm.get(v) for v in study_vertices if v in comm]
        assert len(set(study_comms)) < len(
            study_comms
        ), f"studyAt-connected vertices should share a community: {comm}"


def test_leiden_incremental_data_changed(tmp_path):
    """Incremental Leiden with data changes: unchanged communities should
    inherit their old IDs via majority-vote mapping."""
    db_dir = tmp_path / "gds_leiden_data_change_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "leiden_comm", r1_map)
        conn.execute(
            "MATCH (a:person {id: 0}), (b:person {id: 3}) " "CREATE (a)-[:knows]->(b);"
        )
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        unchanged = sum(
            1 for nid in r1_map if nid in r2_map and r2_map[nid] == r1_map[nid]
        )
        assert unchanged > 0, (
            f"Majority-vote should preserve some community IDs.\n"
            f"r1: {r1_map}\nr2: {r2_map}"
        )
    finally:
        conn.close()
        db.close()


def test_leiden_warmstart_multi_edge(tmp_path):
    """Warm-start with initial_community_property on a multi-edge graph,
    verifying stability when data is unchanged."""
    db_dir = tmp_path / "gds_leiden_warmstart_multi_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], "
            "{'[person, knows, person]': '', '[person, meets, person]': ''});"
        )
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "leiden_comm", r1_map)
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], "
            "{'[person, knows, person]': '', '[person, meets, person]': ''});"
        )
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        # Leiden is heuristic; warm-start preserves most (but not necessarily all)
        # community assignments when data is unchanged.
        unchanged = sum(
            1 for nid in r1_map if nid in r2_map and r2_map[nid] == r1_map[nid]
        )
        total = len(r1_map)
        assert unchanged >= total * 0.8, (
            f"Warm-start should preserve most community assignments.\n"
            f"r1: {r1_map}\nr2: {r2_map}\n"
            f"unchanged: {unchanged}/{total}"
        )
    finally:
        conn.close()
        db.close()


def test_leiden_multi_label_completeness(tmp_path):
    """Leiden on multi-vertex-label graph with concurrency: 4.
    Same graph as test_leiden_two_vertex_labels but with higher concurrency
    to exercise the parallel generic path."""
    with multi_vertex_label_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL leiden('person_org', {concurrency: 4})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        comm = {row[0]: row[1] for row in rows}
        assert set(comm.keys()) == {0, 2, 3, 5, 7, 8, 9, 10, 1, 4, 6}, f"comm={comm}"
        assert all(c >= 0 for c in comm.values()), f"comm={comm}"
        assert len(set(comm.values())) >= 2, f"comm={comm}"
        assert len(comm) > len(set(comm.values())), f"All singletons: {comm}"


def test_leiden_warmstart_stability_strict(tmp_path):
    """Warm-start correctness: when data is unchanged, the partition should be
    identical (not just similar). This verifies warm-start reproduces the same
    community structure."""
    db_dir = tmp_path / "gds_leiden_warmstart_strict_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "leiden_comm", r1_map)
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        # With unchanged data and warm-start, partition should be identical
        assert r1_map == r2_map, (
            f"Warm-start with unchanged data should be identical.\n"
            f"r1: {r1_map}\nr2: {r2_map}"
        )
    finally:
        conn.close()
        db.close()


# ─── previous_community column tests ──────────────────────────────────────


def test_leiden_incremental_previous_community_unchanged(tmp_path):
    """When data is unchanged, previous_community should equal community for
    all vertices (warm-start reproduces the same partition)."""
    db_dir = tmp_path / "gds_prev_comm_unchanged_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        # First run
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "leiden_comm", r1_map)
        # Re-project to pick up updated properties
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        # Second run with initial_community_property + YIELD previous_community
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true}) "
                "YIELD node, community, previous_community "
                "RETURN node.id, community, previous_community;"
            )
        )
        assert len(rows_r2) > 0, "Second run should produce results"
        assert all(
            len(row) == 3 for row in rows_r2
        ), "Each row should have (node_id, community, previous_community)"
        for nid, comm, prev_comm in rows_r2:
            assert prev_comm is not None, (
                f"previous_community should not be None for node {nid} "
                f"(all vertices existed in first run)"
            )
            assert prev_comm == comm, (
                f"previous_community ({prev_comm}) should equal community ({comm}) "
                f"for node {nid} when data is unchanged"
            )
    finally:
        conn.close()
        db.close()


def test_leiden_incremental_previous_community_with_data_change(tmp_path):
    """When data changes (new edge added), previous_community should reflect
    old community IDs even if community assignments change."""
    db_dir = tmp_path / "gds_prev_comm_change_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "leiden_comm", r1_map)
        # Add a new edge to change community structure
        conn.execute(
            "MATCH (a:person {id: 0}), (b:person {id: 3}) " "CREATE (a)-[:knows]->(b);"
        )
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true}) "
                "YIELD node, community, previous_community "
                "RETURN node.id, community, previous_community;"
            )
        )
        assert len(rows_r2) > 0
        prev_map = {row[0]: row[2] for row in rows_r2}
        # All previous_community values should match r1_map (old community IDs)
        for nid, old_comm in r1_map.items():
            assert prev_map.get(nid) == old_comm, (
                f"previous_community for node {nid} should be {old_comm}, "
                f"got {prev_map.get(nid)}"
            )
        # All pre-existing vertices should have non-None previous_community
        assert all(
            prev_map[nid] is not None for nid in r1_map
        ), "All pre-existing vertices should have non-None previous_community"
    finally:
        conn.close()
        db.close()


def test_leiden_incremental_previous_community_new_vertex(tmp_path):
    """Vertices without initial_community_property (simulating new vertices)
    should have previous_community = None.

    Uses a custom graph because tinysnb's CSR doesn't support adding vertices
    beyond the initial range. Instead, we create 6 vertices, write communities
    to all, then REMOVE the property from vertices 5,6 to simulate them being
    "new" (no previous community assignment).
    """
    with custom_graph_connection(
        tmp_path,
        db_name="gds_prev_comm_new_vtx_db",
        create_node_ddl="CREATE NODE TABLE n(id INT64 PRIMARY KEY);",
        create_rel_ddl="CREATE REL TABLE e(FROM n TO n);",
        node_inserts=[
            "CREATE (:n {id: 1});",
            "CREATE (:n {id: 2});",
            "CREATE (:n {id: 3});",
            "CREATE (:n {id: 4});",
            "CREATE (:n {id: 5});",
            "CREATE (:n {id: 6});",
        ],
        edge_inserts=[
            "MATCH (a:n), (b:n) WHERE a.id = 1 AND b.id = 2" " CREATE (a)-[:e]->(b);",
            "MATCH (a:n), (b:n) WHERE a.id = 2 AND b.id = 3" " CREATE (a)-[:e]->(b);",
            "MATCH (a:n), (b:n) WHERE a.id = 4 AND b.id = 5" " CREATE (a)-[:e]->(b);",
        ],
        graph_name="g",
        vertex_entries="['n']",
        edge_entries="{'[n, e, n]': ''}",
    ) as conn:
        # Add community property column
        conn.execute("ALTER TABLE n ADD leiden_comm INT64;")
        # First run
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        # Write back communities to all vertices using inline writeback
        # (can't use _batch_writeback because it hardcodes 'person' label)
        when_clauses = " ".join(
            f"WHEN n.id = {nid} THEN {comm}" for nid, comm in r1_map.items()
        )
        conn.execute(
            f"MATCH (n:n) "
            f"SET n.leiden_comm = CASE {when_clauses} ELSE n.leiden_comm END;"
        )
        # Set vertices 5,6 to -1 (invalid community ID) to simulate "new" vertices
        # C++ code treats negative IDs as invalid -> initial_community_ stays UINT32_MAX
        # -> previous_community outputs NULL
        conn.execute(
            "MATCH (n:n) WHERE n.id = 5 OR n.id = 6 " "SET n.leiden_comm = -1;"
        )
        # Re-project to pick up updated properties
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute("CALL project_graph('g', ['n'], " "{'[n, e, n]': ''});")
        # Second run with initial_community_property + YIELD previous_community
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true}) "
                "YIELD node, community, previous_community "
                "RETURN node.id, community, previous_community;"
            )
        )
        prev_map = {row[0]: row[2] for row in rows_r2}
        # Vertices 5,6 (no initial community) should have previous_community = None
        for nid in [5, 6]:
            assert nid in prev_map, f"Vertex {nid} should appear in results"
            assert (
                prev_map[nid] is None
            ), f"Vertex {nid} previous_community should be None, got {prev_map[nid]}"
        # Vertices 1-4 (had initial community) should have non-None previous_community
        for nid in [1, 2, 3, 4]:
            assert (
                prev_map.get(nid) is not None
            ), f"Vertex {nid} should have non-None previous_community"


def test_leiden_incremental_optional_column_not_yielded(tmp_path):
    """When previous_community is not YIELDed, output should have only 2 columns
    (backward compatibility)."""
    db_dir = tmp_path / "gds_prev_comm_not_yielded_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "leiden_comm", r1_map)
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        # Run with initial_community_property but WITHOUT YIELD previous_community
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        assert len(rows_r2) > 0
        # Each row should have exactly 2 columns (backward compat)
        for row in rows_r2:
            assert (
                len(row) == 2
            ), f"Row should have 2 columns (backward compat), got {len(row)}"
            assert isinstance(row[1], int), "community should be an integer"
    finally:
        conn.close()
        db.close()


def test_louvain_incremental_previous_community_unchanged(tmp_path):
    """Louvain: when data is unchanged, previous_community == community."""
    db_dir = tmp_path / "gds_louvain_prev_comm_unchanged_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD louvain_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r1 = list(
            conn.execute(
                "CALL louvain('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "louvain_comm", r1_map)
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r2 = list(
            conn.execute(
                "CALL louvain('g', "
                "{concurrency: 1, initial_community_property: 'louvain_comm', allow_relocation: true}) "
                "YIELD node, community, previous_community "
                "RETURN node.id, community, previous_community;"
            )
        )
        assert len(rows_r2) > 0
        assert all(len(row) == 3 for row in rows_r2)
        for nid, comm, prev_comm in rows_r2:
            assert (
                prev_comm is not None
            ), f"previous_community should not be None for node {nid}"
            assert prev_comm == comm, (
                f"previous_community ({prev_comm}) should equal community ({comm}) "
                f"for node {nid} when data is unchanged"
            )
    finally:
        conn.close()
        db.close()


def test_louvain_incremental_previous_community_with_data_change(tmp_path):
    """Louvain: when data changes, previous_community reflects old community IDs."""
    db_dir = tmp_path / "gds_louvain_prev_comm_change_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD louvain_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r1 = list(
            conn.execute(
                "CALL louvain('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "louvain_comm", r1_map)
        conn.execute(
            "MATCH (a:person {id: 0}), (b:person {id: 3}) " "CREATE (a)-[:knows]->(b);"
        )
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r2 = list(
            conn.execute(
                "CALL louvain('g', "
                "{concurrency: 1, initial_community_property: 'louvain_comm', allow_relocation: true}) "
                "YIELD node, community, previous_community "
                "RETURN node.id, community, previous_community;"
            )
        )
        assert len(rows_r2) > 0
        prev_map = {row[0]: row[2] for row in rows_r2}
        for nid, old_comm in r1_map.items():
            assert prev_map.get(nid) == old_comm, (
                f"previous_community for node {nid} should be {old_comm}, "
                f"got {prev_map.get(nid)}"
            )
    finally:
        conn.close()
        db.close()


def test_leiden_incremental_cypher_migration_matrix(tmp_path):
    """Cypher aggregation on previous_community x community produces a migration
    matrix. This verifies that previous_community can be used in GROUP BY queries."""
    db_dir = tmp_path / "gds_leiden_cypher_migmtx_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "leiden_comm", r1_map)
        # Add edge to change community structure
        conn.execute(
            "MATCH (a:person {id: 0}), (b:person {id: 3}) " "CREATE (a)-[:knows]->(b);"
        )
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        # Run leiden with previous_community and aggregate via Cypher
        mig_rows = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm', allow_relocation: true}) "
                "YIELD node, community, previous_community "
                "RETURN previous_community, community, count(*) AS members "
                "ORDER BY previous_community, community;"
            )
        )
        assert len(mig_rows) > 0, "Migration matrix should have at least one row"
        for row in mig_rows:
            assert (
                len(row) == 3
            ), f"Each row should have (prev_comm, comm, members), got {len(row)}"
            prev_comm, comm, members = row
            assert (
                isinstance(members, int) and members > 0
            ), f"members count should be positive integer, got {members}"
        # Verify total members sum equals number of vertices
        total = sum(row[2] for row in mig_rows)
        assert total == len(
            r1_map
        ), f"Total members ({total}) should equal vertex count ({len(r1_map)})"
    finally:
        conn.close()
        db.close()


def test_leiden_freeze_old_vertices_unchanged(tmp_path):
    """Freeze mode (default when initial_community_property is set):
    old vertices' communities must not change on re-run."""
    db_dir = tmp_path / "gds_leiden_freeze_unchanged_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD leiden_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        # First run: baseline communities
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "leiden_comm", r1_map)
        # Re-project to pick up updated properties
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        # Second run: freeze mode (default, no allow_relocation)
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm'}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        # All old vertices must have unchanged communities
        for nid, old_comm in r1_map.items():
            assert nid in r2_map, f"Vertex {nid} missing from second run"
            assert r2_map[nid] == old_comm, (
                f"Vertex {nid} community changed: {old_comm} -> {r2_map[nid]} "
                f"(freeze mode should preserve old communities)"
            )
    finally:
        conn.close()
        db.close()


def test_louvain_freeze_old_vertices_unchanged(tmp_path):
    """Freeze mode (default when initial_community_property is set):
    old vertices' communities must not change on re-run."""
    db_dir = tmp_path / "gds_louvain_freeze_unchanged_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE person ADD louvain_comm INT64;")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        # First run: baseline communities
        rows_r1 = list(
            conn.execute(
                "CALL louvain('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        _batch_writeback(conn, "louvain_comm", r1_map)
        # Re-project to pick up updated properties
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute(
            "CALL project_graph('g', ['person'], " "{'[person, knows, person]': ''});"
        )
        # Second run: freeze mode (default, no allow_relocation)
        rows_r2 = list(
            conn.execute(
                "CALL louvain('g', "
                "{concurrency: 1, initial_community_property: 'louvain_comm'}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        # All old vertices must have unchanged communities
        for nid, old_comm in r1_map.items():
            assert nid in r2_map, f"Vertex {nid} missing from second run"
            assert r2_map[nid] == old_comm, (
                f"Vertex {nid} community changed: {old_comm} -> {r2_map[nid]} "
                f"(freeze mode should preserve old communities)"
            )
    finally:
        conn.close()
        db.close()


def test_leiden_freeze_new_vertex_assignment(tmp_path):
    """Freeze mode: new vertices (no initial community) are assigned to existing
    or new communities. Old vertices' communities are frozen.

    Also verifies previous_community semantics:
    - Old vertices: previous_community == community (frozen, non-NULL)
    - New vertices: previous_community is NULL
    """
    with custom_graph_connection(
        tmp_path,
        db_name="gds_leiden_freeze_new_vtx_db",
        create_node_ddl="CREATE NODE TABLE n(id INT64 PRIMARY KEY);",
        create_rel_ddl="CREATE REL TABLE e(FROM n TO n);",
        node_inserts=[
            "CREATE (:n {id: 1});",
            "CREATE (:n {id: 2});",
            "CREATE (:n {id: 3});",
            "CREATE (:n {id: 4});",
            "CREATE (:n {id: 5});",
            "CREATE (:n {id: 6});",
        ],
        edge_inserts=[
            "MATCH (a:n), (b:n) WHERE a.id = 1 AND b.id = 2" " CREATE (a)-[:e]->(b);",
            "MATCH (a:n), (b:n) WHERE a.id = 2 AND b.id = 3" " CREATE (a)-[:e]->(b);",
            "MATCH (a:n), (b:n) WHERE a.id = 4 AND b.id = 5" " CREATE (a)-[:e]->(b);",
            # Connect vertex 6 to vertex 1 so it can join an existing community
            "MATCH (a:n), (b:n) WHERE a.id = 6 AND b.id = 1" " CREATE (a)-[:e]->(b);",
        ],
        graph_name="g",
        vertex_entries="['n']",
        edge_entries="{'[n, e, n]': ''}",
    ) as conn:
        # Add community property column
        conn.execute("ALTER TABLE n ADD leiden_comm INT64;")
        # First run: baseline communities
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        # Write back communities to vertices 1-4 only
        # Vertices 5,6 will have -1 (simulate "new" vertices)
        when_clauses = " ".join(
            f"WHEN n.id = {nid} THEN {comm}"
            for nid, comm in r1_map.items()
            if nid in [1, 2, 3, 4]
        )
        conn.execute(
            f"MATCH (n:n) " f"SET n.leiden_comm = CASE {when_clauses} ELSE -1 END;"
        )
        # Re-project to pick up updated properties
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute("CALL project_graph('g', ['n'], " "{'[n, e, n]': ''});")
        # Second run: freeze mode (default, no allow_relocation)
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{concurrency: 1, initial_community_property: 'leiden_comm'}) "
                "YIELD node, community, previous_community "
                "RETURN node.id, community, previous_community;"
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        prev_map = {row[0]: row[2] for row in rows_r2}
        # Vertices 1-4 (frozen): communities must be unchanged
        for nid in [1, 2, 3, 4]:
            assert nid in r2_map, f"Vertex {nid} missing from second run"
            assert r2_map[nid] == r1_map[nid], (
                f"Vertex {nid} community changed: {r1_map[nid]} -> {r2_map[nid]} "
                f"(freeze mode should preserve old communities)"
            )
            assert prev_map[nid] is not None, (
                f"Vertex {nid} previous_community should be non-NULL "
                f"(had initial community)"
            )
            assert prev_map[nid] == r2_map[nid], (
                f"Vertex {nid} previous_community ({prev_map[nid]}) should equal "
                f"community ({r2_map[nid]}) in freeze mode"
            )
        # Vertices 5,6 (new): should be assigned to some community
        for nid in [5, 6]:
            assert nid in r2_map, f"Vertex {nid} missing from second run"
            assert isinstance(
                r2_map[nid], int
            ), f"Vertex {nid} should have an integer community ID"
            assert prev_map[nid] is None, (
                f"Vertex {nid} previous_community should be NULL "
                f"(new vertex, no initial community)"
            )
