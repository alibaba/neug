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
# Tests for running GDS algorithms on temporary graphs (COPY TEMP)
# and mixed graphs (temporary + persistent).

import os
import shutil

import pytest

from neug import Database

EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
extension_test = pytest.mark.skipif(
    not EXTENSION_TESTS_ENABLED,
    reason="Extension tests disabled by default; set NEUG_RUN_EXTENSION_TESTS=1 to enable.",
)


def _write_csv(directory, filename, content):
    path = os.path.join(directory, filename)
    with open(path, "w") as f:
        f.write(content)
    return path


# ---------------------------------------------------------------------------
# Graph topology for tests (6 nodes, 7 edges):
#
#     0 -- 1 -- 2
#     |         |
#     3 -- 4 -- 5
#          |
#          6
#
# Two connected components would appear if we remove the 4-6 edge:
# Component A: {0,1,2,3,4,5}, Component B: {6}
# But with 4-6, it's all one component.
# ---------------------------------------------------------------------------

NODE_CSV = "id|name|value\n0|Alice|1.0\n1|Bob|2.0\n2|Carol|3.0\n3|Dave|4.0\n4|Eve|5.0\n5|Frank|6.0\n6|Grace|7.0\n"

EDGE_CSV = "src_id|dst_id|weight\n0|1|1.0\n1|2|1.0\n0|3|1.0\n3|4|1.0\n4|5|1.0\n2|5|1.0\n4|6|1.0\n"


@extension_test
class TestGDSOnTempGraph:
    """Test GDS algorithms on pure temporary graphs."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = str(tmp_path / "gds_temp_db")
        self.csv_dir = str(tmp_path / "csv")
        shutil.rmtree(self.db_dir, ignore_errors=True)
        os.makedirs(self.csv_dir, exist_ok=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()

        self.node_csv = _write_csv(self.csv_dir, "nodes.csv", NODE_CSV)
        self.edge_csv = _write_csv(self.csv_dir, "edges.csv", EDGE_CSV)

        # Create temporary node and edge tables
        self.conn.execute(
            f'COPY TEMP TNode FROM "{self.node_csv}" '
            f"(primary_key = 'id', header = true)"
        )
        self.conn.execute(
            f'COPY TEMP TEdge FROM "{self.edge_csv}" '
            f"(header = true, from = 'TNode', to = 'TNode')"
        )

        # Verify temp graph is loaded
        node_count = list(self.conn.execute("MATCH (n:TNode) RETURN count(n);"))[0][0]
        edge_count = list(self.conn.execute("MATCH ()-[r:TEdge]->() RETURN count(r);"))[
            0
        ][0]
        assert node_count == 7, f"Expected 7 temp nodes, got {node_count}"
        assert edge_count == 7, f"Expected 7 temp edges, got {edge_count}"

        yield
        self.conn.close()
        self.db.close()

    def _project_and_load_gds(self, graph_name="temp_graph"):
        """Project the temp graph and load GDS extension."""
        self.conn.execute(
            f"CALL project_graph("
            f"'{graph_name}', "
            f"['TNode'], "
            f"{{'[TNode, TEdge, TNode]': ''}}"
            f");"
        )
        self.conn.execute("LOAD gds;")

    # ------------------------------------------------------------------
    # PageRank on temp graph
    # ------------------------------------------------------------------
    def test_pagerank_on_temp_graph(self):
        """Run PageRank on a pure temporary graph."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL page_rank('temp_graph', {max_iterations: 20})
                YIELD node, rank
                RETURN node.id, rank ORDER BY rank DESC;
                """
            )
        )
        assert len(rows) == 7, f"PageRank should return 7 rows, got {len(rows)}"
        for row in rows:
            assert isinstance(row[1], float), "rank should be a float"
            assert row[1] > 0, "rank should be positive"
        print(f"[PageRank on temp graph] Results: {rows}")

    # ------------------------------------------------------------------
    # WCC (Weakly Connected Components) on temp graph
    # ------------------------------------------------------------------
    def test_wcc_on_temp_graph(self):
        """Run WCC on a pure temporary graph - all nodes should be in one component."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL wcc('temp_graph', {concurrency: 2})
                YIELD node, comp
                RETURN node.id, comp;
                """
            )
        )
        assert len(rows) == 7, f"WCC should return 7 rows, got {len(rows)}"
        components = {row[1] for row in rows}
        assert (
            len(components) == 1
        ), f"All nodes should be in 1 component, got {len(components)}"
        print(f"[WCC on temp graph] Results: {rows}")

    # ------------------------------------------------------------------
    # BFS on temp graph
    # ------------------------------------------------------------------
    def test_bfs_on_temp_graph(self):
        """Run BFS from node 0 on temp graph."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL bfs('temp_graph', {source: '0'})
                YIELD node, distance
                RETURN node.id, distance ORDER BY node.id;
                """
            )
        )
        assert len(rows) == 7, f"BFS should return 7 rows, got {len(rows)}"
        # node 0 distance = 0, node 1 distance = 1, etc.
        id_to_dist = {row[0]: row[1] for row in rows}
        assert id_to_dist[0] == 0, "Source node distance should be 0"
        assert id_to_dist[1] == 1, "Node 1 should be at distance 1 from node 0"
        print(f"[BFS on temp graph] Results: {rows}")

    # ------------------------------------------------------------------
    # CDLP (Community Detection Label Propagation) on temp graph
    # ------------------------------------------------------------------
    def test_cdlp_on_temp_graph(self):
        """Run CDLP on a pure temporary graph."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL cdlp('temp_graph', {concurrency: 2})
                YIELD node, label
                RETURN node.id, label;
                """
            )
        )
        assert len(rows) == 7, f"CDLP should return 7 rows, got {len(rows)}"
        print(f"[CDLP on temp graph] Results: {rows}")

    # ------------------------------------------------------------------
    # Louvain on temp graph
    # ------------------------------------------------------------------
    def test_louvain_on_temp_graph(self):
        """Run Louvain community detection on temp graph."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL louvain('temp_graph', {concurrency: 2})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows) == 7, f"Louvain should return 7 rows, got {len(rows)}"
        for row in rows:
            assert isinstance(row[1], int), "community should be an integer"
        print(f"[Louvain on temp graph] Results: {rows}")

    # ------------------------------------------------------------------
    # LCC (Local Clustering Coefficient) on temp graph
    # ------------------------------------------------------------------
    def test_lcc_on_temp_graph(self):
        """Run LCC on a pure temporary graph."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL lcc('temp_graph', {concurrency: 2})
                YIELD node, lcc
                RETURN node.id, lcc;
                """
            )
        )
        assert len(rows) == 7, f"LCC should return 7 rows, got {len(rows)}"
        for row in rows:
            assert isinstance(row[1], float), "lcc should be a float"
            assert 0.0 <= row[1] <= 1.0, f"lcc should be in [0,1], got {row[1]}"
        print(f"[LCC on temp graph] Results: {rows}")


@extension_test
class TestGDSOnMixedGraph:
    """Test GDS algorithms on mixed graphs (persistent nodes + temporary edges,
    or persistent graph extended with temporary data)."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.db_dir = str(tmp_path / "gds_mixed_db")
        self.csv_dir = str(tmp_path / "csv")
        shutil.rmtree(self.db_dir, ignore_errors=True)
        os.makedirs(self.csv_dir, exist_ok=True)
        self.db = Database(db_path=self.db_dir, mode="w")
        self.conn = self.db.connect()

        self.edge_csv = _write_csv(self.csv_dir, "edges.csv", EDGE_CSV)

        # Create persistent node table
        self.conn.execute(
            "CREATE NODE TABLE PNode(id INT64, name STRING, value DOUBLE, PRIMARY KEY(id));"
        )
        for vid, name, val in [
            (0, "Alice", 1.0),
            (1, "Bob", 2.0),
            (2, "Carol", 3.0),
            (3, "Dave", 4.0),
            (4, "Eve", 5.0),
            (5, "Frank", 6.0),
            (6, "Grace", 7.0),
        ]:
            self.conn.execute(
                f"CREATE (:PNode {{id: {vid}, name: '{name}', value: {val}}});"
            )

        # Create temporary edge table referencing persistent nodes
        self.conn.execute(
            f'COPY TEMP TEdgeMixed FROM "{self.edge_csv}" '
            f"(header = true, from = 'PNode', to = 'PNode')"
        )

        yield
        self.conn.close()
        self.db.close()

    def _project_and_load_gds(self, graph_name="mixed_graph"):
        """Project the mixed graph (persistent nodes + temp edges) and load GDS."""
        self.conn.execute(
            f"CALL project_graph("
            f"'{graph_name}', "
            f"['PNode'], "
            f"{{'[PNode, TEdgeMixed, PNode]': ''}}"
            f");"
        )
        self.conn.execute("LOAD gds;")

    # ------------------------------------------------------------------
    # PageRank on mixed graph
    # ------------------------------------------------------------------
    def test_pagerank_on_mixed_graph(self):
        """Run PageRank on persistent nodes + temporary edges."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL page_rank('mixed_graph', {max_iterations: 20})
                YIELD node, rank
                RETURN node.id, rank ORDER BY rank DESC;
                """
            )
        )
        assert len(rows) == 7, f"PageRank should return 7 rows, got {len(rows)}"
        for row in rows:
            assert isinstance(row[1], float), "rank should be a float"
            assert row[1] > 0, "rank should be positive"
        print(f"[PageRank on mixed graph] Results: {rows}")

    # ------------------------------------------------------------------
    # WCC on mixed graph
    # ------------------------------------------------------------------
    def test_wcc_on_mixed_graph(self):
        """Run WCC on persistent nodes + temporary edges."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL wcc('mixed_graph', {concurrency: 2})
                YIELD node, comp
                RETURN node.id, comp;
                """
            )
        )
        assert len(rows) == 7, f"WCC should return 7 rows, got {len(rows)}"
        components = {row[1] for row in rows}
        assert (
            len(components) == 1
        ), f"All 7 nodes should be in 1 component, got {len(components)}"
        print(f"[WCC on mixed graph] Results: {rows}")

    # ------------------------------------------------------------------
    # BFS on mixed graph
    # ------------------------------------------------------------------
    def test_bfs_on_mixed_graph(self):
        """Run BFS from node 0 on mixed graph."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL bfs('mixed_graph', {source: '0'})
                YIELD node, distance
                RETURN node.id, distance ORDER BY node.id;
                """
            )
        )
        assert len(rows) == 7, f"BFS should return 7 rows, got {len(rows)}"
        id_to_dist = {row[0]: row[1] for row in rows}
        assert id_to_dist[0] == 0, "Source node distance should be 0"
        print(f"[BFS on mixed graph] Results: {rows}")

    # ------------------------------------------------------------------
    # Louvain on mixed graph
    # ------------------------------------------------------------------
    def test_louvain_on_mixed_graph(self):
        """Run Louvain community detection on mixed graph."""
        self._project_and_load_gds()
        rows = list(
            self.conn.execute(
                """
                CALL louvain('mixed_graph', {concurrency: 2})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows) == 7, f"Louvain should return 7 rows, got {len(rows)}"
        print(f"[Louvain on mixed graph] Results: {rows}")
