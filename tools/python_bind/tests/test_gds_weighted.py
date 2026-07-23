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
# Tests for Leiden/Louvain weighted edge support.

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


@contextmanager
def tinysnb_weighted_connection(tmp_path):
    """Open a writable DB with tinysnb loaded, add a 'weight' property on
    knows edges, and create a projected graph.

    Edge weight assignment (from eKnows.csv edges):
      - Edges within {0,2,3,5} clique: weight = 10.0 (strong ties)
      - Edges from 7->{8,9}:           weight = 0.1 (weak ties)
    This should make {0,2,3,5} a tighter community and {7,8,9} looser.
    """
    db_dir = tmp_path / "gds_weighted_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    try:
        # Add weight column to knows edge table
        conn.execute("ALTER TABLE knows ADD weight DOUBLE DEFAULT 1.0;")
        # Set high weights for edges within the {0,2,3,5} clique
        for src, dst in [
            (0, 2),
            (0, 3),
            (0, 5),
            (2, 0),
            (2, 3),
            (2, 5),
            (3, 0),
            (3, 2),
            (3, 5),
            (5, 0),
            (5, 2),
            (5, 3),
        ]:
            conn.execute(
                f"MATCH (a:person {{id: {src}}})-[e:knows]->(b:person {{id: {dst}}}) "
                f"SET e.weight = 10.0;"
            )
        # Set low weights for edges from 7 to {8, 9}
        for src, dst in [(7, 8), (7, 9)]:
            conn.execute(
                f"MATCH (a:person {{id: {src}}})-[e:knows]->(b:person {{id: {dst}}}) "
                f"SET e.weight = 0.1;"
            )
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


def test_louvain_with_weight(tmp_path):
    """Run louvain with edge weight property."""
    with tinysnb_weighted_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL louvain('person_knows', {weight: 'weight', concurrency: 4})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows) > 0, "louvain with weight must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (node_id, community)"
            assert isinstance(row[1], int), "community should be an integer"
        # Vertices 0,2,3,5 have strong ties (weight=10) and should be
        # in the same community
        comm_map = {row[0]: row[1] for row in rows}
        clique_comm = {comm_map[v] for v in [0, 2, 3, 5] if v in comm_map}
        assert len(clique_comm) == 1, (
            f"Vertices 0,2,3,5 with strong ties should be in one community, "
            f"got communities: {clique_comm}"
        )


def test_leiden_with_weight(tmp_path):
    """Run leiden with edge weight property."""
    with tinysnb_weighted_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL leiden('person_knows', {weight: 'weight', concurrency: 4})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows) > 0, "leiden with weight must return at least one row"
        for row in rows:
            assert len(row) == 2, "each row should have (node_id, community)"
            assert isinstance(row[1], int), "community should be an integer"
        # Vertices 0,2,3,5 have strong ties (weight=10) and should be
        # in the same community
        comm_map = {row[0]: row[1] for row in rows}
        clique_comm = {comm_map[v] for v in [0, 2, 3, 5] if v in comm_map}
        assert len(clique_comm) == 1, (
            f"Vertices 0,2,3,5 with strong ties should be in one community, "
            f"got communities: {clique_comm}"
        )


def test_weighted_vs_unweighted_differs(tmp_path):
    """Weighted and unweighted louvain should both run without error when
    weights are asymmetric. The community structures may differ."""
    with tinysnb_weighted_connection(tmp_path) as conn:
        rows_weighted = list(
            conn.execute(
                """
                CALL louvain('person_knows', {weight: 'weight', concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        rows_unweighted = list(
            conn.execute(
                """
                CALL louvain('person_knows', {concurrency: 1})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
    assert len(rows_weighted) > 0
    assert len(rows_unweighted) > 0
    # Both should return the same set of vertices
    assert {r[0] for r in rows_weighted} == {r[0] for r in rows_unweighted}
    # With asymmetric weights (strong clique, weak peripheral), the community
    # count may differ. At minimum, the algorithm should run without error
    # and produce valid integer community IDs.
    for row in rows_weighted:
        assert isinstance(row[1], int)


def test_louvain_weight_missing_prop_no_crash(tmp_path):
    """Louvain with weight option on edges that don't have the property should
    fall back to default weight 1.0 without crashing."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL louvain('person_knows', {weight: 'nonexistent_prop', concurrency: 2})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows) > 0, (
            "louvain should fall back to default weight 1.0 when "
            "the specified property does not exist"
        )
        for row in rows:
            assert isinstance(row[1], int)


def test_leiden_weight_missing_prop_no_crash(tmp_path):
    """Leiden with weight option on edges that don't have the property should
    fall back to default weight 1.0 without crashing."""
    with tinysnb_simple_connection(tmp_path) as conn:
        rows = list(
            conn.execute(
                """
                CALL leiden('person_knows', {weight: 'nonexistent_prop', concurrency: 2})
                YIELD node, community
                RETURN node.id, community;
                """
            )
        )
        assert len(rows) > 0, (
            "leiden should fall back to default weight 1.0 when "
            "the specified property does not exist"
        )
        for row in rows:
            assert isinstance(row[1], int)


def test_louvain_no_weight_backward_compat(tmp_path):
    """Louvain without weight option should behave exactly as before
    (backward compatibility)."""
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
        assert len(rows) > 0, "louvain without weight must return at least one row"
        for row in rows:
            assert isinstance(row[1], int), "community should be an integer"


def test_leiden_no_weight_backward_compat(tmp_path):
    """Leiden without weight option should behave exactly as before
    (backward compatibility)."""
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
        assert len(rows) > 0, "leiden without weight must return at least one row"
        for row in rows:
            assert isinstance(row[1], int), "community should be an integer"


# -----------------------------------------------------------------------
# Weighted + freeze-assign (incremental) mode tests
# -----------------------------------------------------------------------


@contextmanager
def weighted_freeze_connection(tmp_path):
    """Create a custom graph with weighted edges for freeze-assign testing.

    Graph structure (6 vertices, 2 clusters connected by a weak edge):
      Cluster A: 1 -- 2 -- 3  (strong ties, weight=10)
      Cluster B: 4 -- 5 -- 6  (strong ties, weight=10)
      Bridge:    3 -- 4       (weak tie, weight=0.1)

    With weights, A and B should form separate communities.
    Without weights, the bridge is equally strong, so the graph may form
    fewer communities.
    """
    db_dir = tmp_path / "gds_weighted_freeze_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE n(id INT64 PRIMARY KEY);")
        conn.execute("CREATE REL TABLE e(FROM n TO n, weight DOUBLE);")
        for i in range(1, 7):
            conn.execute(f"CREATE (:n {{id: {i}}});")
        # Strong ties within clusters
        for src, dst in [
            (1, 2),
            (2, 1),
            (2, 3),
            (3, 2),
            (4, 5),
            (5, 4),
            (5, 6),
            (6, 5),
        ]:
            conn.execute(
                f"MATCH (a:n {{id: {src}}}), (b:n {{id: {dst}}}) "
                f"CREATE (a)-[:e {{weight: 10.0}}]->(b);"
            )
        # Weak bridge between clusters
        for src, dst in [(3, 4), (4, 3)]:
            conn.execute(
                f"MATCH (a:n {{id: {src}}}), (b:n {{id: {dst}}}) "
                f"CREATE (a)-[:e {{weight: 0.1}}]->(b);"
            )
        conn.execute("CALL project_graph('g', ['n'], {'[n, e, n]': ''});")
        conn.execute("LOAD gds;")
        yield conn
    finally:
        conn.close()
        db.close()


def _batch_writeback_n(conn, prop_name, comm_map):
    """Batch-write community IDs to vertex property on label 'n'."""
    if not comm_map:
        return
    when_clauses = " ".join(
        f"WHEN n.id = {nid} THEN {comm}" for nid, comm in comm_map.items()
    )
    conn.execute(
        f"MATCH (n:n) "
        f"SET n.{prop_name} = CASE {when_clauses} ELSE n.{prop_name} END;"
    )


def test_leiden_weighted_freeze(tmp_path):
    """Leiden with weight + freeze-assign mode:
    1. Run weighted leiden to get baseline communities.
    2. Write back communities to vertex property.
    3. Re-run with weight + initial_community_property (freeze mode).
    4. Old vertices' communities must be frozen (unchanged).
    5. previous_community must equal community for frozen vertices.
    """
    with weighted_freeze_connection(tmp_path) as conn:
        # Add community property column
        conn.execute("ALTER TABLE n ADD leiden_comm INT64;")
        # First run: weighted baseline
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {weight: 'weight', concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        assert len(r1_map) == 6, f"Expected 6 vertices, got {len(r1_map)}"
        # With strong intra-cluster ties and weak bridge, expect >= 2 communities
        assert (
            len(set(r1_map.values())) >= 2
        ), f"Weighted leiden should separate clusters, got {r1_map}"
        # Write back communities
        _batch_writeback_n(conn, "leiden_comm", r1_map)
        # Re-project to pick up updated properties
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute("CALL project_graph('g', ['n'], {'[n, e, n]': ''});")
        # Second run: freeze mode with weight
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{weight: 'weight', concurrency: 1, "
                "initial_community_property: 'leiden_comm'}) "
                "YIELD node, community, previous_community "
                "RETURN node.id, community, previous_community;"
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        prev_map = {row[0]: row[2] for row in rows_r2}
        # All vertices frozen: communities must match first run
        for nid, old_comm in r1_map.items():
            assert nid in r2_map, f"Vertex {nid} missing from second run"
            assert r2_map[nid] == old_comm, (
                f"Vertex {nid} community changed: {old_comm} -> {r2_map[nid]} "
                f"(freeze mode should preserve old communities)"
            )
            assert (
                prev_map[nid] is not None
            ), f"Vertex {nid} previous_community should be non-NULL"
            assert prev_map[nid] == r2_map[nid], (
                f"Vertex {nid} previous_community ({prev_map[nid]}) should "
                f"equal community ({r2_map[nid]}) in freeze mode"
            )


def test_louvain_weighted_freeze(tmp_path):
    """Louvain with weight + freeze-assign mode:
    Same verification as test_leiden_weighted_freeze but for Louvain.
    """
    with weighted_freeze_connection(tmp_path) as conn:
        conn.execute("ALTER TABLE n ADD louvain_comm INT64;")
        # First run: weighted baseline
        rows_r1 = list(
            conn.execute(
                "CALL louvain('g', {weight: 'weight', concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        assert len(r1_map) == 6, f"Expected 6 vertices, got {len(r1_map)}"
        assert (
            len(set(r1_map.values())) >= 2
        ), f"Weighted louvain should separate clusters, got {r1_map}"
        # Write back communities
        _batch_writeback_n(conn, "louvain_comm", r1_map)
        # Re-project
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute("CALL project_graph('g', ['n'], {'[n, e, n]': ''});")
        # Second run: freeze mode with weight
        rows_r2 = list(
            conn.execute(
                "CALL louvain('g', "
                "{weight: 'weight', concurrency: 1, "
                "initial_community_property: 'louvain_comm'}) "
                "YIELD node, community, previous_community "
                "RETURN node.id, community, previous_community;"
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        prev_map = {row[0]: row[2] for row in rows_r2}
        # All vertices frozen
        for nid, old_comm in r1_map.items():
            assert nid in r2_map, f"Vertex {nid} missing from second run"
            assert (
                r2_map[nid] == old_comm
            ), f"Vertex {nid} community changed: {old_comm} -> {r2_map[nid]}"
            assert (
                prev_map[nid] is not None
            ), f"Vertex {nid} previous_community should be non-NULL"
            assert (
                prev_map[nid] == r2_map[nid]
            ), f"Vertex {nid} previous_community should equal community"


def test_weighted_freeze_new_vertex_assignment(tmp_path):
    """Weighted freeze-assign with new vertices:
    1. Run weighted leiden on initial graph (vertices 1-4).
    2. Write back communities for vertices 1-4 only.
    3. Add new vertices 5,6 with edges.
    4. Re-project and run freeze mode with weight.
    5. Old vertices (1-4): frozen, previous_community == community.
    6. New vertices (5,6): assigned to some community, previous_community is NULL.
    """
    db_dir = tmp_path / "gds_weighted_freeze_new_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE n(id INT64 PRIMARY KEY);")
        conn.execute("CREATE REL TABLE e(FROM n TO n, weight DOUBLE);")
        # Initial vertices: 1,2,3,4
        for i in range(1, 5):
            conn.execute(f"CREATE (:n {{id: {i}}});")
        # Strong ties: 1-2, 2-3 (cluster A), 3-4 (bridge, weak)
        for src, dst, w in [
            (1, 2, 10.0),
            (2, 1, 10.0),
            (2, 3, 10.0),
            (3, 2, 10.0),
            (3, 4, 0.1),
            (4, 3, 0.1),
        ]:
            conn.execute(
                f"MATCH (a:n {{id: {src}}}), (b:n {{id: {dst}}}) "
                f"CREATE (a)-[:e {{weight: {w}}}]->(b);"
            )
        conn.execute("CALL project_graph('g', ['n'], {'[n, e, n]': ''});")
        conn.execute("LOAD gds;")
        conn.execute("ALTER TABLE n ADD leiden_comm INT64;")
        # First run: weighted baseline
        rows_r1 = list(
            conn.execute(
                "CALL leiden('g', {weight: 'weight', concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        r1_map = {row[0]: row[1] for row in rows_r1}
        # Write back communities for vertices 1-4
        _batch_writeback_n(conn, "leiden_comm", r1_map)
        # Add new vertices 5,6 with strong ties to each other
        conn.execute("CREATE (:n {id: 5});")
        conn.execute("CREATE (:n {id: 6});")
        # Mark new vertices as having no initial community (-1 = unassigned)
        conn.execute("MATCH (n:n) WHERE n.id = 5 OR n.id = 6 SET n.leiden_comm = -1;")
        for src, dst in [(5, 6), (6, 5)]:
            conn.execute(
                f"MATCH (a:n {{id: {src}}}), (b:n {{id: {dst}}}) "
                f"CREATE (a)-[:e {{weight: 10.0}}]->(b);"
            )
        # Re-project to include new vertices and edges
        conn.execute("CALL drop_projected_graph('g');")
        conn.execute("CALL project_graph('g', ['n'], {'[n, e, n]': ''});")
        # Second run: freeze mode with weight
        rows_r2 = list(
            conn.execute(
                "CALL leiden('g', "
                "{weight: 'weight', concurrency: 1, "
                "initial_community_property: 'leiden_comm'}) "
                "YIELD node, community, previous_community "
                "RETURN node.id, community, previous_community;"
            )
        )
        r2_map = {row[0]: row[1] for row in rows_r2}
        prev_map = {row[0]: row[2] for row in rows_r2}
        # Old vertices (1-4): frozen
        for nid in [1, 2, 3, 4]:
            assert nid in r2_map, f"Vertex {nid} missing from second run"
            assert (
                r2_map[nid] == r1_map[nid]
            ), f"Vertex {nid} community changed: {r1_map[nid]} -> {r2_map[nid]}"
            assert (
                prev_map[nid] is not None
            ), f"Vertex {nid} previous_community should be non-NULL"
        # New vertices (5,6): assigned to some community, previous_community NULL
        for nid in [5, 6]:
            assert nid in r2_map, f"Vertex {nid} missing from second run"
            assert isinstance(
                r2_map[nid], int
            ), f"Vertex {nid} should have an integer community ID"
            assert (
                prev_map[nid] is None
            ), f"Vertex {nid} previous_community should be NULL (new vertex)"
    finally:
        conn.close()
        db.close()


# -----------------------------------------------------------------------
# Resolution convergence tests
# Verifies that the modularity convergence check correctly uses the
# resolution parameter, so that very high resolution produces more
# communities than very low resolution.
# -----------------------------------------------------------------------


@contextmanager
def resolution_test_connection(tmp_path):
    """Create a graph for testing resolution sensitivity.

    Two dense clusters (5 vertices each, K5) connected by a bridge edge.
    With very high resolution (γ=100), vertices are penalized for joining
    any community, producing many singletons.
    With very low resolution (γ=0.001), vertices freely merge based on
    connections, producing few communities.
    """
    db_dir = tmp_path / "gds_resolution_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE n(id INT64 PRIMARY KEY);")
        conn.execute("CREATE REL TABLE e(FROM n TO n);")
        for i in range(1, 11):
            conn.execute(f"CREATE (:n {{id: {i}}});")
        # Cluster A: 1-5 fully connected
        for i in range(1, 6):
            for j in range(i + 1, 6):
                conn.execute(
                    f"MATCH (a:n {{id: {i}}}), (b:n {{id: {j}}}) "
                    f"CREATE (a)-[:e]->(b);"
                )
        # Cluster B: 6-10 fully connected
        for i in range(6, 11):
            for j in range(i + 1, 11):
                conn.execute(
                    f"MATCH (a:n {{id: {i}}}), (b:n {{id: {j}}}) "
                    f"CREATE (a)-[:e]->(b);"
                )
        # Bridge: 1 -> 6
        conn.execute("MATCH (a:n {id: 1}), (b:n {id: 6}) CREATE (a)-[:e]->(b);")
        conn.execute("CALL project_graph('g', ['n'], {'[n, e, n]': ''});")
        conn.execute("LOAD gds;")
        yield conn
    finally:
        conn.close()
        db.close()


def test_leiden_resolution_sensitivity(tmp_path):
    """Very high resolution should produce more communities than very low.

    With γ=100, the penalty for joining any community is extreme, so many
    vertices remain as singletons.
    With γ=0.001, vertices freely merge, producing few communities.

    Before the convergence-check fix, the modularity was computed with
    γ=1.0 regardless of the resolution parameter, causing the algorithm
    to stop prematurely. After the fix, the convergence check uses the
    correct γ, allowing the algorithm to iterate properly.
    """
    with resolution_test_connection(tmp_path) as conn:
        rows_low = list(
            conn.execute(
                "CALL leiden('g', {resolution: 0.001, concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        rows_high = list(
            conn.execute(
                "CALL leiden('g', {resolution: 100.0, concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
    low_communities = {row[1] for row in rows_low}
    high_communities = {row[1] for row in rows_high}
    assert len(low_communities) < len(high_communities), (
        f"Low resolution (γ=0.001) should produce fewer communities than "
        f"high resolution (γ=100.0): {len(low_communities)} vs "
        f"{len(high_communities)}"
    )


def test_louvain_resolution_sensitivity(tmp_path):
    """Same as test_leiden_resolution_sensitivity but for Louvain."""
    with resolution_test_connection(tmp_path) as conn:
        rows_low = list(
            conn.execute(
                "CALL louvain('g', {resolution: 0.001, concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
        rows_high = list(
            conn.execute(
                "CALL louvain('g', {resolution: 100.0, concurrency: 1}) "
                "YIELD node, community RETURN node.id, community;"
            )
        )
    low_communities = {row[1] for row in rows_low}
    high_communities = {row[1] for row in rows_high}
    assert len(low_communities) < len(high_communities), (
        f"Low resolution (γ=0.001) should produce fewer communities than "
        f"high resolution (γ=100.0): {len(low_communities)} vs "
        f"{len(high_communities)}"
    )
