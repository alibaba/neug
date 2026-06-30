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
# Regression tests for GDS CALL ... YIELD column selection and ordering.

import os
import sys
from typing import Callable
from typing import Sequence
from typing import Tuple

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug import Database


def _execute(conn, query: str) -> Tuple[Sequence[str], list]:
    result = conn.execute(query)
    rows = list(result)
    return result.column_names(), rows


def _wcc_reference(conn) -> dict:
    cols, rows = _execute(
        conn,
        "CALL wcc('pk', {concurrency: 1}) YIELD node, comp "
        "RETURN node.id AS id, comp ORDER BY id;",
    )
    return {row[cols.index("id")]: row[cols.index("comp")] for row in rows}


def _pagerank_reference(conn) -> dict:
    cols, rows = _execute(
        conn,
        "CALL page_rank('pk', {max_iterations: 5}) YIELD node, rank "
        "RETURN node.id AS id, rank ORDER BY id;",
    )
    return {row[cols.index("id")]: row[cols.index("rank")] for row in rows}


def _bfs_reference(conn) -> dict:
    cols, rows = _execute(
        conn,
        "CALL bfs('pk', {source: '0'}) YIELD node, distance, path "
        "RETURN node.id AS id, distance ORDER BY id;",
    )
    return {row[cols.index("id")]: row[cols.index("distance")] for row in rows}


@pytest.fixture(scope="module")
def tinysnb_gds_conn():
    """Project tinysnb person/knows graph and load the gds extension."""
    db = Database(mode="w")
    try:
        db.load_builtin_dataset("tinysnb")
        conn = db.connect()
        conn.execute(
            "CALL project_graph('pk', ['person'], " "{'[person, knows, person]': ''});"
        )
        conn.execute("LOAD gds;")
        yield conn
        conn.close()
    finally:
        db.close()


class TestGdsYieldNaturalOrder:
    """Full-column YIELD in natural order."""

    @pytest.mark.parametrize(
        "query",
        [
            pytest.param(
                "CALL wcc('pk', {concurrency: 1}) YIELD node, comp "
                "RETURN node.id AS id, comp ORDER BY id LIMIT 5;",
                id="wcc",
            ),
            pytest.param(
                "CALL wcc('pk', {concurrency: 1}) YIELD node AS n, comp AS c "
                "RETURN n.id AS id, c ORDER BY id LIMIT 5;",
                id="wcc_rename",
            ),
            pytest.param(
                "CALL page_rank('pk', {max_iterations: 5}) YIELD node, rank "
                "RETURN node.id AS id, rank ORDER BY id LIMIT 5;",
                id="pagerank",
            ),
            pytest.param(
                "CALL bfs('pk', {source: '0'}) YIELD node, distance, path "
                "RETURN node.id AS id, distance ORDER BY id LIMIT 5;",
                id="bfs",
            ),
        ],
    )
    def test_full_yield_succeeds(self, tinysnb_gds_conn, query):
        cols, rows = _execute(tinysnb_gds_conn, query)
        assert rows
        assert cols


class TestGdsYieldSubset:
    """YIELD with a strict subset of output columns."""

    def test_wcc_yield_comp_only(self, tinysnb_gds_conn):
        ref = _wcc_reference(tinysnb_gds_conn)
        cols, rows = _execute(
            tinysnb_gds_conn,
            "CALL wcc('pk', {concurrency: 1}) YIELD comp " "RETURN comp ORDER BY comp;",
        )
        assert cols == ["comp"]
        assert sorted(row[0] for row in rows) == sorted(ref.values())

    def test_wcc_yield_node_only(self, tinysnb_gds_conn):
        ref = _wcc_reference(tinysnb_gds_conn)
        cols, rows = _execute(
            tinysnb_gds_conn,
            "CALL wcc('pk', {concurrency: 1}) YIELD node "
            "RETURN node.id AS id ORDER BY id;",
        )
        got_ids = sorted(row[cols.index("id")] for row in rows)
        assert got_ids == sorted(ref.keys())

    def test_pagerank_yield_rank_only(self, tinysnb_gds_conn):
        ref = _pagerank_reference(tinysnb_gds_conn)
        cols, rows = _execute(
            tinysnb_gds_conn,
            "CALL page_rank('pk', {max_iterations: 5}) YIELD rank "
            "RETURN rank ORDER BY rank DESC;",
        )
        assert cols == ["rank"]
        assert len(rows) == len(ref)
        for row in rows:
            assert isinstance(row[0], float)

    def test_bfs_yield_distance_only(self, tinysnb_gds_conn):
        ref = _bfs_reference(tinysnb_gds_conn)
        cols, rows = _execute(
            tinysnb_gds_conn,
            "CALL bfs('pk', {source: '0'}) YIELD distance "
            "RETURN distance ORDER BY distance;",
        )
        assert cols == ["distance"]
        assert sorted(row[0] for row in rows) == sorted(ref.values())

    def test_bfs_yield_node_only(self, tinysnb_gds_conn):
        ref = _bfs_reference(tinysnb_gds_conn)
        cols, rows = _execute(
            tinysnb_gds_conn,
            "CALL bfs('pk', {source: '0'}) YIELD node "
            "RETURN node.id AS id ORDER BY id;",
        )
        got_ids = sorted(row[cols.index("id")] for row in rows)
        assert got_ids == sorted(ref.keys())


class TestGdsYieldReorder:
    """Reordered YIELD should expose columns in YIELD order with correct types."""

    @pytest.mark.parametrize(
        "query,reference",
        [
            pytest.param(
                "CALL wcc('pk', {concurrency: 1}) YIELD comp, node "
                "RETURN node.id AS id, comp ORDER BY id;",
                _wcc_reference,
                id="wcc_comp_node",
            ),
            pytest.param(
                "CALL page_rank('pk', {max_iterations: 5}) YIELD rank, node "
                "RETURN node.id AS id, rank ORDER BY id;",
                _pagerank_reference,
                id="pagerank_rank_node",
            ),
            pytest.param(
                "CALL bfs('pk', {source: '0'}) YIELD distance, node, path "
                "RETURN node.id AS id, distance ORDER BY id;",
                _bfs_reference,
                id="bfs_distance_node_path",
            ),
            pytest.param(
                "CALL bfs('pk', {source: '0'}) YIELD path, distance, node "
                "RETURN node.id AS id, distance ORDER BY id;",
                _bfs_reference,
                id="bfs_path_distance_node",
            ),
        ],
    )
    def test_reordered_yield_matches_reference(
        self, tinysnb_gds_conn, query, reference: Callable
    ):
        ref = reference(tinysnb_gds_conn)
        cols, rows = _execute(tinysnb_gds_conn, query)
        assert "id" in cols
        id_idx = cols.index("id")
        value_col = (
            "comp" if "comp" in cols else "rank" if "rank" in cols else "distance"
        )
        val_idx = cols.index(value_col)
        got = {row[id_idx]: row[val_idx] for row in rows}
        assert got == ref
