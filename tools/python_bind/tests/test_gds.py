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
            ("2021" in label_to_pred[l]) or ("Date" in label_to_pred[l])
            for l in rel_rows
        ), "relationship predicate must be preserved"

        conn.execute("CALL drop_projected_graph('my_subgraph');")
        assert "my_subgraph" not in _shown_projected_graph_names(conn)


def test_run_label_propagation(tmp_path):
    """Load GDS extension and run label_propagation on a projected subgraph."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph("
            "'my_subgraph', "
            "{'person': 'n.age > 20', 'organisation': 'n.name = \"MIT\"'}, "
            "{'[person, studyat, organisation]': 'r.year > 2010'}"
            ");"
        )
        conn.execute("LOAD gds;")

        conn.execute(
            "CALL label_propagation('my_subgraph', {concurrency: 10}) RETURN node, label;"
        )
