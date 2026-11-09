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
# Core Namespace and projected-graph tests; no GDS extension required.

from contextlib import contextmanager

import pytest
from conftest import wait_for_server_ready

from neug.database import Database
from neug.session import Session


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


@contextmanager
def namespace_operator_connection(tmp_path):
    """A small graph that makes node and relationship filtering observable."""
    db_dir = tmp_path / "namespace_operator_db"
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()
    setup = [
        "CREATE NODE TABLE person(id INT64 PRIMARY KEY, score INT64);",
        "CREATE NODE TABLE organisation(id INT64 PRIMARY KEY, score INT64);",
        (
            "CREATE REL TABLE connects(FROM person TO person, "
            "FROM person TO organisation, weight INT64);"
        ),
        "CREATE REL TABLE follows(FROM person TO person, weight INT64);",
        "CREATE (:person {id: 1, score: 20});",
        "CREATE (:person {id: 2, score: 15});",
        "CREATE (:person {id: 3, score: 5});",
        "CREATE (:person {id: 4, score: 30});",
        "CREATE (:organisation {id: 10, score: 20});",
        "CREATE (:organisation {id: 11, score: 5});",
    ]
    edges = [
        (1, "person", 2, 10),
        (2, "person", 4, 7),
        (1, "person", 3, 10),
        (1, "organisation", 10, 9),
        (2, "organisation", 11, 9),
        (4, "person", 1, 2),
    ]
    setup.extend(
        "MATCH (a:person), (b:{}) WHERE a.id = {} AND b.id = {} "
        "CREATE (a)-[:connects {{weight: {}}}]->(b);".format(
            dst_label, src, dst, weight
        )
        for src, dst_label, dst, weight in edges
    )
    setup.extend(
        "MATCH (a:person), (b:person) WHERE a.id = {} AND b.id = {} "
        "CREATE (a)-[:follows {{weight: {}}}]->(b);".format(src, dst, weight)
        for src, dst, weight in [
            (1, 2, 10),
            (2, 4, 7),
            (1, 3, 10),
            (4, 1, 2),
        ]
    )
    try:
        for statement in setup:
            conn.execute(statement)
        conn.execute(
            "CALL project_graph('all_graph', ['person', 'organisation'], "
            "{'[person, connects, person]': '', "
            "'[person, connects, organisation]': ''});"
        )
        conn.execute(
            "CALL project_graph('filtered_graph', "
            "{'person': 'n.score >= 10', "
            "'organisation': 'n.score >= 10'}, "
            "{'[person, connects, person]': 'r.weight >= 5', "
            "'[person, connects, organisation]': 'r.weight >= 5'});"
        )
        conn.execute(
            "CALL project_graph('all_path_graph', ['person'], "
            "{'[person, follows, person]': ''});"
        )
        conn.execute(
            "CALL project_graph('filtered_path_graph', "
            "{'person': 'n.score >= 10'}, "
            "{'[person, follows, person]': 'r.weight >= 5'});"
        )
        yield conn
    finally:
        conn.close()
        db.close()


def _physical_operator_names(result):
    return [
        operator["operator_name"]
        for operator in result.get_profile_metrics()["operators"]
    ]


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

        with pytest.raises(Exception, match="already exists"):
            conn.execute(
                "CALL project_graph("
                "'my_subgraph', "
                "['person'], "
                "{'[person, knows, person]': ''}"
                ");"
            )

        conn.execute("CALL drop_projected_graph('my_subgraph');")
        assert "my_subgraph" not in _shown_projected_graph_names(
            conn
        ), "SHOW_PROJECTED_GRAPHS must not list a dropped alias"

        with pytest.raises(Exception, match="does not exist"):
            conn.execute("CALL drop_projected_graph('my_subgraph');")


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

        all_edges = list(
            conn.execute(
                "MATCH (a:person)-[r:knows]->(b:person) " "RETURN a.id, r.date, b.id;"
            )
        )
        expected_edges = list(
            conn.execute(
                "MATCH (a:person)-[r:knows]->(b:person) "
                'WHERE a.age > 20 AND r.date > Date("2021-01-01") '
                "AND b.age > 20 RETURN a.id, r.date, b.id;"
            )
        )
        namespace_edges = list(
            conn.execute(
                "USE NAMESPACE my_subgraph "
                "MATCH (a:person)-[r:knows]->(b:person) "
                "RETURN a.id, r.date, b.id;"
            )
        )
        assert 0 < len(expected_edges) < len(all_edges)
        assert sorted(namespace_edges) == sorted(expected_edges)

        conn.execute("CALL drop_projected_graph('my_subgraph');")
        assert "my_subgraph" not in _shown_projected_graph_names(conn)


def test_project_graph_predicate_with_dynamic_parameter(tmp_path):
    """Dynamic parameters in predicates remain safe when expressions are copied."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph("
            "'adult_graph', "
            "{'person': 'n.age > $minimum_age'}, "
            "{'[person, knows, person]': ''}"
            ");",
            parameters={"minimum_age": 20},
        )

        ages = [
            row[0]
            for row in conn.execute(
                "USE NAMESPACE adult_graph "
                "MATCH (n:person) RETURN n.age ORDER BY n.age;",
                parameters={"minimum_age": 20},
            )
        ]
        assert ages
        assert all(age > 20 for age in ages)


def test_project_graph_rejects_unsupported_predicate_expression(tmp_path):
    """Predicates whose expression nodes cannot be copied are rejected early."""
    with tinysnb_connection(tmp_path) as conn:
        with pytest.raises(Exception, match="Unsupported projected graph predicate"):
            conn.execute(
                "CALL project_graph("
                "'case_graph', "
                "{'person': 'CASE WHEN n.age > 20 THEN true ELSE false END'}, "
                "{'[person, knows, person]': ''}"
                ");"
            )


def test_namespace_match_isolation_and_clause_scope(tmp_path):
    """USE NAMESPACE filters reads and rejects write queries."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph("
            "'adult_graph', "
            "{'person': 'n.age > 20'}, "
            "{'[person, knows, person]': ''}"
            ");"
        )

        ages = [
            row[0]
            for row in conn.execute(
                "USE NAMESPACE adult_graph "
                "MATCH (n:person) RETURN n.age ORDER BY n.age;"
            )
        ]
        assert ages
        assert all(age > 20 for age in ages)

        explain_result = conn.execute(
            "EXPLAIN USE NAMESPACE adult_graph MATCH (n:person) RETURN n.age;"
        )
        assert list(explain_result) == []
        assert explain_result.has_profile_result()

        profile_result = conn.execute(
            "PROFILE USE NAMESPACE adult_graph "
            "MATCH (n:person) RETURN n.age ORDER BY n.age;"
        )
        assert [row[0] for row in profile_result] == ages
        assert profile_result.has_profile_result()

        wildcard_ages = [
            row[0]
            for row in conn.execute(
                "USE NAMESPACE adult_graph " "MATCH (n) RETURN n.age ORDER BY n.age;"
            )
        ]
        assert wildcard_ages == ages

        young_ages = [
            row[0]
            for row in conn.execute("MATCH (m:person) WHERE m.age <= 20 RETURN m.age;")
        ]
        assert young_ages
        nested_ages = list(
            conn.execute(
                "USE NAMESPACE adult_graph MATCH (n:person) "
                "WHERE EXISTS { MATCH (m:person) WHERE m.age <= 20 } "
                "RETURN n.age;"
            )
        )
        assert nested_ages == []

        endpoints = list(
            conn.execute(
                "USE NAMESPACE adult_graph "
                "MATCH (a)-[r:knows]->(b) RETURN a.age, b.age;"
            )
        )
        assert all(src_age > 20 and dst_age > 20 for src_age, dst_age in endpoints)

        wildcard_endpoints = list(
            conn.execute(
                "USE NAMESPACE adult_graph " "MATCH (a)-[r]->(b) RETURN a.age, b.age;"
            )
        )
        assert sorted(wildcard_endpoints) == sorted(endpoints)

        with pytest.raises(Exception, match="does not exist"):
            conn.execute("USE NAMESPACE missing_graph MATCH (n:person) RETURN n;")

        with pytest.raises(Exception, match="is not part of Namespace"):
            conn.execute("USE NAMESPACE adult_graph MATCH (n:organisation) RETURN n;")

        with pytest.raises(Exception, match="only support read-only"):
            conn.execute("USE NAMESPACE adult_graph CREATE (:person {ID: 999});")

        with pytest.raises(Exception, match="only support read-only"):
            conn.execute("USE NAMESPACE adult_graph MERGE (:person {ID: 999});")

        with pytest.raises(Exception, match="only support read-only"):
            conn.execute("USE NAMESPACE adult_graph MATCH (n:person) SET n.age = 999;")

        with pytest.raises(Exception, match="only support read-only"):
            conn.execute("USE NAMESPACE adult_graph MATCH (n:person) DELETE n;")

        optional_rows = list(
            conn.execute(
                "USE NAMESPACE adult_graph "
                "MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) "
                "RETURN a.age, b.age;"
            )
        )
        assert optional_rows
        assert all(src_age > 20 for src_age, _ in optional_rows)
        assert all(dst_age is None or dst_age > 20 for _, dst_age in optional_rows)

        conn.execute(
            "CALL project_graph('z_graph', ['person'], "
            "{'[person, knows, person]': ''});"
        )
        assert _shown_projected_graph_names(conn) == ["adult_graph", "z_graph"]


def test_namespace_variable_length_paths_and_predicate_planning(tmp_path):
    """Namespace predicates target recursive edge/node variables, not PATH."""
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph('all_people', ['person'], "
            "{'[person, knows, person]': ''});"
        )

        fixed_explain = conn.execute(
            "EXPLAIN USE NAMESPACE all_people "
            "MATCH (a:person)-[:knows]->(:person) RETURN a.id;"
        )
        fixed_operators = [
            op["operator_name"]
            for op in fixed_explain.get_profile_metrics()["operators"]
        ]
        base_explain = conn.execute(
            "EXPLAIN MATCH (a:person)-[:knows]->(:person) RETURN a.id;"
        )
        base_operators = [
            op["operator_name"]
            for op in base_explain.get_profile_metrics()["operators"]
        ]
        assert fixed_operators == base_operators
        assert "FilterOpr" not in fixed_operators

        namespace_rows = list(
            conn.execute(
                "USE NAMESPACE all_people "
                "MATCH (a:person)-[:knows*1..2]->(b:person) "
                "RETURN DISTINCT a.id, b.id ORDER BY a.id, b.id;"
            )
        )
        base_rows = list(
            conn.execute(
                "MATCH (a:person)-[:knows*1..2]->(b:person) "
                "RETURN DISTINCT a.id, b.id ORDER BY a.id, b.id;"
            )
        )
        assert namespace_rows == base_rows

        conn.execute(
            "CALL project_graph('adult_paths', {'person': 'n.age > 20'}, "
            "{'[person, knows, person]': ''});"
        )
        namespace_adult_rows = list(
            conn.execute(
                "USE NAMESPACE adult_paths "
                "MATCH (a:person)-[:knows*1..2]->(b:person) "
                "RETURN DISTINCT a.id, b.id ORDER BY a.id, b.id;"
            )
        )
        explicit_adult_rows = list(
            conn.execute(
                "MATCH (a:person)-[:knows*1..2 (r, n | WHERE n.age > 20)]->"
                "(b:person) WHERE a.age > 20 AND b.age > 20 "
                "RETURN DISTINCT a.id, b.id ORDER BY a.id, b.id;"
            )
        )
        assert namespace_adult_rows == explicit_adult_rows

        conn.execute(
            "CALL project_graph('recent_paths', ['person'], "
            "{'[person, knows, person]': "
            "'r.date > Date(\"2021-01-01\")'});"
        )
        namespace_recent_rows = list(
            conn.execute(
                "USE NAMESPACE recent_paths "
                "MATCH (a:person)-[:knows*1..2]->(b:person) "
                "RETURN DISTINCT a.id, b.id ORDER BY a.id, b.id;"
            )
        )
        explicit_recent_rows = list(
            conn.execute(
                "MATCH (a:person)-[:knows*1..2 "
                '(r, n | WHERE r.date > Date("2021-01-01"))]->(b:person) '
                "RETURN DISTINCT a.id, b.id ORDER BY a.id, b.id;"
            )
        )
        assert namespace_recent_rows == explicit_recent_rows


@pytest.mark.parametrize("direction", ["out", "in", "both"])
@pytest.mark.parametrize("with_edge_predicate", [False, True])
@pytest.mark.parametrize("use_namespace", [False, True])
@pytest.mark.parametrize("path_kind", ["", "TRAIL", "ACYCLIC"])
def test_issue_1207_recursive_node_predicate(
    tmp_path, direction, with_edge_predicate, use_namespace, path_kind
):
    """Reject paths through hidden nodes before they reach a visible endpoint."""
    db = Database(str(tmp_path / "recursive_node_predicate"), mode="w")
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE Doc(id INT64, workspace STRING, content STRING, "
            "PRIMARY KEY(id))"
        )
        conn.execute("CREATE REL TABLE LINK(FROM Doc TO Doc, weight INT64)")
        for doc_id in range(1, 6):
            conn.execute(
                "CREATE (:Doc {id: $id, workspace: $workspace, content: $content})",
                parameters={
                    "id": doc_id,
                    "workspace": "finance" if doc_id == 4 else "payments",
                    "content": "hidden content" if doc_id == 4 else "visible content",
                },
            )
        edges = [(1, 2, 1), (2, 3, 1), (1, 4, 1), (4, 2, 1)]
        if with_edge_predicate:
            edges.extend([(1, 5, 0), (5, 2, 1)])
        for src, dst, weight in edges:
            if direction == "in":
                src, dst = dst, src
            conn.execute(
                "MATCH (a:Doc {id: $src}), (b:Doc {id: $dst}) "
                "CREATE (a)-[:LINK {weight: $weight}]->(b)",
                parameters={"src": src, "dst": dst, "weight": weight},
            )
        edge_predicate = "r.weight > 0" if with_edge_predicate else ""
        conn.execute(
            "CALL project_graph('payments', "
            "{'Doc': 'n.workspace = \"payments\"'}, "
            "{'[Doc, LINK, Doc]': '" + edge_predicate + "'})"
        )
        recursive_filter = ""
        if not use_namespace:
            recursive_filter = "(r, v | WHERE v.workspace = $workspace"
            if with_edge_predicate:
                recursive_filter += " AND " + edge_predicate
            recursive_filter += ")"
        relationship = "[:LINK*{} 2..2 {}]".format(path_kind, recursive_filter)
        hop = {"out": "-{}->", "in": "<-{}-", "both": "-{}-"}[direction].format(
            relationship
        )
        query = (
            ("USE NAMESPACE payments " if use_namespace else "")
            + "MATCH p=(a:Doc {id: 1})"
            + hop
            + '(n:Doc) WHERE a.workspace = "payments" '
            'AND n.workspace = "payments" AND n.id IN [2, 3] '
        )
        parameters = {} if use_namespace else {"workspace": "payments"}
        assert list(
            conn.execute(query + "RETURN n.id ORDER BY n.id", parameters=parameters)
        ) == [[3]]
        rows = list(
            conn.execute(query + "RETURN n.id, p ORDER BY n.id", parameters=parameters)
        )
        assert [row[0] for row in rows] == [3]
        nodes = rows[0][1]["nodes"]
        assert [node["id"] for node in nodes] == [1, 2, 3]
        assert all(node["workspace"] == "payments" for node in nodes)
        assert all(node["content"] == "visible content" for node in nodes)
    finally:
        conn.close()
        db.close()


@pytest.mark.parametrize("direction", ["out", "in", "both"])
@pytest.mark.parametrize("path_kind", ["TRAIL", "ACYCLIC"])
@pytest.mark.parametrize("with_edge_predicate", [False, True])
def test_namespace_recursive_path_uniqueness(
    tmp_path, direction, path_kind, with_edge_predicate
):
    """All-visible cycles preserve TRAIL/ACYCLIC semantics with predicates."""
    db = Database(str(tmp_path / "path_uniqueness"), mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE Doc(id INT64 PRIMARY KEY, visible BOOL)")
        conn.execute("CREATE REL TABLE LINK(FROM Doc TO Doc, weight INT64)")
        for doc_id in [1, 2, 3]:
            conn.execute(
                "CREATE (:Doc {id: $id, visible: true})", parameters={"id": doc_id}
            )
        for src, dst in [(1, 2), (2, 3), (3, 1)]:
            if direction == "in":
                src, dst = dst, src
            conn.execute(
                "MATCH (a:Doc {id: $src}), (b:Doc {id: $dst}) "
                "CREATE (a)-[:LINK {weight: 1}]->(b)",
                parameters={"src": src, "dst": dst},
            )
        edge_predicate = "r.weight > 0" if with_edge_predicate else ""
        conn.execute(
            "CALL project_graph('visible', {'Doc': 'n.visible = true'}, "
            "{'[Doc, LINK, Doc]': '" + edge_predicate + "'})"
        )
        relationship = "[:LINK*{} 0..4]".format(path_kind)
        hop = {"out": "-{}->", "in": "<-{}-", "both": "-{}-"}[direction].format(
            relationship
        )
        rows = list(
            conn.execute(
                "USE NAMESPACE visible MATCH p=(:Doc {id: 1})" + hop + "(:Doc) RETURN p"
            )
        )
        expected = {(1,), (1, 2), (1, 2, 3)}
        if direction == "both":
            expected |= {(1, 3), (1, 3, 2)}
        if path_kind == "TRAIL":
            expected.add((1, 2, 3, 1))
            if direction == "both":
                expected.add((1, 3, 2, 1))
        assert len(rows) == len(expected)
        assert {tuple(n["id"] for n in row[0]["nodes"]) for row in rows} == expected
        for (path,) in rows:
            edge_ids = [edge["_ID"] for edge in path["rels"]]
            assert len(edge_ids) == len(set(edge_ids))
            if path_kind == "ACYCLIC":
                node_ids = [node["id"] for node in path["nodes"]]
                assert len(node_ids) == len(set(node_ids))
    finally:
        conn.close()
        db.close()


def test_issue_1093_namespace_variable_length_path_regression(tmp_path):
    """Reproduce #1093: a namespaced variable-length path must not crash."""
    db = Database(db_path=str(tmp_path / "issue_1093_db"), mode="w")
    conn = db.connect()
    try:
        conn.execute("CREATE NODE TABLE onto(uri STRING PRIMARY KEY, scope_id STRING)")
        conn.execute("CREATE REL TABLE relate(FROM onto TO onto)")

        for i in range(6):
            scope = "s0" if i < 4 else "s1"
            conn.execute(
                "CREATE (:onto {uri:$uri, scope_id:$scope})",
                parameters={"uri": "u{}".format(i), "scope": scope},
            )

        for i in range(5):
            conn.execute(
                "MATCH (a:onto {uri:$a}), (b:onto {uri:$b}) "
                "CREATE (a)-[:relate]->(b)",
                parameters={
                    "a": "u{}".format(i),
                    "b": "u{}".format(i + 1),
                },
            )

        conn.execute(
            "CALL project_graph("
            "'ns0', {'onto': 'n.scope_id = \"s0\"'}, "
            "['[onto, relate, onto]'])"
        )
        query = (
            "USE NAMESPACE ns0 "
            "MATCH (a:onto {uri:'u0'})-[:relate*1..2]-(x) "
            "RETURN DISTINCT x.uri"
        )

        assert sorted(row[0] for row in conn.execute(query)) == ["u0", "u1", "u2"]

        profile = conn.execute("PROFILE " + query)
        assert sorted(row[0] for row in profile) == ["u0", "u1", "u2"]
        operators = _physical_operator_names(profile)
        assert "PathExpandOprWithPred" in operators
        assert "GetVFromEdgesOpr" in operators
        assert "FilterOpr" not in operators
    finally:
        conn.close()
        db.close()


def test_namespace_without_predicates_has_no_filtering_operators(tmp_path):
    """An unfiltered project_graph must plan exactly like the base graph."""
    queries = [
        ("all_graph", "MATCH (n:person) RETURN n.id"),
        (
            "all_graph",
            "MATCH (a:person)-[r:connects]->(b:person) " "RETURN a.id, r.weight",
        ),
        (
            "all_graph",
            "MATCH (a:person)-[r:connects]->(b:person) "
            "RETURN a.id, r.weight, b.score",
        ),
        (
            "all_path_graph",
            "MATCH (a:person)-[:follows*1..2]->(b:person) " "RETURN a.id, b.id",
        ),
    ]
    with namespace_operator_connection(tmp_path) as conn:
        for namespace, query in queries:
            base_plan = _physical_operator_names(conn.execute("EXPLAIN " + query))
            namespace_plan = _physical_operator_names(
                conn.execute("EXPLAIN USE NAMESPACE {} ".format(namespace) + query)
            )
            assert namespace_plan == base_plan, query
            assert "FilterOpr" not in namespace_plan, query
            base_rows = sorted(tuple(row) for row in conn.execute(query))
            namespace_rows = sorted(
                tuple(row)
                for row in conn.execute("USE NAMESPACE {} ".format(namespace) + query)
            )
            assert namespace_rows == base_rows, query

        # Wildcard patterns cannot be compared with the base graph because the
        # namespace intentionally hides non-projected tables. They must still
        # avoid manufacturing label filters when every projected predicate is
        # empty.
        wildcard_queries = [
            ("all_graph", "MATCH (n) RETURN n.id"),
            ("all_graph", "MATCH (a)-[r]->(b) RETURN a.id, r.weight, b.id"),
            ("all_path_graph", "MATCH (a)-[*1..2]->(b) RETURN a.id, b.id"),
        ]
        for namespace, query in wildcard_queries:
            operators = _physical_operator_names(
                conn.execute("EXPLAIN USE NAMESPACE {} ".format(namespace) + query)
            )
            assert "FilterOpr" not in operators, query


@pytest.mark.parametrize(
    "pattern, expected",
    [
        (
            "(a:person)-[r:connects]->(b:person)",
            {(1, 10, 2, 15), (2, 7, 4, 30)},
        ),
        (
            "(a)-[r]->(b)",
            {(1, 10, 2, 15), (2, 7, 4, 30), (1, 9, 10, 20)},
        ),
        (
            "(a:person)-[r]->(b)",
            {(1, 10, 2, 15), (2, 7, 4, 30), (1, 9, 10, 20)},
        ),
    ],
    ids=["specified_labels", "no_labels", "partial_labels"],
)
def test_namespace_node_and_edge_predicates_reach_expand_and_get_v(
    tmp_path, pattern, expected
):
    """Both predicates apply for fully, partially and unlabelled patterns."""
    with namespace_operator_connection(tmp_path) as conn:
        query = (
            "PROFILE USE NAMESPACE filtered_graph MATCH {} "
            "RETURN a.id, r.weight, b.id, b.score".format(pattern)
        )
        result = conn.execute(query)
        assert {tuple(row) for row in result} == expected
        operators = _physical_operator_names(result)
        assert any("EdgeExpand" in operator for operator in operators)
        assert any("GetV" in operator for operator in operators)
        assert "FilterOpr" not in operators


@pytest.mark.parametrize(
    "pattern, expected",
    [
        (
            "(a:person)-[:follows*1..2]->(b:person)",
            {(1, 2), (1, 4), (2, 4)},
        ),
        (
            "(a)-[*1..2]->(b)",
            {(1, 2), (1, 4), (2, 4)},
        ),
        (
            "(a:person)-[*1..2]->(b)",
            {(1, 2), (1, 4), (2, 4)},
        ),
    ],
    ids=["specified_labels", "no_labels", "partial_labels"],
)
def test_namespace_node_and_edge_predicates_reach_path_expand(
    tmp_path, pattern, expected
):
    """PathExpand applies edge and every visited-node predicate recursively."""
    with namespace_operator_connection(tmp_path) as conn:
        result = conn.execute(
            "PROFILE USE NAMESPACE filtered_path_graph "
            "MATCH {} RETURN a.id, b.id".format(pattern)
        )
        assert {tuple(row) for row in result} == expected
        operators = _physical_operator_names(result)
        assert "PathExpandOprWithPred" in operators
        assert "GetVFromEdgesOpr" in operators
        assert "FilterOpr" not in operators


@pytest.mark.parametrize(
    "pattern, expected",
    [
        ("(n:person)", {1, 2, 4}),
        ("(n)", {1, 2, 4, 10}),
    ],
    ids=["specified_label", "unspecified_label"],
)
def test_namespace_node_predicates_reach_scan(tmp_path, pattern, expected):
    with namespace_operator_connection(tmp_path) as conn:
        result = conn.execute(
            "PROFILE USE NAMESPACE filtered_graph MATCH {} RETURN n.id".format(pattern)
        )
        assert {row[0] for row in result} == expected
        operators = _physical_operator_names(result)
        assert any("Scan" in operator for operator in operators)
        assert "FilterOpr" not in operators


def test_namespace_predicates_in_optional_match(tmp_path):
    """OPTIONAL MATCH preserves rows while filtering its expand/GetV branch."""
    with namespace_operator_connection(tmp_path) as conn:
        result = conn.execute(
            "PROFILE USE NAMESPACE filtered_graph MATCH (a) "
            "OPTIONAL MATCH (a)-[r]->(b) "
            "RETURN a.id, r.weight, b.id"
        )
        assert {tuple(row) for row in result} == {
            (1, 10, 2),
            (1, 9, 10),
            (2, 7, 4),
            (4, None, None),
            (10, None, None),
        }
        operators = _physical_operator_names(result)
        assert any("GetV" in operator for operator in operators)
        assert "FilterOpr" not in operators

        path_result = conn.execute(
            "PROFILE USE NAMESPACE filtered_path_graph MATCH (a:person) "
            "OPTIONAL MATCH (a)-[:follows*1..2]->(b:person) "
            "RETURN a.id, b.id"
        )
        assert {tuple(row) for row in path_result} == {
            (1, 2),
            (1, 4),
            (2, 4),
            (4, None),
        }
        path_operators = _physical_operator_names(path_result)
        assert "PathExpandOprWithPred" in path_operators
        assert "FilterOpr" not in path_operators

        unlabeled_path_result = conn.execute(
            "PROFILE USE NAMESPACE filtered_path_graph MATCH (a) "
            "OPTIONAL MATCH (a)-[b*1..2]->(c) "
            "RETURN a.id, c.id"
        )
        assert {tuple(row) for row in unlabeled_path_result} == {
            (1, 2),
            (1, 4),
            (2, 4),
            (4, None),
        }
        unlabeled_path_operators = _physical_operator_names(unlabeled_path_result)
        assert "PathExpandOprWithPred" in unlabeled_path_operators
        assert "GetVFromEdgesOpr" in unlabeled_path_operators
        assert "FilterOpr" not in unlabeled_path_operators


def test_namespace_query_scope_for_union_subquery(tmp_path):
    with tinysnb_connection(tmp_path) as conn:
        conn.execute(
            "CALL project_graph('adult_graph', {'person': 'n.age > 20'}, "
            "{'[person, knows, person]': ''});"
        )
        adult_ages = [
            row[0]
            for row in conn.execute(
                "USE NAMESPACE adult_graph "
                "MATCH (n:person) RETURN n.age ORDER BY n.age;"
            )
        ]

        union_result = conn.execute(
            "USE NAMESPACE adult_graph "
            "MATCH (n:person) RETURN count(n) AS count "
            "UNION ALL MATCH (n:person) RETURN count(n) AS count;"
        )
        assert len(union_result) == 2
        with pytest.raises(Exception, match="is not part of Namespace"):
            conn.execute(
                "USE NAMESPACE adult_graph "
                "MATCH (n:person) RETURN n.age AS value "
                "UNION ALL MATCH (n:organisation) RETURN n.name AS value;"
            )

        subquery_ages = [
            row[0]
            for row in conn.execute(
                "USE NAMESPACE adult_graph "
                "MATCH (seed:person) WITH seed "
                "CALL (seed) { "
                "MATCH (n:person) WHERE n.age < 40 RETURN n.age AS age "
                "UNION ALL "
                "MATCH (n:person) WHERE n.age >= 40 RETURN n.age AS age "
                "} "
                "RETURN DISTINCT age ORDER BY age;"
            )
        ]
        assert sorted(subquery_ages) == sorted(adult_ages)


def test_namespace_keyword_remains_a_valid_identifier(tmp_path):
    db = Database(db_path=str(tmp_path / "namespace_identifier_db"), mode="w")
    conn = db.connect()
    try:
        conn.execute(
            "CREATE NODE TABLE namespace(" "namespace INT64, PRIMARY KEY(namespace));"
        )
        conn.execute("CREATE (:namespace {namespace: 1});")
        assert list(
            conn.execute("MATCH (namespace:namespace) RETURN namespace.namespace;")
        ) == [[1]]
        assert list(
            conn.execute(
                "MATCH (`namespace`:`namespace`) " "RETURN `namespace`.`namespace`;"
            )
        ) == [[1]]
    finally:
        conn.close()
        db.close()


def test_projected_graph_mutations_are_rejected_in_read_only_mode(tmp_path):
    db_dir = tmp_path / "namespace_read_only_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    conn.execute(
        "CALL project_graph('persisted_graph', ['person'], "
        "{'[person, knows, person]': ''});"
    )
    conn.close()
    db.close()

    read_only_db = Database(db_path=str(db_dir), mode="r")
    read_only_conn = read_only_db.connect()
    try:
        with pytest.raises(Exception):
            read_only_conn.execute(
                "CALL project_graph('blocked_graph', ['person'], "
                "{'[person, knows, person]': ''});"
            )
        with pytest.raises(Exception):
            read_only_conn.execute("CALL drop_projected_graph('persisted_graph');")
    finally:
        read_only_conn.close()
        read_only_db.close()

    reopened_db = Database(db_path=str(db_dir), mode="w")
    reopened_conn = reopened_db.connect()
    try:
        assert _shown_projected_graph_names(reopened_conn) == ["persisted_graph"]
    finally:
        reopened_conn.close()
        reopened_db.close()


def test_projected_graph_persists_after_reopen(tmp_path):
    db_dir = tmp_path / "namespace_reopen_db"
    db = Database(db_path=str(db_dir), mode="w")
    db.load_builtin_dataset("tinysnb")
    conn = db.connect()
    conn.execute(
        "CALL project_graph('adult_graph', {'person': 'n.age > 20'}, "
        "{'[person, knows, person]': ''});"
    )
    conn.close()
    db.close()

    reopened = Database(db_path=str(db_dir), mode="w")
    reopened_conn = reopened.connect()
    try:
        assert _shown_projected_graph_names(reopened_conn) == ["adult_graph"]
        ages = [
            row[0]
            for row in reopened_conn.execute(
                "USE NAMESPACE adult_graph MATCH (n:person) RETURN n.age;"
            )
        ]
        assert ages and all(age > 20 for age in ages)

    finally:
        reopened_conn.close()
        reopened.close()


def test_tp_projected_graph_wal_survives_reopen_without_checkpoint(
    tmp_path, unused_tcp_port
):
    db_dir = tmp_path / "namespace_tp_wal_db"
    seed_db = Database(db_path=str(db_dir), mode="w")
    seed_db.load_builtin_dataset("tinysnb")
    seed_db.close()

    def execute_without_checkpoint(query):
        db = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
        session = None
        try:
            endpoint = db.serve(
                port=unused_tcp_port,
                host="localhost",
                blocking=False,
                auto_compaction=False,
            )
            wait_for_server_ready(endpoint)
            session = Session.open(endpoint, timeout="10s")
            session.execute(query)
        finally:
            if session is not None:
                session.close()
            db.stop_serving()
            db.close()

    execute_without_checkpoint(
        "CALL project_graph('wal_graph', {'person': 'n.age > 20'}, "
        "{'[person, knows, person]': ''});"
    )

    reopened = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    reopened_conn = reopened.connect()
    try:
        assert _shown_projected_graph_names(reopened_conn) == ["wal_graph"]
        assert all(
            row[0] > 20
            for row in reopened_conn.execute(
                "USE NAMESPACE wal_graph MATCH (n:person) RETURN n.age;"
            )
        )
    finally:
        reopened_conn.close()
        reopened.close()
    execute_without_checkpoint("CALL drop_projected_graph('wal_graph');")

    reopened = Database(db_path=str(db_dir), mode="w", checkpoint_on_close=False)
    reopened_conn = reopened.connect()
    try:
        assert _shown_projected_graph_names(reopened_conn) == []
    finally:
        reopened_conn.close()
        reopened.close()
