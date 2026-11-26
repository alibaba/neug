# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Literal string matching, including line terminators (issue #1259)."""

import pytest
from conftest import cypher_path

# Expected results are in CONTAINS, STARTS WITH, ENDS WITH order.
CASES = [
    ("alpha\nrhprobe", "rhprobe", [True, False, True]),
    ("alpha\rrhprobe", "alpha", [True, True, False]),
    ("alpha\r\nrhprobe", "a\r\nr", [True, False, False]),
    ("\nalpha\r", "\nalpha", [True, True, False]),
    ("\nalpha\r", "alpha\r", [True, False, True]),
    ("alpha\nrhprobe", "", [True, True, True]),
    ("alpha\rrhprobe", "", [True, True, True]),
    ("", "", [True, True, True]),
    ("", "alpha", [False, False, False]),
    ("alpha", "alpha", [True, True, True]),
    ("alpha", "alphabeta", [False, False, False]),
    ("alpha\nbeta", "absent", [False, False, False]),
    ("Alpha", "alpha", [False, False, False]),
    ("x.*[a](b){c}^$|?+\\y", ".*[a](b){c}^$|?+\\", [True, False, False]),
    (".*[", ".*[", [True, True, True]),
    ("axb", "a.b", [False, False, False]),
    ("你好\n世界", "世界", [True, False, True]),
    ("alpha\tbeta\u2028gamma\u2029", "gamma\u2029", [True, False, True]),
    (None, "", [None, None, None]),
    ("alpha", None, [None, None, None]),
    (None, None, [None, None, None]),
]


def expressions(value, pattern, syntax):
    if syntax == "operator":
        return [
            f"{value} {operator} {pattern}"
            for operator in ("CONTAINS", "STARTS WITH", "ENDS WITH")
        ]
    return [
        f"{function}({value}, {pattern})"
        for function in ("CONTAINS", "STARTS_WITH", "ENDS_WITH")
    ]


def literal(value):
    return "NULL" if value is None else cypher_path(value)


@pytest.mark.parametrize("syntax", ["operator", "function"])
@pytest.mark.parametrize("value,pattern,expected", CASES)
def test_string_predicate_literals(empty_db, syntax, value, pattern, expected):
    _, conn = empty_db
    query = ", ".join(expressions(literal(value), literal(pattern), syntax))
    assert list(conn.execute(f"RETURN {query};")) == [expected]


@pytest.mark.parametrize("syntax", ["operator", "function"])
def test_string_predicate_properties_and_filters(empty_db, syntax):
    _, conn = empty_db
    conn.execute(
        "CREATE NODE TABLE Note(id INT64 PRIMARY KEY, text STRING, pattern STRING);"
    )
    for index, (value, pattern, _) in enumerate(CASES):
        conn.execute(
            f"CREATE (:Note {{id: {index}, text: {literal(value)}, "
            f"pattern: {literal(pattern)}}});"
        )
    predicates = expressions("n.text", "n.pattern", syntax)
    assert list(
        conn.execute(f"MATCH (n:Note) RETURN {', '.join(predicates)} ORDER BY n.id;")
    ) == [expected for _, _, expected in CASES]
    for column, predicate in enumerate(predicates):
        assert list(
            conn.execute(f"MATCH (n:Note) WHERE {predicate} RETURN n.id ORDER BY n.id;")
        ) == [[i] for i, (_, _, expected) in enumerate(CASES) if expected[column]]


@pytest.mark.parametrize("syntax", ["operator", "function"])
def test_string_predicate_parameters(empty_db, syntax):
    _, conn = empty_db
    predicates = ", ".join(expressions("$value", "$pattern", syntax))
    # Reuse the query with different parameters, including a pattern with LF.
    for value, pattern, expected in CASES:
        if value is None or pattern is None:
            continue
        assert list(
            conn.execute(
                f"RETURN {predicates};", parameters={"value": value, "pattern": pattern}
            )
        ) == [expected]


@pytest.mark.parametrize("syntax", ["operator", "function"])
def test_string_predicate_edge_filter(empty_db, syntax):
    _, conn = empty_db
    conn.execute("CREATE NODE TABLE Note(id INT64 PRIMARY KEY);")
    conn.execute("CREATE REL TABLE Link(FROM Note TO Note, text STRING);")
    conn.execute("CREATE (:Note {id: 0}), (:Note {id: 1});")
    conn.execute(
        "MATCH (a:Note {id: 0}), (b:Note {id: 1}) "
        "CREATE (a)-[:Link {text: 'alpha\r\nrhprobe'}]->(b);"
    )
    for predicate in expressions("r.text", "''", syntax):
        assert list(
            conn.execute(
                f"MATCH (:Note)-[r:Link]->(:Note) WHERE {predicate} RETURN r.text;"
            )
        ) == [["alpha\r\nrhprobe"]]
