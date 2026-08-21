#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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

import pytest

from neug.database import Database


def _nested_list(value):
    if isinstance(value, (str, bytes)):
        return value
    try:
        return [_nested_list(item) for item in value]
    except TypeError:
        return value


def test_list_append_and_concat(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    cases = [
        ("RETURN list_append([1, 2], 3);", [1, 2, 3]),
        ("RETURN list_append([1, 2], 3.5);", [1.0, 2.0, 3.5]),
        ("RETURN list_append([], 1);", [1]),
        ("RETURN list_append([], NULL);", [None]),
        ("RETURN list_append(CAST([1, 2], 'INT64[]'), 3);", [1, 2, 3]),
        ("RETURN list_concat([1, 2], [3, 4]);", [1, 2, 3, 4]),
        (
            "RETURN list_concat(CAST([1, 2], 'INT64[]'), [3, 4]);",
            [1, 2, 3, 4],
        ),
        (
            "RETURN list_concat([1, 2], CAST([3, 4], 'INT64[]'));",
            [1, 2, 3, 4],
        ),
        ("RETURN list_concat([1, 2], [3.5, 4.5]);", [1.0, 2.0, 3.5, 4.5]),
        ("RETURN list_concat([], [1, 2]);", [1, 2]),
        ("RETURN list_concat([1, 2], []);", [1, 2]),
        ("RETURN list_concat([], []);", []),
        ("RETURN list_append([1, 2], NULL);", [1, 2, None]),
        (
            "RETURN list_append([[1, 2], [3, 4]], [5, 6]);",
            [[1, 2], [3, 4], [5, 6]],
        ),
        (
            "RETURN list_concat([[1, 2]], [[3, 4], [5, 6]]);",
            [[1, 2], [3, 4], [5, 6]],
        ),
        (
            "RETURN list_append(CAST([1, CAST(NULL, 'INT64'), 3], 'INT64[]'), 4);",
            [1, None, 3, 4],
        ),
        (
            "RETURN list_concat("
            "CAST([1, CAST(NULL, 'INT64')], 'INT64[]'), "
            "CAST([2, CAST(NULL, 'INT64')], 'INT64[]'));",
            [1, None, 2, None],
        ),
    ]
    for query, expected in cases:
        value = list(conn.execute(query))[0][0]
        assert _nested_list(value) == expected

    # A typed top-level NULL list propagates to a NULL result.
    assert list(conn.execute("RETURN list_append(CAST(NULL, 'INT64[]'), 3);")) == [
        [None]
    ]
    assert list(conn.execute("RETURN list_concat(CAST(NULL, 'INT64[]'), [1]);")) == [
        [None]
    ]

    with pytest.raises(Exception, match="first argument to be LIST or ARRAY"):
        conn.execute("RETURN list_append(1, 2);")
    with pytest.raises(Exception, match="cannot find a common element type"):
        conn.execute("RETURN list_append([1, 2], [3]);")
    with pytest.raises(Exception, match="expects LIST or ARRAY arguments"):
        conn.execute("RETURN list_concat([1], 2);")

    conn.close()
    db.close()


def test_list_append_and_concat_properties_and_nulls(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Item("
        "id INT64, list_values INT64[], array_values INT64[3], PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE (:Item {"
        "id: 1, "
        "list_values: CAST([1, 2, 3], 'INT64[]'), "
        "array_values: CAST([4, 5, 6], 'INT64[3]')"
        "});"
    )
    conn.execute(
        "CREATE (:Item {"
        "id: 2, "
        "list_values: CAST([], 'INT64[]'), "
        "array_values: CAST([7, 8, 9], 'INT64[3]')"
        "});"
    )

    rows = list(
        conn.execute(
            "MATCH (item:Item) "
            "RETURN item.id, "
            "list_append(item.list_values, 10), "
            "list_concat(item.list_values, item.array_values), "
            "list_append(item.array_values, NULL) "
            "ORDER BY item.id;"
        )
    )
    assert [row[0] for row in rows] == [1, 2]
    assert [_nested_list(value) for value in rows[0][1:]] == [
        [1, 2, 3, 10],
        [1, 2, 3, 4, 5, 6],
        [4, 5, 6, None],
    ]
    assert [_nested_list(value) for value in rows[1][1:]] == [
        [10],
        [7, 8, 9],
        [7, 8, 9, None],
    ]

    conn.close()
    db.close()


def test_list_cast_contract(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    assert _nested_list(list(conn.execute("RETURN CAST([], 'STRING[]');"))[0][0]) == []
    assert _nested_list(
        list(conn.execute("RETURN CAST([1, 2, 3], 'INT64[]');"))[0][0]
    ) == [1, 2, 3]
    assert _nested_list(
        list(
            conn.execute(
                "UNWIND [1, 2, 3] AS v WITH collect(v) AS values "
                "RETURN CAST(values, 'INT64[3]');"
            )
        )[0][0]
    ) == [1, 2, 3]

    with pytest.raises(Exception):
        conn.execute(
            "UNWIND [1, 2] AS v WITH collect(v) AS values "
            "RETURN CAST(values, 'INT64[3]');"
        )
    with pytest.raises(Exception):
        conn.execute("RETURN CAST([], 'INT64[2]');")

    conn.execute("CREATE NODE TABLE T(id INT64, values INT64[], PRIMARY KEY(id));")
    with pytest.raises(Exception):
        conn.execute("CREATE (:T {id: 1, values: [1, 2]});")
    with pytest.raises(Exception):
        conn.execute("CREATE NODE TABLE Bad(id INT64[], PRIMARY KEY(id));")

    conn.close()
    db.close()


def test_list_preserves_null_elements_during_unwind(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    rows = list(conn.execute("RETURN CAST(NULL, 'INT64[]');"))
    assert rows == [[None]]

    rows = list(
        conn.execute(
            "UNWIND CAST([1, CAST(NULL, 'INT64'), 3], 'INT64[]') AS value "
            "RETURN value;"
        )
    )
    assert rows == [[1], [None], [3]]

    rows = list(
        conn.execute(
            "UNWIND CAST([1, CAST(NULL, 'INT64'), 3], 'INT64[]') AS value "
            "RETURN collect(value);"
        )
    )
    assert _nested_list(rows[0][0]) == [1, 3]

    rows = list(
        conn.execute(
            "UNWIND CAST([CAST([1, CAST(NULL, 'INT64'), 3], 'INT64[]')], "
            "'INT64[][]') AS values "
            "UNWIND values AS value RETURN value;"
        )
    )
    assert rows == [[1], [None], [3]]

    conn.close()
    db.close()


def test_list_edge_preserves_null_elements_during_unwind(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person);")
    conn.execute("CREATE (:Person {id: 1}), (:Person {id: 2});")
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) " "CREATE (a)-[:Knows]->(b);"
    )

    rows = list(
        conn.execute(
            "MATCH (person:Person) "
            "OPTIONAL MATCH (person)-[edge:Knows]->(:Person) "
            "WITH person.id AS id, [edge] AS values "
            "UNWIND values AS value "
            "RETURN id, value ORDER BY id;"
        )
    )
    assert rows == [
        [
            1,
            {
                "_ID": 1,
                "_LABEL": "Knows",
                "_SRC_ID": 0,
                "_DST_ID": 1,
            },
        ],
        [2, None],
    ]

    conn.close()
    db.close()


def test_list_vertex_preserves_null_elements_during_unwind(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()

    conn.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id));")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person);")
    conn.execute("CREATE (:Person {id: 1}), (:Person {id: 2});")
    create_edge = "MATCH (a:Person {id: 1}), "
    create_edge += "(b:Person {id: 2}) CREATE (a)-[:Knows]->(b);"
    conn.execute(create_edge)

    rows = list(
        conn.execute(
            "MATCH (person:Person) "
            "OPTIONAL MATCH (person)-[:Knows]->(friend:Person) "
            "WITH person.id AS id, [friend] AS values "
            "UNWIND values AS value "
            "RETURN id, value ORDER BY id;"
        )
    )
    assert rows == [
        [1, {"_ID": 1, "_LABEL": "Person", "id": 2}],
        [2, None],
    ]

    conn.close()
    db.close()


def test_list_point_edge_update_and_reopen(tmp_path):
    db_path = str(tmp_path)
    db = Database(db_path=db_path, mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE Person("
        "id INT64, tags STRING[], nested STRING[][2][], PRIMARY KEY(id));"
    )
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person, scores INT64[]);")
    conn.execute(
        "CREATE (:Person {id: 1, tags: CAST(['a'], 'STRING[]'), nested: "
        "CAST([CAST([CAST(['x'], 'STRING[]'), CAST([], 'STRING[]')], "
        "'STRING[][2]')], 'STRING[][2][]')});"
    )
    conn.execute(
        "CREATE (:Person {id: 2, tags: CAST([], 'STRING[]'), "
        "nested: CAST([], 'STRING[][2][]')});"
    )
    conn.execute(
        "MATCH (a:Person {id: 1}), (b:Person {id: 2}) "
        "CREATE (a)-[:Knows {scores: CAST([1, 2], 'INT64[]')}]->(b);"
    )

    conn.execute("MATCH (p:Person {id: 1}) SET p.tags = CAST(['b'], 'STRING[]');")
    conn.execute(
        "MATCH (p:Person {id: 1}) " "SET p.tags = CAST(['c', 'd', 'e'], 'STRING[]');"
    )
    conn.execute(
        "MATCH (:Person {id: 1})-[e:Knows]->(:Person {id: 2}) "
        "SET e.scores = CAST([7], 'INT64[]');"
    )
    conn.execute(
        "MERGE (p:Person {id: 2}) "
        "ON MATCH SET p.tags = CAST(['merged'], 'STRING[]');"
    )

    row = list(
        conn.execute("MATCH (p:Person {id: 1}) RETURN p.tags, p.tags[1], p.nested;")
    )[0]
    assert _nested_list(row[0]) == ["c", "d", "e"]
    assert row[1] == "d"
    assert _nested_list(row[2]) == [[["x"], []]]
    assert _nested_list(
        list(
            conn.execute(
                "MATCH (p:Person) WITH p ORDER BY p.id " "RETURN collect(p.tags);"
            )
        )[0][0]
    ) == [["c", "d", "e"], ["merged"]]
    assert [
        item[0]
        for item in conn.execute(
            "MATCH (p:Person {id: 1}) UNWIND p.tags AS tag " "RETURN tag ORDER BY tag;"
        )
    ] == ["c", "d", "e"]
    assert [
        item[0]
        for item in conn.execute(
            "MATCH (p:Person {id: 1}) UNWIND p.nested AS pair "
            "UNWIND pair[0] AS value RETURN value;"
        )
    ] == ["x"]

    conn.close()
    db.close()

    db = Database(db_path=db_path, mode="w", checkpoint_on_close=False)
    conn = db.connect()
    assert _nested_list(
        list(conn.execute("MATCH (p:Person {id: 1}) RETURN p.tags;"))[0][0]
    ) == ["c", "d", "e"]
    assert _nested_list(
        list(
            conn.execute(
                "MATCH (:Person {id: 1})-[e:Knows]->(:Person {id: 2}) "
                "RETURN e.scores;"
            )
        )[0][0]
    ) == [7]

    conn.close()
    db.close()


def test_return_single_list(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, name STRING, score FLOAT, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p: PERSON {id: 0, name: 'Alice', score: 99.5});")
    conn.execute("CREATE (p: PERSON {id: 1, name: 'Bob', score: 98.5});")

    result = conn.execute(
        "MATCH (p: PERSON) RETURN [p.id, p.name, p.score] ORDER BY p.id;"
    )
    result = list(result)
    assert result[0][0] == [0, "Alice", 99.5]
    assert result[1][0] == [1, "Bob", 98.5]

    conn.close()
    db.close()


def test_return_multiple_lists(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, name STRING, score FLOAT, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p: PERSON {id: 0, name: 'Alice', score: 99.5});")
    conn.execute("CREATE (p: PERSON {id: 1, name: 'Bob', score: 98.5});")

    result = conn.execute(
        "MATCH (p: PERSON) RETURN [p.id], [p.name, p.score] ORDER BY p.id;"
    )
    result = list(result)
    assert result[0][0] == [0]
    assert result[0][1] == ["Alice", 99.5]
    assert result[1][0] == [1]
    assert result[1][1] == ["Bob", 98.5]

    conn.close()
    db.close()
