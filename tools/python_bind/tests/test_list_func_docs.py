#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""End-to-end coverage for cypher_manual/expression/list_func.md."""

import pytest

from neug.database import Database


def _as_list(value):
    if isinstance(value, (str, bytes)):
        return value
    try:
        return [_as_list(item) for item in value]
    except TypeError:
        return value


def _scalar(conn, query, parameters=None):
    return _as_list(list(conn.execute(query, parameters=parameters))[0][0])


@pytest.fixture
def item_connection(tmp_path):
    db = Database(db_path=str(tmp_path), mode="w", checkpoint_on_close=False)
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE Item("
        "id INT64, tags INT64[], coordinates INT64[3], PRIMARY KEY(id));"
    )
    conn.execute(
        "CREATE (:Item {"
        "id: 1, tags: CAST([1, 2], 'INT64[]'), "
        "coordinates: CAST([1, 2, 3], 'INT64[3]')"
        "});"
    )
    yield conn
    conn.close()
    db.close()


def test_list_append_documentation_examples(item_connection):
    conn = item_connection

    assert _scalar(conn, "RETURN list_append([1, 2], 3);") == [1, 2, 3]
    assert _scalar(
        conn,
        "WITH $value AS value RETURN list_append([1, 2], value);",
        parameters={"value": 3},
    ) == [1, 2, 3]
    assert _scalar(conn, "MATCH (item:Item) RETURN list_append(item.tags, 10);") == [
        1,
        2,
        10,
    ]
    assert _scalar(
        conn, "MATCH (item:Item) RETURN list_append(item.coordinates, 10);"
    ) == [1, 2, 3, 10]
    assert _scalar(conn, "RETURN list_append([], 1);") == [1]
    # The property argument prevents constant folding, so the empty list is
    # executed as a zero-child ToList expression at runtime.
    assert _scalar(conn, "MATCH (item:Item) RETURN list_append([], item.id);") == [1]
    assert _scalar(conn, "RETURN list_append([1, 2], NULL);") == [1, 2, None]
    assert _scalar(conn, "RETURN list_append([1, 2], 3.5);") == [1.0, 2.0, 3.5]
    assert _scalar(
        conn,
        "RETURN list_append([[1, 2], [3, 4]], [5, 6]);",
    ) == [[1, 2], [3, 4], [5, 6]]

    with pytest.raises(Exception, match="first argument to be LIST or ARRAY"):
        conn.execute("RETURN list_append(1, 2);")


def test_list_concat_documentation_examples(item_connection):
    conn = item_connection

    assert _scalar(conn, "RETURN list_concat([1, 2], [3, 4]);") == [1, 2, 3, 4]
    assert _scalar(
        conn,
        "MATCH (item:Item) RETURN list_concat(item.tags, item.coordinates);",
    ) == [1, 2, 1, 2, 3]
    assert _scalar(
        conn,
        "MATCH (item:Item) RETURN list_concat(item.coordinates, item.tags);",
    ) == [1, 2, 3, 1, 2]
    assert _scalar(
        conn,
        "MATCH (item:Item) " "RETURN list_concat(item.coordinates, item.coordinates);",
    ) == [1, 2, 3, 1, 2, 3]
    assert _scalar(conn, "RETURN list_concat([1], [2, 3, 4]);") == [1, 2, 3, 4]
    assert _scalar(conn, "RETURN list_concat([], [1, 2]);") == [1, 2]
    # The property argument prevents constant folding, so the empty list is
    # executed as a zero-child ToList expression at runtime.
    assert _scalar(conn, "MATCH (item:Item) RETURN list_concat([], item.tags);") == [
        1,
        2,
    ]
    assert _scalar(conn, "RETURN list_concat([1, 2], []);") == [1, 2]
    assert _scalar(conn, "RETURN list_concat([], []);") == []
    assert _scalar(conn, "RETURN list_concat([1, 2], [3.5, 4.5]);") == [
        1.0,
        2.0,
        3.5,
        4.5,
    ]
    assert _scalar(
        conn,
        "RETURN list_concat([[1, 2]], [[3, 4], [5, 6]]);",
    ) == [[1, 2], [3, 4], [5, 6]]

    with pytest.raises(Exception, match="expects LIST or ARRAY arguments"):
        conn.execute("RETURN list_concat([1], 2);")


def test_list_null_storage_documentation_example(item_connection):
    conn = item_connection

    assert _scalar(conn, "RETURN list_append([1, 2], NULL);") == [1, 2, None]
    conn.execute(
        "CREATE (:Item {"
        "id: 2, tags: list_append([1, 2], NULL), "
        "coordinates: CAST([1, 2, 3], 'INT64[3]')"
        "});"
    )
    assert _scalar(conn, "MATCH (item:Item {id: 2}) RETURN item.tags;") == [1, 2, 0]
