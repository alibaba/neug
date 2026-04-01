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
#

import os
import shutil
import sys

import pytest

from neug.database import Database


def test_return_single_list(tmp_path):
    db_dir = tmp_path / "return_list"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, name STRING, score FLOAT, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p: PERSON {id: 0, name: 'Alice', score: 99.5});")
    conn.execute("CREATE (p: PERSON {id: 1, name: 'Bob', score: 98.5});")

    result = conn.execute("MATCH (p: PERSON) RETURN [p.id, p.name, p.score];")
    result = list(result)
    assert result[0][0] == [0, "Alice", 99.5]
    assert result[1][0] == [1, "Bob", 98.5]

    conn.close()
    db.close()


def test_return_multiple_lists(tmp_path):
    db_dir = tmp_path / "return_multiple_lists"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, name STRING, score FLOAT, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p: PERSON {id: 0, name: 'Alice', score: 99.5});")
    conn.execute("CREATE (p: PERSON {id: 1, name: 'Bob', score: 98.5});")

    result = conn.execute("MATCH (p: PERSON) RETURN [p.id], [p.name, p.score];")
    result = list(result)
    assert result[0][0] == [0]
    assert result[0][1] == ["Alice", 99.5]
    assert result[1][0] == [1]
    assert result[1][1] == ["Bob", 98.5]

    conn.close()
    db.close()


@pytest.mark.skip(
    reason="heterogeneous nested list (mixed child types) becomes STRUCT not LIST"
)
def test_return_nesting_lists(tmp_path):
    db_dir = tmp_path / "return_nesting_lists"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, name STRING, score FLOAT, PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p: PERSON {id: 0, name: 'Alice', score: 99.5});")
    conn.execute("CREATE (p: PERSON {id: 1, name: 'Bob', score: 98.5});")

    result = conn.execute("MATCH (p: PERSON) RETURN [[p.id], [p.name, p.score]];")
    result = list(result)
    assert result[0][0] == [[0], ["Alice", 99.5]]
    assert result[1][0] == [[1], ["Bob", 98.5]]

    conn.close()
    db.close()


def test_return_nested_list_literal(tmp_path):
    """Homogeneous nested list literal: RETURN [[1],[2,3]]"""
    db_dir = tmp_path / "return_nested_literal"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    result = conn.execute("RETURN [[1, 2], [3, 4]];")
    result = list(result)
    assert result[0][0] == [[1, 2], [3, 4]]

    conn.close()
    db.close()


def test_nested_list(tmp_path):
    db_dir = tmp_path / "nested_list"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE PERSON(id INT64, string_prop STRING[][], PRIMARY KEY(id));"
    )
    conn.execute("CREATE (p: PERSON {id: 0, string_prop: [['a', 'b'], ['c', 'd']]} );")
    conn.execute("CREATE (p: PERSON {id: 1, string_prop: [['e', 'f'], ['g', 'h']]} );")

    result = conn.execute("MATCH (p: PERSON) RETURN p.string_prop;")
    result = list(result)
    assert result[0][0] == [["a", "b"], ["c", "d"]]
    assert result[1][0] == [["e", "f"], ["g", "h"]]

    conn.close()
    db.close()


def test_nested_list_default_value(tmp_path):
    db_dir = tmp_path / "nested_list_default"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE PERSON("
        "id INT64 PRIMARY KEY, "
        "list_prop STRING[][] DEFAULT [['x'], ['y', 'z']]);"
    )

    conn.execute("CREATE (p: PERSON {id: 0});")
    conn.execute("CREATE (p: PERSON {id: 1, list_prop: [['a', 'b'], ['c']]} );")

    result = conn.execute("MATCH (p: PERSON) RETURN p.id, p.list_prop ORDER BY p.id;")
    rows = list(result)
    assert rows[0] == [
        0,
        [["x"], ["y", "z"]],
    ], f"expected nested default, got {rows[0]}"
    assert rows[1] == [
        1,
        [["a", "b"], ["c"]],
    ], f"expected explicit nested list, got {rows[1]}"

    conn.close()
    db.close()


def test_list_as_primary_key_rejected(tmp_path):
    """Test that LIST type cannot be used as primary key."""
    db_dir = tmp_path / "list_pk_rejection"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()
    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    with pytest.raises(Exception) as excinfo:
        conn.execute("CREATE NODE TABLE BadTable(id INT64[] PRIMARY KEY);")
    # Verify error message mentions invalid primary key type
    assert "primary key" in str(excinfo.value).lower()

    conn.close()
    db.close()


def test_list_wal_replay(tmp_path):
    """List properties survive WAL replay after DB restart."""
    db_dir = tmp_path / "list_wal"
    shutil.rmtree(db_dir, ignore_errors=True)
    db_dir.mkdir()

    db = Database(db_path=str(db_dir), mode="w")
    conn = db.connect()

    conn.execute(
        "CREATE NODE TABLE ITEM("
        "id INT64 PRIMARY KEY, "
        "tags STRING[], "
        "scores DOUBLE[], "
        "matrix INT64[][]);"
    )
    conn.execute(
        "CREATE (i: ITEM {id: 1, tags: ['a', 'b'], "
        "scores: [1.5, 2.5], matrix: [[10, 20], [30]]});"
    )
    conn.execute("CREATE (i: ITEM {id: 2, tags: ['c'], scores: [3.0], matrix: [[1]]});")

    conn.close()
    db.close()

    db = Database(db_path=str(db_dir), mode="r")
    conn = db.connect()

    result = list(
        conn.execute(
            "MATCH (i: ITEM) RETURN i.id, i.tags, i.scores, i.matrix ORDER BY i.id;"
        )
    )
    assert result[0] == [1, ["a", "b"], [1.5, 2.5], [[10, 20], [30]]]
    assert result[1] == [2, ["c"], [3.0], [[1]]]

    conn.close()
    db.close()
