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

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
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


@pytest.mark.skip(reason="list nesting is not supported")
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
