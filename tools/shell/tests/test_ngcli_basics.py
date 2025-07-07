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

import pytest
from neug.database import Database

from ngcli import neug_cli


def test_shell_do_help(capsys, tmp_path):
    db_path = tmp_path / "test_shell_help_db"
    database = Database(db_path=str(db_path), mode="r", planner="gopt")
    connection = database.connect()
    shell = neug_cli.NeugShell(connection)
    shell.default(":help")
    captured = capsys.readouterr()
    expected_output = """
            Usage hints:
            - Enter Cypher queries directly to execute them on the connected database.
            - Use :help to display this help message.
            - Use :quit to leave the shell.
            - Use :max_rows <number> to set the maximum number of rows to display for query results.
            - Multi-line commands are supported. Use ';' at the end to execute.
        """
    assert expected_output.strip() in captured.out.strip()
    connection.close()


def test_shell_do_quit(capsys, tmp_path):
    db_path = tmp_path / "test_shell_quit_db"
    database = Database(db_path=str(db_path), mode="r", planner="gopt")
    connection = database.connect()
    shell = neug_cli.NeugShell(connection)
    shell.default(":quit")
    captured = capsys.readouterr()
    assert "Exiting..." in captured.out
    connection.close()


def test_shell_do_max_rows(capsys):
    db_path = "/tmp/modern_graph"
    database = Database(db_path=str(db_path), mode="r", planner="gopt")
    connection = database.connect()
    shell = neug_cli.NeugShell(connection)
    shell.default("MATCH (n) RETURN n;")
    captured = capsys.readouterr()
    expected_output = """
+-----------------------------------------------------------------------------+
| n                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: person, id: 2, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: person, id: 4, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: person, id: 6, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: software, id: 3, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: software, id: 5, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
    """
    assert expected_output.strip() in captured.out.strip()
    # Set max_rows to 1
    shell.default(":max_rows 1")
    captured = capsys.readouterr()
    assert "Set max_rows to 1" in captured.out
    shell.default("MATCH (n) RETURN n;")
    captured = capsys.readouterr()
    expected_output = """
+-------------------------------------------------------+
| n                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, id: 1, name: marko, age: 29} |
+-------------------------------------------------------+
| ...                                                   |
+-------------------------------------------------------+
    """
    connection.close()


def test_shell_do_max_rows_invalid(capsys, tmp_path):
    db_path = tmp_path / "test_shell_max_rows_invalid_db"
    database = Database(db_path=str(db_path), mode="r", planner="gopt")
    connection = database.connect()
    shell = neug_cli.NeugShell(connection)
    shell.default(":max_rows -1")
    captured = capsys.readouterr()
    assert "max_rows must be a positive integer." in captured.out
    connection.close()


def test_shell_do_query(capsys):
    db_path = "/tmp/modern_graph"
    database = Database(db_path=str(db_path), mode="r", planner="gopt")
    connection = database.connect()
    shell = neug_cli.NeugShell(connection)
    shell.default("MATCH (n) where n.name = 'marko' RETURN n;")
    captured = capsys.readouterr()
    expected_output = """
+-------------------------------------------------------+
| n                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, id: 1, name: marko, age: 29} |
+-------------------------------------------------------+
    """
    assert expected_output.strip() == captured.out.strip()
    shell.default("match (n) where n.name= 'marko' return n.id, n.name, n.age;")
    captured = capsys.readouterr()
    expected_output = """
+-----------+-------------+------------+
|   _0_n.id | _0_n.name   |   _0_n.age |
+===========+=============+============+
|         1 | marko       |         29 |
+-----------+-------------+------------+
    """
    assert expected_output.strip() == captured.out.strip()
    shell.default(
        "match (n) where n.name= 'marko' return n.id as id, n.name as name, n.age as age;"
    )
    captured = capsys.readouterr()
    expected_output = """
+------+--------+-------+
|   id | name   |   age |
+======+========+=======+
|    1 | marko  |    29 |
+------+--------+-------+
    """
    assert expected_output.strip() == captured.out.strip()
    connection.close()


def test_shell_do_query_multiline(capsys):
    db_path = "/tmp/modern_graph"
    database = Database(db_path=str(db_path), mode="r", planner="gopt")
    connection = database.connect()
    shell = neug_cli.NeugShell(connection)
    shell.default("MATCH (n) where n.name = 'marko'")
    shell.default("RETURN n;")
    captured = capsys.readouterr()
    expected_output = """
+-------------------------------------------------------+
| n                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, id: 1, name: marko, age: 29} |
+-------------------------------------------------------+
    """
    assert expected_output.strip() == captured.out.strip()
    connection.close()


def test_shell_do_query_in_write_mode(capsys, tmp_path):
    db_path = tmp_path / "test_shell_query_write_db"
    database = Database(db_path=str(db_path), mode="rw", planner="gopt")
    connection = database.connect()
    shell = neug_cli.NeugShell(connection)
    # Attempt to query a non-existing table, return error
    shell.default("MATCH (n: person123) RETURN n;")
    captured = capsys.readouterr()
    assert "Failed to execute query" in captured.out.strip()
    assert "Table person123 does not exist." in captured.out.strip()
    # Create the new node table and query it
    shell.default(
        "CREATE NODE TABLE person123 (name STRING, age INT,  PRIMARY KEY (name));"
    )
    shell.default("MATCH (n: person123) RETURN n;")
    captured = capsys.readouterr()
    # Check that the query returns no failure
    assert "Failed to execute query" not in captured.out.strip()
    connection.close()
