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
from prompt_toolkit.document import Document

from neug import neug_cli
from neug.database import Database


def _token_classes(line):
    lexer = neug_cli.NeugLexer()
    return [style for style, _ in lexer.lex_document(Document(line))(0)]


def test_cli_inline_autosuggestion_casing():
    auto_suggest = neug_cli.NeugAutoSuggest()
    assert auto_suggest.get_suggestion(None, Document(":q")).text == "uit"
    assert auto_suggest.get_suggestion(None, Document("ma")).text == "tch"
    assert auto_suggest.get_suggestion(None, Document("MA")).text == "TCH"
    assert auto_suggest.get_suggestion(None, Document("Ma")).text == "tch"
    assert auto_suggest.get_suggestion(None, Document("m")).text == "atch"
    assert auto_suggest.get_suggestion(None, Document("M")).text == "ATCH"
    assert auto_suggest.get_suggestion(None, Document("RETURN col")).text == "umn"
    assert auto_suggest.get_suggestion(None, Document("ORDER B")).text == "Y"
    assert auto_suggest.get_suggestion(None, Document("IS N")).text == "ULL"


class FakeBuffer:
    def __init__(self, text):
        self.text = text
        self.document = Document(text)
        self.auto_suggest = neug_cli.NeugAutoSuggest()
        self.suggestion = None
        self._neug_completion_state = None
        self.inserted_text = ""

        class SuggestionEvent:
            def fire(self):
                pass

        self.on_suggestion_set = SuggestionEvent()

    def insert_text(self, text):
        self.inserted_text += text
        self.text += text
        self.document = Document(self.text)


def test_cli_tab_cycles_inline_suggestions():
    buffer = FakeBuffer("m")
    assert neug_cli._handle_tab(buffer)
    assert buffer.suggestion.text == "erge"
    assert neug_cli._handle_tab(buffer)
    assert buffer.suggestion.text == "acro"
    assert neug_cli._handle_tab(buffer)
    assert buffer.suggestion.text == "axvalue"


def test_cli_tab_accepts_single_inline_suggestion():
    buffer = FakeBuffer(":q")
    assert neug_cli._handle_tab(buffer)
    assert buffer.inserted_text == "uit"
    assert buffer.text == ":quit"


def test_cli_lexer_highlights_commands_and_cypher_keywords():
    assert "class:command" in _token_classes(":q")
    assert "class:cypher-keyword" in _token_classes("MATCH")
    assert "class:cypher-keyword" in _token_classes("match")
    assert "class:cypher-keyword" in _token_classes("ma")
    assert "class:cypher-keyword" in _token_classes("SKIP")
    assert "class:cypher-keyword" in _token_classes("EXPLAIN")
    assert _token_classes("MATCH (n) RETURN n").count("class:cypher-keyword") == 2


def test_cli_cypher_keywords_align_with_parser_keywords():
    assert "REMOVE" not in neug_cli.CYPHER_CANDIDATES
    assert "SKIP" in neug_cli.CYPHER_CANDIDATES
    for keyword in ["MATCH", "EXPLAIN", "PROFILE", "UNION", "YIELD"]:
        assert keyword in neug_cli.CYPHER_CANDIDATES


def test_shell_do_help(capsys, tmp_path):
    db_path = tmp_path / "test_shell_help_db"
    shutil.rmtree(db_path, ignore_errors=True)
    database = Database(db_path=str(db_path), mode="r")
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
            - Use :ui <endpoint> to start a web ui service on endpoint.
            - Multi-line commands are supported. Use ';' at the end to execute.
            - Command history is supported; use the up/down arrow keys to navigate previous commands.
            - Use Tab to cycle candidates and right arrow to accept the current suggestion.
        """
    assert expected_output.strip() in captured.out.strip()
    connection.close()


def test_shell_do_quit(capsys, tmp_path):
    db_path = tmp_path / "test_shell_quit_db"
    shutil.rmtree(db_path, ignore_errors=True)
    database = Database(db_path=str(db_path), mode="r")
    connection = database.connect()
    shell = neug_cli.NeugShell(connection)
    shell.default(":quit")
    captured = capsys.readouterr()
    assert "Exiting..." in captured.out
    connection.close()


def test_shell_do_max_rows(capsys):
    db_path = "/tmp/modern_graph"
    database = Database(db_path=str(db_path), mode="r")
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
    """
    print("OKKKKKKKKKKk")
    print(captured.out.strip())
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
    database.close()


def test_shell_do_max_rows_invalid(capsys, tmp_path):
    db_path = tmp_path / "test_shell_max_rows_invalid_db"
    shutil.rmtree(db_path, ignore_errors=True)
    database = Database(db_path=str(db_path), mode="r")
    connection = database.connect()
    shell = neug_cli.NeugShell(connection)
    shell.default(":max_rows -1")
    captured = capsys.readouterr()
    assert "max_rows must be a positive integer." in captured.out
    connection.close()


def test_shell_do_query(capsys):
    db_path = "/tmp/modern_graph"
    database = Database(db_path=str(db_path), mode="r")
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
    database = Database(db_path=str(db_path), mode="r")
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
    shutil.rmtree(db_path, ignore_errors=True)
    database = Database(db_path=str(db_path), mode="rw")
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
