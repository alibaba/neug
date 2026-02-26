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

import ctypes
import os
import shutil
import sys
import threading
import time

import pytest
import requests
from click.testing import CliRunner

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from neug import neug_cli
from neug import web_ui
from neug.database import Database
from neug.session import Session


@pytest.fixture
def runner():
    return CliRunner()


def test_help_option(runner):
    result = runner.invoke(neug_cli.cli, ["--help"])
    assert result.exit_code == 0
    expected_output = """\
Usage: neug-cli [OPTIONS] COMMAND [ARGS]...

  Neug CLI Tool.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  connect  Connect to a remote database.
  open     Open a local database.
"""
    assert expected_output.strip() == result.output.strip()


def test_open_local_database(monkeypatch, runner, tmp_path):
    db_path = tmp_path / "test_open_local_db"
    shutil.rmtree(db_path, ignore_errors=True)
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result = runner.invoke(neug_cli.cli, ["open", str(db_path)])
    assert result.exit_code == 0
    assert f"Opened database at {db_path} in read-write mode" in result.output


def test_open_local_database_readonly(monkeypatch, runner, tmp_path):
    db_path = tmp_path / "test_open_local_db_readonly"
    shutil.rmtree(db_path, ignore_errors=True)
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result1 = runner.invoke(neug_cli.cli, ["open", str(db_path), "-m", "read-only"])
    assert result1.exit_code == 0
    assert f"Opened database at {db_path} in read-only mode" in result1.output
    result2 = runner.invoke(neug_cli.cli, ["open", str(db_path), "-m", "r"])
    assert result2.exit_code == 0
    assert f"Opened database at {db_path} in r mode" in result2.output


def test_open_pure_memory_database(monkeypatch, runner):
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result = runner.invoke(neug_cli.cli, ["open"])
    assert result.exit_code == 0
    assert "Opened in-memory database in read-write mode" in result.output


def test_connect_help_option(monkeypatch, runner):
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result = runner.invoke(neug_cli.cli, ["connect", "--help"])
    assert result.exit_code == 0
    excepted_output = """\
Usage: neug-cli connect [OPTIONS] DB_URI

  Connect to a remote database.

Options:
  --timeout INTEGER  Connection timeout in seconds.  [default: 300]
  --help             Show this message and exit.
"""
    assert excepted_output.strip() == result.output.strip()


@pytest.mark.skip(
    "Skipping remote database connection test as it is not supported yet."
)
def test_connect_remote_database(monkeypatch, runner):
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result = runner.invoke(neug_cli.cli, ["connect", "localhost:7687"])
    assert result.exit_code == 0
    assert "Connecting to localhost:7687" in result.output


@pytest.mark.skip(
    "Skipping remote database connection with authentication test as it is not supported yet."
)
def test_connect_remote_database_with_auth(monkeypatch, runner):
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result = runner.invoke(
        neug_cli.cli, ["connect", "localhost:7687", "-u", "user", "-p", "password"]
    )
    assert result.exit_code == 0
    assert "Connecting to localhost:7687" in result.output


@pytest.mark.skip(
    "Skipping remote database connection with timeout test as it is not supported yet."
)
def test_connect_remote_database_with_timeout(monkeypatch, runner):
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result = runner.invoke(
        neug_cli.cli, ["connect", "localhost:7687", "--timeout", "100"]
    )
    assert result.exit_code == 0
    assert "Connecting to localhost:7687" in result.output


def test_connect_remote_database_fails_without_uri(runner):
    result = runner.invoke(neug_cli.cli, ["connect"])
    assert result.exit_code != 0
    assert "Error: Missing argument 'DB_URI'." in result.output


def test_start_remote_database_neug_ui(runner):
    db_endpoint = "http://127.0.0.1:10010"
    db_path = "/tmp/modern_graph"
    db = Database(db_path=db_path, mode="w")
    db.serve(host="127.0.0.1", port=10010, blocking=False)

    time.sleep(1)
    session = Session(endpoint=db_endpoint)
    shell = neug_cli.NeugShell(session)
    thread = threading.Thread(
        target=shell.default,
        args=(":ui",),
    )
    thread.start()
    time.sleep(1)
    url = "http://127.0.0.1:5000/cypherv2"
    headers = {"Content-Type": "text/plain"}
    data = "MATCH (n) RETURN count(n)"
    response = requests.post(url, headers=headers, data=data)
    db.stop_serving()
    db.close()

    if thread.is_alive():
        tid = thread.ident
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(tid), ctypes.py_object(SystemExit)
        )
    thread.join()
    expected_output = """COUNT(_0_n)": 6"""
    assert response.status_code == 200
    assert expected_output in response.text


def test_start_local_database_neug_ui(runner):
    db_path = "/tmp/modern_graph"
    db = Database(db_path=db_path, mode="w")
    connection = db.connect()
    shell = neug_cli.NeugShell(connection)
    thread = threading.Thread(
        target=shell.default,
        args=(":ui",),
    )
    thread.start()
    time.sleep(1)
    url = "http://127.0.0.1:5000/cypherv2"
    headers = {"Content-Type": "text/plain"}
    data = "MATCH (n) RETURN count(n)"
    response = requests.post(url, headers=headers, data=data)
    db.close()
    if thread.is_alive():
        tid = thread.ident
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(tid), ctypes.py_object(SystemExit)
        )
    thread.join()
    expected_output = """COUNT(_0_n)": 6"""
    assert response.status_code == 200
    assert expected_output in response.text
