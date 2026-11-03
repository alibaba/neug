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
import socket
import sys
import threading
from contextlib import contextmanager

import pytest
import requests
from click.testing import CliRunner
from conftest import wait_for_server_ready
from werkzeug.serving import make_server

from neug import neug_cli
from neug import web_ui
from neug.database import Database
from neug.session import Session


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def run_web_ui(monkeypatch):
    if sys.platform == "darwin":
        # Avoid the 35s getfqdn lookup between bind() and listen() on macOS CI.
        # https://github.com/actions/runner-images/issues/14409#issuecomment-5034633535
        monkeypatch.setattr(socket, "getfqdn", lambda name="": name)

    @contextmanager
    def run(shell):
        server = None
        ready = threading.Event()

        def run_server(app, host, port, **kwargs):
            nonlocal server
            server = make_server(host, port, app, threaded=True)
            ready.set()
            server.serve_forever()

        monkeypatch.setattr(web_ui.Flask, "run", run_server)
        thread = threading.Thread(target=shell.default, args=(":ui",), daemon=True)
        thread.start()
        try:
            assert ready.wait(timeout=10), "Web UI did not start listening"
            yield
        finally:
            if server is not None:
                if thread.is_alive():
                    server.shutdown()
                server.server_close()
            thread.join(timeout=5)
            assert not thread.is_alive(), "Web UI thread did not stop"

    return run


def _install_dummy_shell(monkeypatch):
    class DummyShell:
        def __init__(self, connection):
            self.connection = connection

        def cmdloop(self):
            self.connection.close()

    monkeypatch.setattr(neug_cli, "NeugShell", DummyShell)


def _install_dummy_database(monkeypatch):
    class DummyConnection:
        def close(self):
            pass

    class DummyDatabase:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def connect(self):
            return DummyConnection()

    monkeypatch.setattr(neug_cli, "Database", DummyDatabase)


def _install_dummy_open_runtime(monkeypatch):
    _install_dummy_database(monkeypatch)
    _install_dummy_shell(monkeypatch)


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

  Run 'neug-cli COMMAND --help' for more information on a command.

  e.g. neug-cli open --help
"""
    assert expected_output.strip() == result.output.strip()


def test_open_local_database(monkeypatch, runner, tmp_path):
    db_path = tmp_path / "test_open_local_db"
    shutil.rmtree(db_path, ignore_errors=True)
    _install_dummy_open_runtime(monkeypatch)
    result = runner.invoke(neug_cli.cli, ["open", str(db_path)])
    assert result.exit_code == 0
    assert f"Opened database at {db_path} in read-write mode" in result.output


def test_open_local_database_readonly(monkeypatch, runner, tmp_path):
    db_path = tmp_path / "test_open_local_db_readonly"
    shutil.rmtree(db_path, ignore_errors=True)
    _install_dummy_open_runtime(monkeypatch)
    result1 = runner.invoke(neug_cli.cli, ["open", str(db_path), "-m", "read-only"])
    assert result1.exit_code == 0
    assert f"Opened database at {db_path} in read-only mode" in result1.output
    result2 = runner.invoke(neug_cli.cli, ["open", str(db_path), "-m", "r"])
    assert result2.exit_code == 0
    assert f"Opened database at {db_path} in read-only mode" in result2.output


def test_open_pure_memory_database(monkeypatch, runner):
    _install_dummy_open_runtime(monkeypatch)
    result = runner.invoke(neug_cli.cli, ["open"])
    assert result.exit_code == 0
    assert "Opened in-memory database in read-write mode" in result.output


def test_connect_help_option(monkeypatch, runner):
    _install_dummy_shell(monkeypatch)
    result = runner.invoke(neug_cli.cli, ["connect", "--help"])
    assert result.exit_code == 0
    excepted_output = """\
Usage: neug-cli connect [OPTIONS] DB_URI

  Connect to a remote database.

  Start an interactive shell connected to a remote NeuG server. DB_URI should be
  in the format host:port or http://host:port.

  Examples:

    neug-cli connect localhost:8182

    neug-cli connect http://192.168.1.1:8182 --timeout 60

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


def test_start_remote_database_neug_ui(runner, tmp_path, run_web_ui):
    db_endpoint = "http://127.0.0.1:10010"
    db_path = tmp_path / "modern_graph"
    db = Database(db_path=str(db_path), mode="w")
    session = None
    try:
        db.load_builtin_dataset("modern_graph")
        db.serve(host="127.0.0.1", port=10010, blocking=False)
        wait_for_server_ready(db_endpoint)
        session = Session(endpoint=db_endpoint)
        shell = neug_cli.NeugShell(session)
        with run_web_ui(shell):
            url = "http://127.0.0.1:5000/cypherv2"
            headers = {"Content-Type": "text/plain"}
            data = "MATCH (n) RETURN count(n)"
            response = requests.post(url, headers=headers, data=data, timeout=10)
            expected_output = """COUNT(_0_n)": 6"""
            assert response.status_code == 200
            assert expected_output in response.text
    finally:
        if session is not None:
            session.close()
        db.close()


def test_start_local_database_neug_ui(runner, tmp_path, run_web_ui):
    db_path = tmp_path / "modern_graph"
    db = Database(db_path=str(db_path), mode="w")
    try:
        db.load_builtin_dataset("modern_graph")
        connection = db.connect()
        shell = neug_cli.NeugShell(connection)
        with run_web_ui(shell):
            url = "http://127.0.0.1:5000/cypherv2"
            headers = {"Content-Type": "text/plain"}
            data = "MATCH (n) RETURN count(n)"
            response = requests.post(url, headers=headers, data=data, timeout=10)
            expected_output = """COUNT(_0_n)": 6"""
            assert response.status_code == 200
            assert expected_output in response.text
    finally:
        db.close()
