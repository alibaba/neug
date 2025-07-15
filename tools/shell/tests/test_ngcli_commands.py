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
from click.testing import CliRunner

from ngcli import neug_cli


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


def test_open_help_option(monkeypatch, runner):
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result = runner.invoke(neug_cli.cli, ["open", "--help"])
    assert result.exit_code == 0
    expected_output = """\
Usage: neug-cli open [OPTIONS] PATH

  Open a local database.

Options:
  -r, --readonly  Open database in read-only mode.
  --help          Show this message and exit.
"""
    assert expected_output.strip() == result.output.strip()


def test_open_local_database(monkeypatch, runner, tmp_path):
    db_path = tmp_path / "test_open_local_db"
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result = runner.invoke(neug_cli.cli, ["open", str(db_path)])
    assert result.exit_code == 0
    assert f"Opened database at {db_path} in rw mode" in result.output


def test_open_local_database_readonly(monkeypatch, runner, tmp_path):
    db_path = tmp_path / "test_open_local_db_readonly"
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result1 = runner.invoke(neug_cli.cli, ["open", str(db_path), "--readonly"])
    assert result1.exit_code == 0
    assert f"Opened database at {db_path} in r mode" in result1.output
    result2 = runner.invoke(neug_cli.cli, ["open", str(db_path), "-r"])
    assert result2.exit_code == 0
    assert f"Opened database at {db_path} in r mode" in result2.output


def test_open_local_database_fails_without_path(runner):
    result = runner.invoke(neug_cli.cli, ["open"])
    assert result.exit_code != 0
    assert "Error: Missing argument 'PATH'." in result.output


def test_connect_help_option(monkeypatch, runner):
    # mock cmdloop to avoid entering the shell loop
    monkeypatch.setattr(neug_cli.NeugShell, "cmdloop", lambda self: None)
    result = runner.invoke(neug_cli.cli, ["connect", "--help"])
    assert result.exit_code == 0
    excepted_output = """\
Usage: neug-cli connect [OPTIONS] URI

  Connect to a remote database.

Options:
  -u, --user TEXT      Username for authentication.
  -p, --password TEXT  Password for authentication.
  --timeout INTEGER    Connection timeout in seconds.  [default: 300]
  --help               Show this message and exit.
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
    assert "Error: Missing argument 'URI'." in result.output
