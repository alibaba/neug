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

import atexit
import cmd
import logging
import os
import readline

import click
from neug.connection import Connection
from neug.database import Database

from ngcli.format import parse_and_format_results

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Build-in commands
COMMAND_HELP = ":help"
COMMAND_QUIT = ":quit"
COMMAND_MAX_ROWS = ":max_rows"

# Default prompt
PROMPT = "neug > "
ALTPROMPT = "... "


class NeugShell(cmd.Cmd):
    intro = "Welcome to the Neug shell. Type :help for usage hints.\n"
    prompt = PROMPT

    def __init__(self, connection: Connection):
        super().__init__()
        self.connection = connection
        self.buffer = []
        self.multi_line_mode = False
        self.max_rows = 20  # Default max rows for query results

        # Set and read history file
        histfile = os.path.join(os.path.expanduser("~"), ".neug_history")
        try:
            readline.read_history_file(histfile)
        except FileNotFoundError:
            pass
        # Register history save at exit
        atexit.register(readline.write_history_file, histfile)

        logger.info("Connection established.")

    def do_quit(self, arg):
        """Exit the shell: quit"""
        print("Exiting...")
        self.connection.close()
        return True

    def default(self, line):
        """Handles any input not matched by a command method."""
        stripped_line = line.strip()
        if stripped_line.startswith(COMMAND_HELP):
            # Handle help command
            self.do_help(stripped_line)
        elif stripped_line.startswith(COMMAND_QUIT):
            # Handle quit command
            return self.do_quit(stripped_line)
        elif stripped_line.startswith(COMMAND_MAX_ROWS):
            # Handle max_rows command
            arg = stripped_line[len(COMMAND_MAX_ROWS) :].strip()
            self.do_max_rows(arg)
        elif stripped_line:
            self.buffer.append(stripped_line)
            self.multi_line_mode = not stripped_line.endswith(";")
            # Support multi-line commands
            if not self.multi_line_mode:
                full_query = " ".join(self.buffer)
                self.buffer = []
                self.prompt = PROMPT
                self.do_query(full_query)
                # Add complete query to history after execution
                readline.add_history(full_query)
            else:
                self.prompt = ALTPROMPT
        else:
            print("Invalid command. Type :help for usage hints.")

    def do_query(self, arg):
        """Execute a Cypher query"""
        try:
            result = self.connection.execute(arg)
            if result:
                parse_and_format_results(result, max_rows=self.max_rows)
        except Exception as e:
            print(e)

    def do_max_rows(self, arg):
        """Set the maximum number of rows to display for query results. Usage: :max_rows 10"""
        try:
            value = int(arg.strip())
            if value <= 0:
                print("max_rows must be a positive integer.")
                return
            self.max_rows = value
            print(f"Set max_rows to {self.max_rows}")
        except Exception:
            print("Usage: :max_rows <number>")

    def do_help(self, arg):
        """Provide usage hints."""
        print(
            """
            Usage hints:
            - Enter Cypher queries directly to execute them on the connected database.
            - Use :help to display this help message.
            - Use :quit to leave the shell.
            - Use :max_rows <number> to set the maximum number of rows to display for query results.
            - Multi-line commands are supported. Use ';' at the end to execute.
            - Command history is supported; use the up/down arrow keys to navigate previous commands.
            """
        )


@click.group(name="ngcli")
@click.version_option(version="0.1.0")
def cli():
    """Neug CLI Tool."""


@cli.command()
@click.argument("path", required=True)
@click.option("-r", "--readonly", is_flag=True, help="Open database in read-only mode.")
def open(path, readonly):
    """Open a local database."""
    mode = "r" if readonly else "rw"
    click.echo(f"Opened database at {path} in {mode} mode")
    database = Database(db_path=str(path), mode=mode, planner="gopt")
    connection = database.connect()
    shell = NeugShell(connection)
    shell.cmdloop()


@cli.command()
@click.argument("uri", required=True)
@click.option("-u", "--user", default=None, help="Username for authentication.")
@click.option("-p", "--password", default=None, help="Password for authentication.")
@click.option(
    "--timeout", default=300, show_default=True, help="Connection timeout in seconds."
)
def connect(uri, user, password, timeout):
    """Connect to a remote database."""
    host, port = uri.split(":")
    click.echo(f"Connecting to {host}:{port}")

    auth = (user, password) if user and password else None
    # TODO: connect to neug in TP mode
    from neug.session import Session

    session = Session.open(f"neug://{auth}@{host}:{port}/", timeout=timeout)
    shell = NeugShell(session)
    shell.cmdloop()


if __name__ == "__main__":
    cli()
