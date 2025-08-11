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

"""The Neug database module."""

import logging
import os

try:
    import neug_py_bind
except ImportError as e:
    if os.environ.get("BUILD_DOC", "OFF") == "OFF":
        # re-raise the import error if building documentation
        raise e

from neug.async_connection import AsyncConnection
from neug.connection import Connection
from neug.proto.error_pb2 import ERR_CONFIG_INVALID
from neug.proto.error_pb2 import ERR_INVALID_ARGUMENT
from neug.proto.error_pb2 import ERR_INVALID_PATH
from neug.version import __version__

logger = logging.getLogger(__name__)

cur_file_path = os.path.dirname(os.path.abspath(__file__))
cur_dir_path = os.path.dirname(cur_file_path)
resource_dir = os.path.join(cur_dir_path, "neug", "resources")


def readable(mode: str) -> str:
    """Convert mode to a readable string."""
    if mode in ["r", "read", "read-only", "read_only"]:
        return "read-only"
    elif mode in ["w", "rw", "write", "readwrite", "read-write", "read_write"]:
        return "read-write"
    else:
        raise ValueError(
            f"Invalid mode: {mode}. Must be one of 'r', 'read', 'w', 'rw', 'write', 'readwrite'."
        )


class Database(object):
    """The entrance of the Neug database.

    This class is used to open a database connection and manage the database. User should use this class to
    open a database connection, and then use the `connect` method to get a `Connection` object to interact with the database.

    By passing an empty string as the database path, the database will be opened in memory mode.

    The database could be opened with different modes(read-only or read-write) and different planners.

    When the database is opened in read-only mode, other databases could also open the same database directory in
    read-only mode, inside the same process or in different processes.
    When the database is opened in read-write mode, no other databases could open the same database directory in
    either read-only or read-write mode, inside the same process or in different processes.

    When the database is closed, all the connections to the database will be closed automatically.

    .. code:: python

        >>> from neug import Database
        >>> db = Database("/tmp/test.db", mode="w")
        >>> conn = db.connect()

        >>> # Use the connection to interact with the database
        >>> conn.execute('CREATE TABLE person(id INT64, name STRING);')
        >>> conn.execute('CREATE TABLE knows(FROM person TO person, weight DOUBLE);')

        >>> # Import data from csv file.
        >>> conn.execute('COPY person FROM "person.csv"')
        >>> conn.execute('COPY knows FROM "knows.csv" (from="person", to="person");')

        >>> res = conn.execute('MATCH(n) return n.id;)
        >>> for record in res:
        >>>     print(record)
    """

    def __init__(
        self,
        db_path: str = None,
        mode: str = "read-write",
        max_thread_num: int = 0,
        planner_config_path=None,
    ):
        """
        Open a database.

        Parameters
        ----------
        db_path : str
            Path to the database file. required. If it is set to empty string, the database will be opened in memory mode.
            Note that in memory mode, the database will not be persisted to disk, and all data will be
            lost when the program exits. In this case, the db_path should not contain any illegal characters.
        mode : str
            Mode to open the database, could be 'r', 'read', 'readwrite', 'w', 'rw', 'write'. Default is 'r' for read-only.
        max_thread_num : int
            Maximum number of threads to use. Default is 0, which means no limit.
        planner_config_path : str
            Path to the planner config file for the planner to use. Default is None.
            If none, the default config path will be used.

        Raises
        ------
        RuntimeError
            If the database file does not exist or the mode is invalid.
        ValueError
            If the mode is not one of 'r', 'read', 'w', 'rw', 'write'.
            If the planner is not 'gopt'.
        """
        self._database = None
        self._db_path = None
        self._connections = []
        self._async_connections = []
        self._illegal_chars = ["?", "*", '"', "<", ">", "|", ":", "\\"]
        if isinstance(db_path, str):
            if any(char in db_path for char in self._illegal_chars):
                raise ValueError(
                    f"invalid path: database path '{db_path}' contains illegal characters: {self._illegal_chars},"
                    f"error code: {ERR_INVALID_PATH}."
                )
        self._db_path = db_path if db_path is not None else ""
        self._mode = mode
        if self._mode not in [
            "r",
            "read",
            "w",
            "rw",
            "write",
            "readwrite",
            "read-write",
            "read_write",
            "read-only",
            "read_only",
        ]:
            raise ValueError(
                f"Invalid mode: {self._mode}. Must be one of 'r', 'read', 'w', 'rw', 'write', 'readwrite', 'read-write'."
            )
        # The default connection of the database, will be lazy initialized if get_default_connection is called.
        # In 'r' mode, the default connection will be a read-only connection.
        # In 'w' mode, the default connection will be a read-write connection.
        # And we won't allow to create any new connections.
        if planner_config_path is None:
            planner_config_path = self._get_default_planner_config_path()

        if max_thread_num < 0:
            raise ValueError(
                f"Invalid config: max_thread_num: {max_thread_num}. Must be a non-negative integer."
                f"Error code: {ERR_CONFIG_INVALID}."
            )

        if max_thread_num > os.cpu_count():
            raise ValueError(
                f"Invalid argument: max_thread_num: {max_thread_num}. "
                f"Must be less than or equal to the number of CPU cores: {os.cpu_count()}."
                f" Error code: {ERR_INVALID_ARGUMENT}."
            )

        if db_path is None and mode in ["r", "read", "read-only", "read_only"]:
            raise ValueError(
                f"Invalid mode: {mode}. In-memory database can not be opened in read-only mode."
            )

        # Currently, no intellisense here. self._database is of class PyDatabase,
        # defined in tools/python_bind/src/py_database.h
        self._database = neug_py_bind.PyDatabase(
            database_path=self._db_path,
            max_thread_num=max_thread_num,
            mode=readable(mode),
            planner="gopt",
            planner_config_path=planner_config_path,
        )
        self._serving = False
        if self._db_path is None or self._db_path.strip() == "":
            # In memory mode, the database will not be persisted to disk, and all data will be lost when the program exits.
            # So we don't need to log the db_path.
            logger.info(
                f"Open in-memory database in {readable(mode)} mode, config: {planner_config_path}"
            )
        else:
            logger.info(
                f"Open database {self._db_path} in {mode} mode, config: {planner_config_path}"
            )

    def __enter__(self):
        return self

    @property
    def version(self):
        """
        Get the version of the database.
        """
        return __version__

    @property
    def mode(self) -> str:
        """
        Get the mode of the database.

        Returns
        -------
        str
            The mode of the database, could be 'r', 'read', 'w', 'rw', 'write', 'readwrite'.
        """
        return self._mode

    def connect(self) -> Connection:
        """
        Connect to the database.

        Returns
        -------
        Connection
            A Connection object to interact with the database.
        Raises
        ------
        RuntimeError
            If the database is closed or not opened.
        """
        if not self._database:
            raise RuntimeError("Database is closed.")
        if self._serving:
            raise RuntimeError(
                "Cannot create connection while the database server is running."
            )
        conn = Connection(self._database.connect())
        self._connections.append(conn)
        return conn

    def serve(self, port: int = 10000, host: str = "localhost"):
        """
        Start the database server for handling remote connections(TP mode).
        This method is used to start the database server for handling remote connections.
        When db.serve() is called, the database will switch to the TP mode, and all the connections to the local database
        will be closed. After that, no new connections to the local database will be allowed.
        It will start a server that listens on a specific port, and clients can connect to the server to interact with the
        database. User could use Session to connect to the server. For detail usage, please refer to the
        documentation of Session.

        Parameters
        ----------
        port : int
            The port to listen on. Default is 10000.
        host : str
            The host to listen on. Default is 'localhost'.

        Returns
        -------
        uri : str
            The URI of the server, in the format of 'http://host:port'.

        Raises
        ------
        RuntimeError
            If there are open connections to the local database.
            If the database is already serving.

        Notes
        -----
        Make sure to close all connections before starting the server.
        After starting the server, no new connections to the local database will be allowed.
        """
        # Before starting the server, we should check all current connections are closed.
        # And also after starting the server, no new connections should be allowed to the local database.
        for conn in self._connections:
            if conn and conn.is_open:
                raise RuntimeError(
                    "Cannot start the server while there are open connections to the local database."
                )
        for async_conn in self._async_connections:
            if async_conn and async_conn.is_open:
                raise RuntimeError(
                    "Cannot start the server while there are open async connections to the local database."
                )
        # We should not clear the connections here, because the connection maybe held by the user.
        # Instead, we will close all connections when the server is stopped.
        if self._serving:
            logger.warning("Database server is already running.")
            return
        self._serving = True
        logger.info(f"Starting database server on {host}:{port}.")
        return self._database.serve(port, host)

    def stop_serving(self):
        """
        Stop the database server.
        This method is used to stop the database server that was started by the `serve` method.
        After calling this method, the database will switch back to the local mode, and new connections to the local
        database will be allowed again.

        Raises
        ------
        RuntimeError
            If the database is not serving.
        """
        if not self._serving:
            raise RuntimeError("Database server is not running.")
        logger.info("Stopping database server.")
        self._database.stop_serving()
        self._serving = False

    def async_connect(self) -> AsyncConnection:
        """
        Connect to the database asynchronously.

        Returns
        -------
        AsyncConnection
            An AsyncConnection object to interact with the database asynchronously.
        Raises
        ------
        RuntimeError
            If the database is closed or not opened.
        """
        if not self._database:
            raise RuntimeError("Database is closed.")
        if self._serving:
            raise RuntimeError(
                "Cannot create async connection while the database server is running."
            )
        async_conn = AsyncConnection(self._database.connect())
        self._async_connections.append(async_conn)
        return async_conn

    def close(self):
        """
        Close the database connection.
        """
        if self._db_path and self._db_path.strip() != "":
            logger.info(f"Closing database {self._db_path}.")
        # Close all connections
        if self._connections:
            for conn in self._connections:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"Failed to close connection: {e}")
        if self._async_connections:
            for async_conn in self._async_connections:
                try:
                    async_conn.close()
                except Exception as e:
                    logger.warning(f"Failed to close async connection: {e}")
        if self._database:
            self._database.close()
            self._database = None
        # Don't clear the connections list, because the connections may be held by the user.

    def _get_default_planner_config_path(self):
        """
        Get the default planner config path.
        """
        config_path = os.path.join(resource_dir, "planner_config.yaml")
        if not os.path.exists(config_path):
            raise RuntimeError(f"Planner config file not found: {config_path}")
        logger.info(f"Using planner config file: {config_path}")
        # convert to string
        return str(config_path)
