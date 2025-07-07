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

import neug_py_bind

from neug.async_connection import AsyncConnection
from neug.connection import Connection
from neug.version import __version__

logger = logging.getLogger(__name__)

cur_file_path = os.path.dirname(os.path.abspath(__file__))
cur_dir_path = os.path.dirname(cur_file_path)
resource_dir = os.path.join(cur_dir_path, "neug", "resources")


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
        db_path: str,
        mode: str = "r",
        max_thread_num: int = 0,
        planner="gopt",
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
        planner : str
            The planner to use, should be one of 'jni', 'gopt'. Default is 'gopt'.
        planner_config_path : str
            Only take effect when planner is 'jni'. Path to the planner config file. Default is None.
            If none, the default config path will be used.

        Raises
        ------
        RuntimeError
            If the database file does not exist or the mode is invalid.
        ValueError
            If the mode is not one of 'r', 'read', 'w', 'rw', 'write'.
            If the planner is not one of 'jni', 'gopt'.
        """
        self._database = None
        self._db_path = None
        self._illegal_chars = ["?", "*", '"', "<", ">", "|", ":", "\\"]
        if not isinstance(db_path, str):
            raise TypeError("db_path must be a string." + str(type(db_path)))
        if any(char in db_path for char in self._illegal_chars):
            raise ValueError(
                f"invalid path: database path '{db_path}' contains illegal characters: {self._illegal_chars}."
            )
        self._db_path = db_path
        self._mode = mode
        if self._mode not in ["r", "read", "w", "rw", "write", "readwrite"]:
            raise ValueError(
                f"Invalid mode: {self._mode}. Must be one of 'r', 'read', 'w', 'rw', 'write', 'readwrite'."
            )
        if planner not in [
            "jni",
            "gopt",
            "dummy",
        ]:  # TODO(zhanglei): Remove 'dummy' when we have a real planner.
            raise ValueError(
                f"Invalid planner: {planner}. Must be one of 'jni', 'gopt', 'dummy'."
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
            )

        # Currently, no intellisense here. self._database is of class PyDatabase,
        # defined in tools/python_bind/src/py_database.h
        self._database = neug_py_bind.PyDatabase(
            database_path=db_path,
            max_thread_num=max_thread_num,
            mode=mode,
            planner=planner,
            planner_config_path=planner_config_path,
        )
        if db_path.strip() == "":
            # In memory mode, the database will not be persisted to disk, and all data will be lost when the program exits.
            # So we don't need to log the db_path.
            logger.info(
                f"Open in-memory database in {mode} mode, planner: {planner},"
                f"config: {planner_config_path}"
            )
        else:
            logger.info(
                f"Open database {db_path} in {mode} mode, planner: {planner}, config: {planner_config_path}"
            )

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def serve(self, port: int = 10000, host: str = "localhost"):
        """
        Start the database server for handling remote connections(TP mode).

        Neug does not support the ideally htap (hybrid transactional and analytical processing) mode, could only switch between
        analytical and transactional mode. This method is used to start the database server for handling remote connections.
        When db.serve() is called, the database will switch to the TP mode, and all the connections to the local database
        will be closed.

        It will start a server that listens on a specific port, and clients can connect to the server to interact with the
        database. User could use RemoveDatabase to connect to the server. For detail usage, please refer to the
        documentation of RemoveDatabase.
        """
        pass

    @property
    def version(self):
        """
        Get the version of the database.
        """
        return __version__

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
        return Connection(self._database.connect())

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
        return AsyncConnection(self._database.connect())

    def close(self):
        """
        Close the database connection.
        """
        if self._db_path and self._db_path.strip() != "":
            logger.info(f"Closing database {self._db_path}.")
        if self._database:
            self._database.close()
            self._database = None

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
