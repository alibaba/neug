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

import logging
import os

import nexg_py_bind

from nexg.connection import Connection
from nexg.version import __version__

logger = logging.getLogger(__name__)


class Database(object):
    """
    Database class to manage the database connection and operations.
    """

    def __init__(self, db_path: str, mode: str = "r"):
        """
        Open a database connection.

        Parameters
        ----------
        db_path : str
            Path to the database file.
        mode : str
            Mode to open the database. Default is 'r' for read-only.
        """
        self._db_path = db_path
        self._mode = mode
        # The default connection of the database, will be lazy initialized if get_default_connection is called.
        # In 'r' mode, the default connection will be a read-only connection.
        # In 'w' mode, the default connection will be a read-write connection. And we won't allow to create any new connections.
        self._database = nexg_py_bind.PyDatabase(db_path, mode)
        logger.info(f"Open database {db_path} in {mode} mode.")

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    @property
    def version(self):
        """
        Get the version of the database.
        """
        return __version__

    def connect(self) -> Connection:
        """
        Connect to the database.
        """
        if not self._database:
            raise RuntimeError("Database is closed.")
        return Connection(self._database.connect())

    def close(self):
        """
        Close the database connection.
        """
        if self._database:
            self._database.close()
            self._database = None
            logger.info(f"Close database {self._db_path}.")
