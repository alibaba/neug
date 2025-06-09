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

cur_file_path = os.path.dirname(os.path.abspath(__file__))
cur_dir_path = os.path.dirname(cur_file_path)
resource_dir = os.path.join(cur_dir_path,"nexg", "resources")


class Database(object):
    """
    Database class to manage the database connection and operations.
    """

    def __init__(self, db_path: str, mode: str = "r",  max_thread_num : int = 0,  planner = "jni", jni_planner_jar_path = None, planner_config_path = None):
        """
        Open a database connection.

        Parameters
        ----------
        db_path : str
            Path to the database file.
        mode : str
            Mode to open the database. Default is 'r' for read-only.
        max_thread_num : int
            Maximum number of threads to use. Default is 0, which means no limit.
        planner : str
            The planner to use. Default is 'jni'.
        jni_planner_jar_path : str
            Path to the JNI planner jar file. Default is None. If none, the default jar path will be used.
        planner_config_path : str
            Path to the planner config file. Default is None. If none, the default config path will be used.
        """
        self._db_path = db_path
        self._mode = mode
        # The default connection of the database, will be lazy initialized if get_default_connection is called.
        # In 'r' mode, the default connection will be a read-only connection.
        # In 'w' mode, the default connection will be a read-write connection. And we won't allow to create any new connections.
        if planner == "jni" and jni_planner_jar_path is None:
            jni_planner_jar_path = self._get_default_jni_planner_jar_path()
        if planner_config_path is None:
            planner_config_path = self._get_default_planner_config_path()
        # Get the absolute path to files("nexg.resources")
        # if python 3.9 or later, use importlib.resources.files
        # if before python 3.9, use importlib_resources
        try:
            from importlib.resources import files
            resource_path = files("nexg").joinpath("resources")
        except ImportError:
            import importlib_resources
            #TODO
            resource_path = importlib_resources.files("nexg").joinpath("resources")
        
        if not resource_path.exists():
            raise RuntimeError(f"Resource path not found: {resource_path}")
        # Convert to string
        resource_path = str(resource_path.resolve())
        # TODO: refactor it into a pydict.
        # Currently, no intellisense here. self._database is of class PyDatabase, defined in tools/python_bind/src/py_database.h
        self._database = nexg_py_bind.PyDatabase(db_path, max_thread_num, mode, planner, jni_planner_jar_path, planner_config_path, resource_path)
        logger.info(f"Open database {db_path} in {mode} mode, planner: {planner}, config: {planner_config_path}, jar: {jni_planner_jar_path}, resource_path: {resource_path}.")

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
        logger.info(f"Closing database {self._db_path}.")
        if self._database:
            self._database.close()
            self._database = None
            logger.info(f"Close database {self._db_path}.")
            
    
    def _get_default_jni_planner_jar_path(self):
        """
        Get the default JNI planner jar path.
        """
        # Load the file under nexg/resources/, ended with .jar
        jar_path = os.path.join(resource_dir, "compiler.jar")
        print(f"jar_path: {jar_path}")
        if not os.path.exists(jar_path):
            raise RuntimeError(f"JNI planner jar file not found: {jar_path}")
        logger.info(f"Using JNI planner jar file: {jar_path}")
        return jar_path
    
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
