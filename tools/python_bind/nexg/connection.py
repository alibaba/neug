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

from nexg.result import QueryResult


class Connection(object):
    """
    Connection represents a logical connection to a database.
    """

    def __init__(self, py_connection):
        self._py_connection = py_connection
        pass

    def __del__(self):
        self.close()

    def close(self):
        """
        Close the connection.
        """
        self._py_connection.close()

    def execute(self, query: str) -> QueryResult:
        """
        Execute a query on the database.

        Parameters
        ----------
        query : str
            The query to execute.

        Returns
        -------
        query_result : QueryResult
            The result of the query.
        """
        return self._py_connection.execute(query)
