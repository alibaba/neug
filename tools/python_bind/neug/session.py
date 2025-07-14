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

import requests
from neug_py_bind import PyQueryResult

from neug.proto.results_pb2 import CollectiveResults
from neug.query_result import QueryResult

logger = logging.getLogger(__name__)


class Session:
    """
    Session is a class that connects to the NeuG server. User could use it just like a normal NeuG Connection,
    while it is actually a session that connects to the NeuG server.

    A NeuG Server could be started with `Database::serve()` method, and it will listen to the specified endpoint.

    .. code:: python

        >>> from neug import Database
        >>> db = Database("/tmp/test.db", mode="w")
        >>> db.serve(port = 10000, host = "localhost")

    And on another python shell, user could connect to the NeuG server with the following code:

    .. code:: python

        >>> from neug import Session
        >>> sess = Session('http://localhost:10000', timeout='10s')
        >>> sess.execute('MATCH(n) return count(n)')

    The query will be sent to the NeuG http server, and the result will be returned as a response.
    The session will automatically handle the connection and disconnection to the server.

    To stop the NeuG server, user could send terminal signal to the process.
    To close the session, user could call the `close()` method.
    """

    def __init__(self, endpoint: str = "http://localhost:10000", timeout: str = "10s"):
        """
        Initialize a session with the given endpoint and timeout.

        :param endpoint: The endpoint URL for the session.
        :param timeout: The timeout duration for the session.
        """
        self.endpoint = endpoint + "/cypher"
        self._timeout = timeout

    def execute(self, query: str):
        """
        Execute a query on the NeuG server.

        :param query: The query string to be executed.
        :return: The result of the query execution.
        """
        logger.info(
            f"Executing query: {query} on endpoint: {self.endpoint} with timeout: {self.timeout}"
        )
        response = requests.post(self.endpoint, data=query, timeout=self.timeout)
        response.raise_for_status()
        if response.status_code != 200:
            raise Exception(
                f"Failed to execute query: {query}. Status code: {response.status_code}, Response: {response.text}"
            )

        return QueryResult(PyQueryResult(response._content))

    @property
    def timeout(self):
        """
        Get the timeout duration for the session, in seconds.
        """
        if isinstance(self._timeout, str):
            if self._timeout.endswith("s"):
                return int(self._timeout[:-1])
            elif self._timeout.endswith("ms"):
                return int(self._timeout[:-2]) / 1000
            else:
                raise ValueError("Timeout must be a string ending with 's' or 'ms'.")
        elif isinstance(self._timeout, int):
            return self._timeout
        else:
            raise TypeError("Timeout must be a string or an integer.")
