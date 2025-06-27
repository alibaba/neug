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

"""The Neug connection module."""

from neug_py_bind import PyConnection

# This is the C++ binding for the Python interface, which provides the actual connection to the database.
from neug.query_result import QueryResult


class Connection(object):
    """
    Connection represents a logical connection to a database. User should use this class to interact
    with the database, such as executing queries and managing transactions.
    The connection is created by the `Database.connect` method, and should be closed by calling the `close` method
    when it is no longer needed. If the database is closed, all the connections to the database will be closed automatically.
    """

    def __init__(self, py_connection: PyConnection):
        """
        Initialize a Connection object.
        Parameters
        ----------
        py_connection : PyConnection
            The underlying c++ connection object that provides the actual database connection.
        """
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
        Execute a cypher query on the database. User could specify multiple queries in a single string,
        separated by semicolons. The query will be executed in the order they are specified.
        If any query fails, the whole execution will be rolled back.
        If the query is a DDL query, such as `CREATE TABLE`, `DROP TABLE`, etc., the database will be
        modified accordingly.

        For the details of the query syntax, please refer to the documentation of cypher manual.
        The result of the query will be returned as a `QueryResult` object, which contains the result of
        the query and the metadata of the query.
        The QueryResult object is like an iterator, providing methods to iterate over the results,
        such as `__iter__` and `__next__`.

        If the query is a DDL or DML query, the result will be an empty `QueryResult` object.

        Some of the cypher queries could change the state of the database, such as `CREATE TABLE`, `INSERT`,
        `UPDATE`, `DELETE`, etc. Other queries, such as `MATCH(n) RETURN n.id`, will not change the state of
        the database, but will return the results of the query.

        If the database is opened in read-only mode, any DDL or DML query will raise an exception.
        If the database is opened in read-write mode, all queries could be executed, and the state of the
        database will be changed accordingly.

        .. code:: python

            >>> from neug import Database
            >>> db = Database("/tmp/test.db", mode="w")
            >>> conn = db.connect()
            >>> res = conn.execute('CREATE TABLE person(id INT64, name STRING);')
            >>> res = conn.execute('CREATE TABLE knows(FROM person TO person, weight DOUBLE);')
            >>> res = conn.execute('COPY person FROM "person.csv"')
            >>> res = conn.execute('COPY knows FROM "knows.csv" (from="person", to="person");')
            >>> res = conn.execute('MATCH(n) RETURN n.id')
            >>> for record in res:
            >>>    print(record)
            >>> res = conn.execute('MATCH(p:person)-[knows]->(q:person) RETURN p.id, q.id LIMIT 10;')


        Parameters
        ----------
        query : str
            The query to execute.

        Returns
        -------
        query_result : QueryResult
            The result of the query.
        """
        return QueryResult(self._py_connection.execute(query))
