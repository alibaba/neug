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

from nexg_py_bind import PyQueryResult
# PyQueryResult is defined in the nexg_py_bind module, which is a C++ binding for the Python interface.
# See py_query_result.h for the definition of PyQueryResult.


class QueryResult(object):
    """
    QueryResult represents the result of a cypher query. Could be visited as a iterator.

    Attributes
    ----------
    _result : PyQueryResult
        The underlying result object that contains the query results, a c++ object that implements the necessary methods to access the results.
        It should have methods like `hasNext()`, `getNext()`, and `length()` to iterate over the results.
        - `hasNext()`: Returns True if there are more results to iterate over.
        - `getNext()`: Returns the next result as a list.
        - `length()`: Returns the total number of results.
    """

    def __init__(self, result: PyQueryResult):
        """
        Initialize the QueryResult.
        TODO(zhanglei): add type hint for result.

        Parameters
        ----------
        result : object
            The result of the query.
        """
        self._result = result

    def __iter__(self):
        """
        Iterate over the result.
        """
        return self
    
    def __next__(self):
        if self._result.hasNext():
            return self._result.getNext() # Returns a list
        else:
            raise StopIteration

    def __len__(self):
        """
        Get the length of the result.
        """
        return self._result.length()

    def __str__(self):
        """
        Get the string representation of the result.

        Returns
        -------
        str
            The string representation of the result.
        """
        return f"QueryResult(size {self._result.length()})"

    def __repr__(self):
        """
        Get the string representation of the result.

        Returns
        -------
        str
            The string representation of the result.
        """
        return self.__str__()

    def __del__(self):
        """
        Delete the QueryResult.
        """
        del self._result
        self._result = None
