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


class QueryResult(object):
    """
    QueryResult represents the result of a cypher query. Could be visited as a iterator.
    """

    def __init__(self, result):
        """
        Initialize the QueryResult.

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
        return iter(self._result)

    def __len__(self):
        """
        Get the length of the result.
        """
        return len(self._result)

    def __getitem__(self, index):
        """
        Get the item at the specified index.

        Parameters
        ----------
        index : int
            The index of the item to get.

        Returns
        -------
        object
            The item at the specified index.
        """
        return self._result[index]

    def __str__(self):
        """
        Get the string representation of the result.

        Returns
        -------
        str
            The string representation of the result.
        """
        return f"QueryResult({self._result.get_status_code()}, {self._result.get_status_message()})"

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
