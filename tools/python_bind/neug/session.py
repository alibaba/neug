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

import json
import logging

import requests
import requests.adapters

try:
    from neug_py_bind import PyQueryRequest
    from neug_py_bind import PyQueryResult
except ImportError as e:
    import os

    if os.environ.get("BUILD_DOC", "OFF") == "OFF":
        # re-raise the import error if building documentation
        raise e

from neug.proto.error_pb2 import ERR_NETWORK
from neug.proto.error_pb2 import ERR_QUERY_TIMEOUT
from neug.proto.error_pb2 import ERR_SESSION_CLOSED
from neug.query_result import QueryResult
from neug.utils import is_access_mode_valid
from neug.utils import valid_access_modes

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

    def __init__(
        self,
        endpoint: str = "http://localhost:10000",
        timeout: str = "10s",
        num_threads: int = 1,
    ):
        """
        Initialize a session with the given endpoint and timeout.

        :param endpoint: The endpoint URL for the session.
        :param timeout: The timeout duration for the session.
        """
        self._endpoint = endpoint
        self._query_endpoint = endpoint + "/cypher"
        self._status_endpoint = endpoint + "/service_status"
        self._schema_endpoint = endpoint + "/schema"
        self._transactions_endpoint = endpoint + "/transactions"
        self._timeout = timeout
        if isinstance(self._timeout, int):
            self._timeout = f"{self._timeout}s"
        self._http_session = requests.Session()
        self._http_adapter = requests.adapters.HTTPAdapter(
            pool_connections=num_threads,
            pool_maxsize=num_threads,
            max_retries=5,
            pool_block=False,
        )
        self._http_session.mount("http://", self._http_adapter)
        # check whether the endpoint is reachable
        try:
            self._http_session.get(self._status_endpoint, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Failed to connect to the endpoint {self._status_endpoint}: {e}"
            )
            raise ConnectionError(
                f"Could not connect to the endpoint: {self._status_endpoint}, Error code: {ERR_NETWORK}"
            ) from e
        logger.info(
            f"Session initialized with endpoint: {endpoint} and timeout: {self.timeout}"
        )
        self._closed = False
        self._transaction_id = None

    @staticmethod
    def open(
        endpoint: str = "http://localhost:10000",
        timeout: str = "10s",
        num_threads: int = 1,
    ):
        """
        Open a session with the given endpoint and timeout.
        :param endpoint: The endpoint URL for the session.
        :param timeout: The timeout duration for the session.
        :return: An instance of the Session class.
        """
        logger.info(
            f"Opening session at endpoint: {endpoint} with timeout: {timeout}, num_threads: {num_threads}"
        )
        return Session(endpoint, timeout, num_threads)

    def close(self):
        """
        Close the session. An active explicit transaction is rolled back on a
        best-effort basis.
        """
        if self._closed:
            logger.warning("Session is already closed.")
            return
        logger.info(f"Closing session at endpoint: {self._endpoint}")
        if self._transaction_id is not None:
            try:
                self.rollback()
            except (ConnectionError, RuntimeError) as e:
                logger.warning("Failed to roll back transaction while closing: %s", e)
            self._transaction_id = None
        self._closed = True
        self._http_session.close()
        self._http_adapter.close()
        self._http_session = None
        self._http_adapter = None

    @property
    def has_active_transaction(self) -> bool:
        """Whether this session has an active explicit transaction.

        The property remains true while a failed transaction is rollback-only.
        Call `rollback()` to discard that transaction before issuing another
        query or beginning a new transaction.
        """
        return not self._closed and self._transaction_id is not None

    def _require_open(self, operation: str):
        if self._closed:
            raise ConnectionError(
                f"Session is closed. Cannot {operation}, Error code: {ERR_SESSION_CLOSED}"
            )

    def _post_transaction_request(
        self, endpoint: str, payload: str, operation: str, expected_status: int = 200
    ):
        try:
            response = self._http_session.post(
                endpoint, data=payload, timeout=self.timeout
            )
        except requests.exceptions.RequestException as e:
            logger.error("Failed to %s: %s", operation, e)
            raise ConnectionError(
                f"Could not {operation}, Error code: {ERR_NETWORK}"
            ) from e
        if response.status_code != expected_status:
            self._clear_terminated_transaction(response)
            error_message = (
                f"Failed to {operation}. Http code: {response.status_code}, "
                f"Response: {response.text}"
            )
            logger.error(error_message)
            raise RuntimeError(error_message)
        return response

    def _clear_terminated_transaction(self, response):
        if response.status_code == requests.codes.gone:
            self._transaction_id = None

    def _transaction_id_from_location(self, response):
        location = response.headers.get("Location")
        location_prefix = "/transactions/"
        if not isinstance(location, str) or not location.startswith(location_prefix):
            return None
        transaction_id = location[len(location_prefix) :]
        if not transaction_id or any(char in transaction_id for char in "/?#"):
            return None
        return transaction_id

    def _rollback_transaction(self, transaction_id):
        if transaction_id is None:
            logger.warning(
                "Transaction begin response did not include a usable Location."
            )
            return
        try:
            rollback_response = self._http_session.post(
                f"{self._transactions_endpoint}/{transaction_id}/rollback",
                data="",
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as e:
            logger.warning("Failed to clean up transaction after begin: %s", e)
            return
        if rollback_response.status_code != 200:
            logger.warning(
                "Failed to clean up transaction after begin. Http code: %s, "
                "Response: %s",
                rollback_response.status_code,
                rollback_response.text,
            )

    def _require_active_transaction(self, operation: str):
        self._require_open(operation)
        if self._transaction_id is None:
            raise RuntimeError(f"No active explicit transaction to {operation}.")

    def _active_transaction_endpoint(self, operation: str) -> str:
        return f"{self._transactions_endpoint}/{self._transaction_id}/{operation}"

    def begin_transaction(self, read_only: bool = False):
        """Begin an explicit transaction.

        Parameters
        ----------
        read_only : bool
            Pin one read view and reject writes when true. The default starts
            a read-write transaction with a private COW view.

        Raises
        ------
        ConnectionError
            If the session is closed or the service cannot be reached.
        RuntimeError
            If the session already has an active transaction or the service
            rejects the begin request.
        """
        self._require_open("begin a transaction")
        if self._transaction_id is not None:
            raise RuntimeError("An explicit transaction is already active.")
        mode = "read_only" if read_only else "read_write"
        response = self._post_transaction_request(
            self._transactions_endpoint,
            json.dumps({"mode": mode}),
            "begin transaction",
            expected_status=201,
        )
        location_transaction_id = self._transaction_id_from_location(response)
        try:
            response_body = response.json()
        except ValueError as e:
            self._rollback_transaction(location_transaction_id)
            raise RuntimeError(
                "Transaction begin response did not contain valid JSON."
            ) from e
        transaction_id = (
            response_body.get("transaction_id")
            if isinstance(response_body, dict)
            else None
        )
        if not isinstance(transaction_id, str) or not transaction_id:
            self._rollback_transaction(location_transaction_id)
            raise RuntimeError("Transaction begin response did not include an ID.")
        if (
            location_transaction_id is not None
            and location_transaction_id != transaction_id
        ):
            self._rollback_transaction(location_transaction_id)
            raise RuntimeError(
                "Transaction begin response ID did not match its Location."
            )
        self._transaction_id = transaction_id

    def commit(self):
        """Commit the active explicit transaction.

        A rollback-only transaction must be rolled back instead.
        """
        self._require_active_transaction("commit")
        self._post_transaction_request(
            self._active_transaction_endpoint("commit"),
            "",
            "commit transaction",
        )
        self._transaction_id = None

    def rollback(self):
        """Roll back the active explicit transaction and return to auto-commit."""
        self._require_active_transaction("roll back")
        self._post_transaction_request(
            self._active_transaction_endpoint("rollback"),
            "",
            "roll back transaction",
        )
        self._transaction_id = None

    def execute(
        self, query: str, access_mode: str = "", parameters: dict = None
    ) -> QueryResult:
        """
        Execute a query on the NeuG server.

        :param query: The query string to be executed.
        :param access_mode: The access mode for the query. Supported modes are:
            - `read` or `r`: Read-only queries
            - `insert` or `i`: Insert-only operations
            - `update` or `u`: Update/delete operations (default)
            - `schema` or `s`: Schema modification operations
        :param parameters: Optional dict of query parameters.
        :return: The result of the query execution.

        While an explicit transaction is active, the query runs in that
        transaction. A failure reported by the service leaves it rollback-only;
        call `rollback()` before issuing another query. Client-side validation
        errors, such as an invalid `access_mode`, do not change the transaction
        state.
        """
        if self._closed:
            logger.error("Session is closed. Cannot execute query.")
            raise ConnectionError(
                f"Session is closed. Cannot execute query, Error code: {ERR_SESSION_CLOSED}"
            )
        query_endpoint = (
            self._active_transaction_endpoint("query")
            if self._transaction_id is not None
            else self._query_endpoint
        )
        logger.info(
            f"Executing query: {query} on endpoint: {query_endpoint} with timeout: {self.timeout}"
        )
        access_mode = access_mode.lower()
        if access_mode != "" and not is_access_mode_valid(access_mode):
            raise ValueError(
                f"Invalid access_mode: {access_mode}. Supported access modes are "
                f"{valid_access_modes}."
            )
        try:
            if parameters is not None:
                payload = PyQueryRequest.serialize_request(
                    query, access_mode, parameters
                )
            else:
                payload = PyQueryRequest.serialize_request(query, access_mode)
            logger.info(f"Payload for query: {query} is {payload}")
            response = self._http_session.post(
                query_endpoint, data=payload, timeout=self.timeout
            )
        except requests.exceptions.Timeout as e:
            error_message = (
                f"Query timed out after {self.timeout} seconds "
                f"(session timeout: {self._timeout}) while requesting "
                f"{self._query_endpoint}. Consider increasing the Session "
                "timeout or optimizing the query. "
                'For example: Session(endpoint, timeout="30s"). '
                f"Error code: {ERR_QUERY_TIMEOUT}"
            )
            logger.error(f"Failed to execute query: {query}. {error_message}")
            raise TimeoutError(error_message) from e
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to execute query: {query}. Error: {e}")
            raise ConnectionError(
                f"Could not execute query: {query}, Error code: {ERR_NETWORK}"
            ) from e
        if response.status_code != 200:
            self._clear_terminated_transaction(response)
            error_message = f"Failed to execute query: {query}. Http code: {response.status_code}, Response: {response.text}"
            logger.error(error_message)
            raise Exception(error_message)

        return QueryResult(PyQueryResult(response._content))

    def service_status(self):
        """
        Get the service status of the NeuG server.

        :return: The status of the NeuG server.
        """
        logger.info(f"Fetching service status from endpoint: {self._status_endpoint}")
        try:
            response = self._http_session.get(
                self._status_endpoint, timeout=self.timeout
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch service status: {e}")
            raise ConnectionError("Could not fetch service status") from e

        # Json string
        return response.json()

    def get_schema(self):
        """
        Get the schema of the NeuG database.

        :return: The schema of the NeuG database.
        """
        self._require_open("fetch schema")
        logger.info(f"Fetching schema from endpoint: {self._schema_endpoint}")
        try:
            response = self._http_session.get(
                self._schema_endpoint, timeout=self.timeout
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch schema: {e}")
            raise ConnectionError("Could not fetch schema") from e
        # Json string
        return response.json()

    @property
    def timeout(self):
        """
        Get the timeout duration for the session, in seconds.
        """
        if isinstance(self._timeout, str):
            if self._timeout.endswith("ms"):
                return int(self._timeout[:-2]) / 1000
            elif self._timeout.endswith("s"):
                return int(self._timeout[:-1])
            else:
                raise ValueError("Timeout must be a string ending with 's' or 'ms'.")
        elif isinstance(self._timeout, int):
            return self._timeout
        else:
            raise TypeError("Timeout must be a string or an integer.")
