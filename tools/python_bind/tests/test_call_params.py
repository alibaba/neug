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
# CALL TEST_ECHO_PARAM via the test_out_of_tree extension ($param + literal).

import os

import pytest

from neug import Database

EXTENSION_TESTS_ENABLED = os.environ.get("NEUG_RUN_EXTENSION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
extension_test = pytest.mark.skipif(
    not EXTENSION_TESTS_ENABLED,
    reason="Extension tests disabled by default; set NEUG_RUN_EXTENSION_TESTS=1 to enable.",
)


@extension_test
def test_call_echo_param(tmp_path):
    db = Database(db_path=str(tmp_path / "call_params"), mode="w")
    conn = db.connect()
    try:
        conn.execute("LOAD test_out_of_tree;")

        rows = list(
            conn.execute(
                "CALL TEST_ECHO_PARAM($param) RETURN value;",
                parameters={"param": "hello-from-params"},
            )
        )
        assert rows == [["hello-from-params"]]

        missing = list(
            conn.execute(
                "CALL TEST_ECHO_PARAM($param) RETURN value;",
                parameters={},
            )
        )
        assert missing == [[""]]

        literal = list(
            conn.execute("CALL TEST_ECHO_PARAM('literal-ok') RETURN value;")
        )
        assert literal == [["literal-ok"]]
    finally:
        conn.close()
        db.close()
