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
import sys
import time
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database

logger = logging.getLogger(__name__)


def test_run_query():
    logger.info("Test query")
    modern_graph_db_dir = os.environ.get("NEUG_DB_DIR")
    if not modern_graph_db_dir:
        raise Exception("NEUG_DB_DIR is not set")
    db = Database(modern_graph_db_dir, "r")
    conn = db.connect()
    query = os.environ.get("NEUG_QUERY")
    if not query:
        query = "MATCH (n) return count(n)"
    logger.info(f"Running query: {query}")
    res = conn.execute(query)
    logger.info(res)
    cnt = 0
    for record in res:
        logger.info(f"line {cnt}")
        logger.info(record)
        cnt += 1
