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


class TestDDL(unittest.TestCase):
    """
    Test running query on a graph that is already created and loaded
    """

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_batch_loading_modern_graph(self):
        # create a tmp directory for the graph
        db_dir = "/tmp/test_batch_loading"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        db = Database(db_dir, "w")
        conn = db.connect()
        # First create the graph schema
        conn.execute(
            "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")

        # Test Adding Column
        conn.execute("ALTER TABLE person ADD birthday DATE;")

        # Test adding column if not exists
        conn.execute("ALTER TABLE person ADD IF NOT EXISTS birthday DATE;")

        # Add name STRING, expect exception since name already exists
        with self.assertRaises(Exception):
            conn.execute("ALTER TABLE person ADD name STRING;")

        conn.execute("ALTER TABLE knows ADD registion DATE;")

        # Drop an non-existing column
        with self.assertRaises(Exception):
            conn.execute("ALTER TABLE person DROP non_existing_column;")

        # Drop a column if not exists
        conn.execute("ALTER TABLE person DROP birthday;")

        # Drop an existing column
        conn.execute("ALTER TABLE person DROP IF EXISTS birthday;")

        # Rename a column
        conn.execute("ALTER TABLE person RENAME name TO username;")

        # Batch delete vertices in table
        # Neng: This is a temporary query intended to test the batch deletion functionality.
        conn.execute("MATCH (v:person) DELETE v;")

        # Delete a edge type
        conn.execute("DROP TABLE knows;")

        # Delete a vertex type
        conn.execute("DROP TABLE person;")
