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


class TestBachLoading(unittest.TestCase):
    """
    Test running alter property query on a graph that is already created and loaded
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

    def test_alter_properties(self):
        # create a tmp directory for the graph
        db_dir = "/tmp/test_batch_loading"
        if os.path.exists(db_dir):
            os.system("rm -rf %s" % db_dir)
        os.makedirs(db_dir)

        # get env : FLEX_DATA_DIR
        flex_data_dir = os.environ.get("FLEX_DATA_DIR")
        if not flex_data_dir:
            raise Exception("FLEX_DATA_DIR is not set")
        person_csv = os.path.join(flex_data_dir, "person.csv")
        person_knows_person_csv = os.path.join(flex_data_dir, "person_knows_person.csv")

        db = Database(db_dir, "w")
        conn = db.connect()
        # First create the graph schema
        conn.execute(
            "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));"
        )
        conn.execute("CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);")

        # Then load data.
        conn.execute(f'COPY person from "{person_csv}"')
        conn.execute(
            f'COPY knows from "{person_knows_person_csv}" (from="person", to="person")'
        )

        # Test add edge property
        res = conn.execute("ALTER TABLE knows ADD since INT32")

        res = conn.execute("MATCH (n:person)-[e:knows]->(m:person) return e;")
        for record in res:
            print(record)

        # Test delete edge property
        res = conn.execute("ALTER TABLE knows DROP since")
        res = conn.execute("MATCH (:person)-[e:knows]->(:person) return e;")
        for record in res:
            print(record)

        # Test delete all edge property
        res = conn.execute("ALTER TABLE knows DROP weight")
        res = conn.execute("MATCH (:person)-[e:knows]->(:person) return e;")
        for record in res:
            print(record)

        # Add new table
        conn.execute("CREATE REL TABLE follows(FROM person TO person, weight DOUBLE);")
        conn.execute(
            f'COPY follows from "{person_knows_person_csv}" (from="person", to="person")'
        )

        # Test delete the only one property
        res = conn.execute("ALTER TABLE follows DROP weight")
        res = conn.execute("MATCH (:person)-[e:follows]->(:person) return e;")
        for record in res:
            print(record)

        res = conn.execute("ALTER TABLE follows ADD since INT32")
        res = conn.execute("MATCH (:person)-[e:follows]->(:person) return e;")
        for record in res:
            print(record)
