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

import os
import shutil
import sys
import time

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))
from neug.database import Database


def execute_1(endpoint, id):
    from neug.session import Session

    session = Session.open(endpoint)

    if id == 1:
        session.execute("CREATE NODE TABLE T(id INT32, PRIMARY KEY(id));")
        session.execute("CREATE (n:T {id: 1});")
    else:
        time.sleep(1)
        session.execute("CREATE (n:T {id: 2});")

    session.close()


def test_1(tmp_path):
    db_dir = tmp_path / "test_db"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=str(db_dir), mode="w")
    endpoint = db.serve(port=10010, host="localhost", blocking=False)

    import multiprocessing as mp

    from neug.session import Session

    proc1 = mp.Process(
        target=execute_1,
        args=(
            endpoint,
            1,
        ),
    )
    proc2 = mp.Process(
        target=execute_1,
        args=(
            endpoint,
            2,
        ),
    )
    proc1.start()
    proc2.start()
    proc1.join()
    proc2.join()

    session = Session.open(endpoint)
    result = session.execute("MATCH (n) RETURN count(n);")
    assert result.__next__()[0] == 2
    session.close()

    db.close()


def execute_2(endpoint, id):
    from neug.session import Session

    session = Session.open(endpoint)

    for i in range(100):
        session.execute("MATCH (n: T {id: 1}) SET n.value = n.value + CAST(1, 'INT32')")

    session.close()


def test_2(tmp_path):
    db_dir = tmp_path / "test_db"
    shutil.rmtree(db_dir, ignore_errors=True)
    db = Database(db_path=str(db_dir), mode="w")
    endpoint = db.serve(port=10010, host="localhost", blocking=False)

    import multiprocessing as mp

    from neug.session import Session

    session = Session.open(endpoint)
    session.execute("CREATE NODE TABLE T(id INT32, value INT32, PRIMARY KEY(id));")
    session.execute("CREATE (n:T {id: 1, value: 0});")

    proc1 = mp.Process(
        target=execute_2,
        args=(
            endpoint,
            1,
        ),
    )
    proc2 = mp.Process(
        target=execute_2,
        args=(
            endpoint,
            2,
        ),
    )
    proc1.start()
    proc2.start()
    proc1.join()
    proc2.join()

    session = Session.open(endpoint)
    result = session.execute("MATCH (n: T {id: 1}) RETURN n.value;")
    assert result.__next__()[0] == 200
    session.close()

    db.close()
