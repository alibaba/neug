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

import subprocess
import sys
import textwrap
from contextlib import closing

import pytest

from neug.database import Database


@pytest.mark.parametrize(
    "mode", ["STANDARD", "standard", "sTaNdArD", "FULL", "full", "fUlL"]
)
@pytest.mark.parametrize("checkpoint", [False, True])
def test_wal_sync_crash_recovery(tmp_path, mode, checkpoint):
    path = str(tmp_path / "db")
    # Terminate without destructors or close-time checkpoints. Writes after an
    # explicit checkpoint also exercise the replacement WAL writer timeline.
    script = textwrap.dedent(
        f"""
        import os
        from neug.database import Database
        db = Database(
            {path!r}, wal_sync_mode={mode!r}, checkpoint_on_close=False
        )
        conn = db.connect()
        conn.execute("CREATE NODE TABLE item(id INT64, PRIMARY KEY(id))")
        conn.execute("CREATE (:item {{id: 1}})")
        if {checkpoint!r}:
            conn.execute("CHECKPOINT")
        conn.execute("CREATE (:item {{id: 2}})")
        os._exit(0)
    """
    )
    subprocess.run([sys.executable, "-c", script], check=True, timeout=120)
    # The choice is per open and does not alter the persisted WAL format.
    other_mode = "FULL" if mode.upper() == "STANDARD" else "STANDARD"
    with closing(Database(path, wal_sync_mode=other_mode)) as db:
        with closing(db.connect()) as conn:
            rows = sorted(conn.execute("MATCH (n:item) RETURN n.id"))
            assert rows == [[1], [2]]


@pytest.mark.parametrize("mode", ["", "NONE", "INVALID", " FULL", "full ", "ſtandard"])
def test_invalid_wal_sync_mode(tmp_path, mode):
    path = tmp_path / "invalid"
    with pytest.raises(ValueError, match="STANDARD or FULL"):
        Database(str(path), wal_sync_mode=mode)
    assert not path.exists()


@pytest.mark.parametrize("mode", [None, 1, True])
def test_wal_sync_mode_type(tmp_path, mode):
    with pytest.raises(TypeError, match="must be a string"):
        Database(str(tmp_path / "invalid"), wal_sync_mode=mode)


@pytest.mark.parametrize("mode", ["standard", "sTaNdArD", "full", "fUlL"])
def test_native_wal_sync_case_insensitive(tmp_path, mode):
    # Bypass Python normalization to cover the parser shared by C++ and the CLI.
    from neug.database import neug_py_bind

    with closing(
        neug_py_bind.PyDatabase(
            database_path=str(tmp_path / "native"), mode="w", wal_sync_mode=mode
        )
    ):
        pass
