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

"""A malformed array expression must never terminate the query process."""

import json
import os
import subprocess
import sys
import textwrap

import pytest


def test_issue_1217_reproducer(tmp_path):
    script = textwrap.dedent(
        """
        import sys
        from neug import Database

        db = Database(sys.argv[1], mode="w")
        conn = db.connect()
        try:
            conn.execute("CREATE NODE TABLE Doc(id INT64 PRIMARY KEY)")
            conn.execute("CREATE REL TABLE LINK(FROM Doc TO Doc, value FLOAT[2])")
            conn.execute("CREATE (:Doc {id:1}), (:Doc {id:2})")
            conn.execute('''
                MATCH (a:Doc {id:1}), (b:Doc {id:2})
                CREATE (a)-[:LINK {
                    value: CAST([CAST(2.0, 'FLOAT'), 1.0], 'FLOAT[2]')
                }]->(b)
            ''')
            assert list(conn.execute(
                "MATCH (a:Doc)-[e:LINK]->(b:Doc) RETURN a.id, b.id, e.value"
            )) == [[1, 2, [2.0, 1.0]]]
            assert list(conn.execute("RETURN 1")) == [[1]]
        finally:
            conn.close()
            db.close()
        """
    )
    result = subprocess.run(
        [sys.executable, "-X", "faulthandler", "-c", script, str(tmp_path / "db")],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("mode", ["ap", "session"])
@pytest.mark.parametrize(
    "elements, expected",
    [
        ("CAST(2.0, 'FLOAT'), 1.0", [2.0, 1.0]),
        ("1.0, CAST(2.0, 'FLOAT')", [1.0, 2.0]),
        ("2.0, 1.0", [2.0, 1.0]),
        ("CAST(2.0, 'FLOAT'), CAST(1.0, 'FLOAT')", [2.0, 1.0]),
        ("CAST(2, 'INT32'), 1.0", [2.0, 1.0]),
        ("CAST(a.id, 'FLOAT'), 1.0", [2.0, 1.0]),
        ("CAST($x, 'FLOAT'), 1.0", [2.0, 1.0]),
    ],
)
def test_mixed_numeric_array_in_subprocess(tmp_path, elements, expected, mode):
    script = textwrap.dedent(
        """
        import json
        import sys
        from neug import Database

        path, elements, expected, mode = sys.argv[1:]
        expected = json.loads(expected)
        db = Database(path, mode="w", checkpoint_on_close=False)
        conn = db.connect()
        try:
            conn.execute("CREATE NODE TABLE Doc(id INT64 PRIMARY KEY, v FLOAT[2])")
            conn.execute("CREATE REL TABLE LINK(FROM Doc TO Doc, value FLOAT[2])")
            conn.execute("CREATE (:Doc {id:1}), (:Doc {id:2})")
            if sys.argv[-1] == "session":
                import socket
                from neug.session import Session
                conn.close()
                with socket.socket() as sock:
                    sock.bind(("127.0.0.1", 0))
                    port = sock.getsockname()[1]
                endpoint = db.serve(port, "127.0.0.1", False)
                conn = Session.open(endpoint, timeout="30s")
            expr = "CAST([" + elements + "], 'FLOAT[2]')"
            prefix = "MATCH (a:Doc {id:2}) "
            params = {"x": 2.0}
            assert list(conn.execute(prefix + "RETURN " + expr,
                                     parameters=params)) == [[expected]]
            conn.execute(prefix + "CREATE (:Doc {id:3, v:" + expr + "})",
                         parameters=params)
            assert list(conn.execute("MATCH(n:Doc {id:3}) RETURN n.v")) == [[expected]]
            conn.execute("MATCH(a:Doc {id:2}),(b:Doc {id:1}) "
                         "CREATE (a)-[:LINK {value:" + expr + "}]->(b)",
                         parameters=params)
            assert list(conn.execute("MATCH()-[e:LINK]->() RETURN e.value")) == [[expected]]
            assert list(conn.execute("RETURN 1")) == [[1]]
        finally:
            conn.close()
            db.close()
        """
    )
    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "faulthandler",
            "-c",
            script,
            str(tmp_path / "db"),
            elements,
            json.dumps(expected),
            mode,
        ],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("mode", ["ap", "session"])
@pytest.mark.parametrize(
    "expression",
    [
        "CAST([CAST(2.0,'FLOAT'), 'invalid'], 'FLOAT[2]')",
        "CAST([CAST(a.id,'FLOAT'), 'invalid'], 'FLOAT[2]')",
        "CAST([CAST(2.0,'FLOAT'), 1.0], 'FLOAT[3]')",
        "CAST({x:2.0, y:1.0}, 'FLOAT[2]')",
    ],
)
def test_invalid_array_cast_keeps_process_and_connection_usable(
    tmp_path, expression, mode
):
    script = textwrap.dedent(
        """
        import sys
        from neug import Database

        db = Database(sys.argv[1], mode="w", checkpoint_on_close=False)
        conn = db.connect()
        try:
            conn.execute("CREATE NODE TABLE Doc(id INT64 PRIMARY KEY, v FLOAT[2])")
            conn.execute("CREATE REL TABLE LINK(FROM Doc TO Doc, value FLOAT[2])")
            conn.execute("CREATE (:Doc {id:1}), (:Doc {id:2})")
            if sys.argv[-1] == "session":
                import socket
                from neug.session import Session
                conn.close()
                with socket.socket() as sock:
                    sock.bind(("127.0.0.1", 0))
                    port = sock.getsockname()[1]
                endpoint = db.serve(port, "127.0.0.1", False)
                conn = Session.open(endpoint, timeout="30s")
            expr = sys.argv[2]
            for query in [
                "MATCH(a:Doc {id:2}) RETURN " + expr,
                "MATCH(a:Doc {id:2}) CREATE (:Doc {id:3,v:" + expr + "})",
                "MATCH(a:Doc {id:2}),(b:Doc {id:1}) "
                "CREATE (a)-[:LINK {value:" + expr + "}]->(b)",
            ]:
                try:
                    conn.execute(query)
                except Exception as error:
                    if sys.argv[-1] != "session":
                        assert isinstance(error, RuntimeError), error
                    assert "cast" in str(error).lower() or "length mismatch" in str(error).lower(), error
                else:
                    raise AssertionError("Expected query error: " + query)
                assert list(conn.execute("RETURN 1")) == [[1]]
                assert list(conn.execute("MATCH(n:Doc) RETURN count(n)")) == [[2]]
                assert list(conn.execute("MATCH()-[e:LINK]->() RETURN count(e)")) == [[0]]
        finally:
            conn.close()
            db.close()
        """
    )
    result = subprocess.run(
        [
            sys.executable,
            "-X",
            "faulthandler",
            "-c",
            script,
            str(tmp_path / "db"),
            expression,
            mode,
        ],
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
