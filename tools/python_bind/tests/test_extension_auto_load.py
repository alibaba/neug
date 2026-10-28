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

"""Offline end-to-end coverage of extension loading during Database open.

Each scenario runs in a fresh process: extension registrations are process-wide.
Small native fixtures exercise Init failures without depending on published packages.
"""

import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

BINDING_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BINDING_ROOT.parents[1]


def run_python(home, code):
    env = os.environ.copy()
    env["NEUG_EXTENSION_HOME_PYENV"] = str(home)
    env["PYTHONPATH"] = str(BINDING_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        env=env,
        text=True,
        capture_output=True,
        timeout=90,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return completed


@pytest.fixture(scope="module")
def native_fixtures(tmp_path_factory):
    compiler = shutil.which("c++")
    if sys.platform == "win32" or not compiler:
        pytest.skip("Native Init fixtures require a POSIX C++ compiler")
    root = tmp_path_factory.mktemp("auto_load_libraries")
    for name, body in {
        "ok": (
            'std::ofstream(std::getenv("NEUG_EXTENSION_HOME_PYENV") + '
            'std::string("/init_calls"), std::ios::app) << "ok\\n";'
        ),
        "throws": 'throw std::runtime_error("test Init failure");',
        "unknown": "throw 42;",
        "missing": "",
    }.items():
        source = root / f"{name}.cc"
        entry = "Other" if name == "missing" else "Init"
        source.write_text(
            "#include <cstdlib>\n#include <fstream>\n#include <stdexcept>\n"
            f'extern "C" void {entry}() {{ {body} }}\n'
        )
        subprocess.run(
            [compiler, "-shared", "-fPIC", str(source), "-o", str(root / name)],
            check=True,
            capture_output=True,
        )
    return root


def install_fixture(home, name, library):
    target = home / "extension" / name / f"lib{name}.neug_extension"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(library, target)
    return target


@pytest.mark.parametrize(
    "option", ["", ", auto_load_ext=False", ", auto_load_ext=True"]
)
def test_default_and_explicit_auto_load(tmp_path, native_fixtures, option):
    install_fixture(tmp_path, "ok", native_fixtures / "ok")
    run_python(
        tmp_path,
        f"""
        from neug import Database
        db = Database(max_thread_num=1{option})
        conn = db.connect()
        assert list(conn.execute("RETURN 1")) == [[1]]
        conn.close()
        db.close()
        """,
    )
    marker = tmp_path / "init_calls"
    assert marker.exists() == ("True" in option)
    if marker.exists():
        assert marker.read_text() == "ok\n"


@pytest.mark.parametrize("value", ["None", "1", "0", "'true'", "[]"])
@pytest.mark.parametrize("native", [False, True])
def test_invalid_auto_load_type(tmp_path, value, native):
    constructor = "neug_py_bind.PyDatabase" if native else "Database"
    run_python(
        tmp_path,
        f"""
        from neug import Database, neug_py_bind
        try:
            {constructor}(auto_load_ext={value})
        except TypeError as exc:
            assert "auto_load_ext" in str(exc)
        else:
            raise AssertionError("Non-boolean config was accepted")
        """,
    )


@pytest.mark.parametrize("layout", ["absent", "empty", "file"])
def test_empty_or_invalid_root(tmp_path, layout):
    root = tmp_path / "extension"
    if layout == "empty":
        root.mkdir()
    elif layout == "file":
        root.write_text("not a directory")
    result = run_python(
        tmp_path,
        """
        from neug import Database
        db = Database(auto_load_ext=True, max_thread_num=1)
        conn = db.connect()
        assert list(conn.execute("RETURN 1")) == [[1]]
        conn.close()
        db.close()
        """,
    )
    assert ("[AutoLoadExtension]" in result.stderr) == (layout != "empty")
    if layout == "absent":
        assert "Extension directory does not exist" in result.stderr
        assert str(root) in result.stderr


def test_failures_continue_to_next_extension(tmp_path, native_fixtures):
    for name in ["throws", "unknown", "missing"]:
        install_fixture(tmp_path, f"a_{name}", native_fixtures / name)
    broken = tmp_path / "broken"
    broken.write_text("not a dynamic library")
    install_fixture(tmp_path, "a_broken", broken)
    install_fixture(tmp_path, "z_ok", native_fixtures / "ok")
    (tmp_path / "extension" / "incomplete").mkdir()
    (tmp_path / "extension" / "README").write_text("ignored")
    result = run_python(
        tmp_path,
        """
        from neug import Database
        db = Database(auto_load_ext=True, max_thread_num=1)
        conn = db.connect()
        assert list(conn.execute("RETURN 1")) == [[1]]
        # Manual LOAD still reports errors instead of silently swallowing them.
        try:
            conn.execute("LOAD a_broken")
        except Exception:
            pass
        else:
            raise AssertionError("Manual LOAD should fail")
        conn.close()
        db.close()
        """,
    )
    for name in ["a_throws", "a_unknown", "a_missing", "a_broken", "incomplete"]:
        assert f"extension={name}" in result.stderr
    assert "test Init failure" in result.stderr
    assert (tmp_path / "init_calls").read_text() == "ok\n"


def test_manual_load_deduplicates_and_reopen_rescans(tmp_path, native_fixtures):
    install_fixture(tmp_path, "ok", native_fixtures / "ok")
    run_python(
        tmp_path,
        """
        from neug import Database
        for _ in range(2):
            db = Database(auto_load_ext=True, max_thread_num=1)
            conn = db.connect()
            conn.execute("LOAD ok")
            conn.close()
            db.close()
        """,
    )
    assert (tmp_path / "init_calls").read_text() == "ok\nok\n"


def test_discovery_is_nonrecursive_and_supports_symlinks(tmp_path, native_fixtures):
    root = tmp_path / "extension"
    root.mkdir()
    nested = tmp_path / "nested_home"
    target = install_fixture(nested, "ok", native_fixtures / "ok")
    (root / "ok").symlink_to(target.parent, target_is_directory=True)
    (root / "dangling").symlink_to(tmp_path / "missing")
    install_fixture(tmp_path, "UPPER", native_fixtures / "ok")
    install_fixture(root / "container", "hidden", native_fixtures / "ok")
    result = run_python(
        tmp_path,
        """
        from neug import Database
        db = Database(auto_load_ext=True, max_thread_num=1)
        db.close()
        """,
    )
    assert (tmp_path / "init_calls").read_text() == "ok\n"
    assert "dangling" in result.stderr
    assert "UPPER" in result.stderr


@pytest.fixture
def vector_home(tmp_path):
    build = Path(os.environ.get("NEUG_BUILD_DIR", REPO_ROOT / "build"))
    home = Path(os.environ.get("NEUG_EXTENSION_HOME_PYENV", build))
    candidates = [
        home / "extension/vector_search/libvector_search.neug_extension",
        build / "extension/vector_search/libvector_search.neug_extension",
    ]
    library = next((p for p in candidates if p.is_file()), None)
    if library is None:
        pytest.skip("Build vector_search to exercise index recovery")
    install_fixture(tmp_path, "vector_search", library)
    return tmp_path


@pytest.mark.parametrize("mode", ["r", "w"])
@pytest.mark.parametrize("automatic", [True, False])
@pytest.mark.parametrize("with_wal", [True, False])
def test_index_recovery_and_delayed_manual_load(vector_home, mode, automatic, with_wal):
    db_path = str(vector_home / "db")
    run_python(
        vector_home,
        f"""
        from neug import Database
        db = Database({db_path!r}, auto_load_ext=True, max_thread_num=1)
        conn = db.connect()
        conn.execute("CREATE NODE TABLE Item(id STRING PRIMARY KEY, embedding FLOAT[4]);")
        conn.execute("CREATE (:Item {{id: 'a', embedding: [1.0, 0.0, 0.0, 0.0]}});")
        conn.execute("CREATE INDEX item_hnsw ON Item USING HNSW (embedding) WITH (metric = 'ip');")
        conn.close()
        db.close()
        """,
    )
    if with_wal:
        run_python(
            vector_home,
            f"""
            from neug import Database
            db = Database({db_path!r}, auto_load_ext=True, checkpoint_on_close=False,
                          max_thread_num=1)
            conn = db.connect()
            conn.execute("MATCH (n:Item {{id: 'a'}}) SET n.embedding = [2.0, 0.0, 0.0, 0.0];")
            conn.close()
            db.close()
            """,
        )
    run_python(
        vector_home,
        f"""
        from neug import Database
        db = Database({db_path!r}, mode={mode!r}, auto_load_ext={automatic!r},
                      checkpoint_on_close=False, max_thread_num=1)
        conn = db.connect()
        if not {automatic!r}:
            if {mode!r} == "r":
                try:
                    conn.execute("LOAD vector_search")
                except RuntimeError as exc:
                    assert "read-only" in str(exc)
                else:
                    raise AssertionError("Read-only admin restriction changed")
                assert list(conn.execute("MATCH (n:Item) RETURN n.id")) == [["a"]]
                conn.close()
                db.close()
                raise SystemExit(0)
            conn.execute("LOAD vector_search")
        result = conn.execute(
            "PROFILE MATCH (n:Item) RETURN n.id, "
            "vector_distance_ip(n.embedding, [1.0, 0.0, 0.0, 0.0]) "
            "AS score ORDER BY score DESC LIMIT 1;")
        rows = list(result)
        operators = result.get_profile_metrics()["operators"]
        assert "IndexScanOpr" in [op["operator_name"] for op in operators], operators
        assert rows == [["a", {2.0 if with_wal else 1.0}]], rows
        conn.close()
        db.close()
        """,
    )


def test_auto_load_does_not_hide_database_errors(tmp_path, native_fixtures):
    install_fixture(tmp_path, "ok", native_fixtures / "ok")
    run_python(
        tmp_path,
        f"""
        from neug import Database, neug_py_bind
        try:
            Database({str(tmp_path / 'nonexistent')!r}, mode="r", auto_load_ext=True)
        except Exception:
            pass
        else:
            raise AssertionError("Missing read-only database should fail")
        try:
            neug_py_bind.PyDatabase(mode="w", planner="invalid", auto_load_ext=True)
        except Exception as exc:
            assert "planner" in str(exc).lower()
        else:
            raise AssertionError("Invalid planner should fail")
        """,
    )


def test_permission_failure_is_logged(tmp_path):
    if os.name != "posix" or os.geteuid() == 0:
        pytest.skip("Permission test requires an unprivileged POSIX user")
    root = tmp_path / "extension"
    root.mkdir()
    root.chmod(0)
    try:
        result = run_python(
            tmp_path,
            """
            from neug import Database
            db = Database(auto_load_ext=True, max_thread_num=1)
            db.close()
            """,
        )
        assert "[AutoLoadExtension] stage=discover" in result.stderr
    finally:
        root.chmod(0o700)
