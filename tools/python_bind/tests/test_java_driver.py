#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import socket
import subprocess
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from neug.database import Database


def wait_until_ready(host, port, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(1)
    raise RuntimeError(f"Timed out waiting for NeuG server on {host}:{port}")


def test_java_driver_e2e():
    host = os.environ.get("NEUG_JAVA_DRIVER_E2E_HOST", "127.0.0.1")
    port = int(os.environ.get("NEUG_JAVA_DRIVER_E2E_PORT", "10010"))
    db_path = os.environ.get("NEUG_JAVA_DRIVER_E2E_DB_PATH", "/tmp/modern_graph")
    test_name = os.environ.get("NEUG_JAVA_DRIVER_E2E_TEST", "JavaDriverE2ETest")
    repo_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    java_driver_dir = os.path.join(repo_root, "tools", "java_driver")
    endpoint = f"http://{host}:{port}"

    db = Database(db_path=db_path, mode="w")
    try:
        db.serve(host=host, port=port, blocking=False)
        wait_until_ready(host, port)

        env = os.environ.copy()
        env["NEUG_JAVA_DRIVER_E2E_URI"] = endpoint

        result = subprocess.run(
            ["mvn", "-q", f"-Dtest={test_name}", "test"],
            cwd=java_driver_dir,
            env=env,
            check=False,
        )
        assert result.returncode == 0
    finally:
        try:
            db.stop_serving()
        except Exception:
            pass
        try:
            db.close()
        except Exception:
            pass
