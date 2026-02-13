#!/usr/bin/env bash
set -euo pipefail

# Starts a NeuG HTTP server via the Python bindings and runs the Java client tests.
# Ensures the Python server is cleaned up even if Maven fails.

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PY_BIND_DIR="${ROOT_DIR}/tools/python_bind"
JAVA_DIR="${ROOT_DIR}/tools/java"
SERVER_PORT=10015
SERVER_HOST="localhost"
SERVER_BASE="http://${SERVER_HOST}:${SERVER_PORT}"

server_pid=""

cleanup() {
	if [[ -n "${server_pid}" ]] && ps -p "${server_pid}" > /dev/null 2>&1; then
		kill -TERM "${server_pid}" 2>/dev/null || true
		wait "${server_pid}" 2>/dev/null || true
	fi
}
trap cleanup EXIT INT TERM

start_server() {
	cd "${PY_BIND_DIR}"
	# Launch a minimal NeuG instance serving on port 10000.
	python3 - <<'PY' &
import shutil
import signal
import sys
import tempfile
import time

from neug.database import Database

db_dir = tempfile.mkdtemp(prefix="neug_java_test_")
db = Database(db_dir, "w")

# Create a tiny dataset so Java tests have something to read.
conn = db.connect()
conn.execute("CREATE NODE TABLE person(id INT64, name STRING, PRIMARY KEY(id));")
conn.execute("CREATE (p:person {id: 1, name: 'alice'});")
conn.execute("CREATE (p:person {id: 2, name: 'bob'});")
conn.close()

uri = db.serve(10015, "localhost", False)
print(f"NeuG server started at {uri}", flush=True)

def _cleanup_and_exit(code=0):
		try:
				db.stop_serving()
				db.close()
		finally:
				shutil.rmtree(db_dir, ignore_errors=True)
		sys.exit(code)

def _handler(signum, frame):
		_cleanup_and_exit(0)

signal.signal(signal.SIGINT, _handler)
signal.signal(signal.SIGTERM, _handler)

try:
		while True:
				time.sleep(1)
except KeyboardInterrupt:
		_cleanup_and_exit(0)
PY
	server_pid=$!
}

wait_for_server() {
	local retry=10
	until curl -sf "${SERVER_BASE}/service_status" > /dev/null 2>&1; do
		retry=$((retry - 1))
		if [[ ${retry} -le 0 ]]; then
			echo "NeuG server failed to start" >&2
			exit 1
		fi
		sleep 1
	done
}

run_maven_tests() {
	cd "${JAVA_DIR}"
    export NEUG_SERVER_URI="${SERVER_BASE}/cypher"
	if ! mvn test; then
		echo "mvn test failed" >&2
		exit 1
	fi
}

start_server
wait_for_server
run_maven_tests
