# AGENTS.md

This file provides guidance to LLM tools when working with code in this repository.

## Project Overview

**NeuG** is a C++20 graph database for HTAP workloads with Cypher query support. Two modes: **Embedded** (analytics) and **Service** (transactional).

## Build & Test

All bindings (Python, future Node/Rust) share a single root build tree at
`<repo>/build/`. The core library `libneug.dylib`/`libneug.so` is built once;
each binding produces its own `.so` that dynamically links to it.

### Quick Start

```bash
# One-shot from repo root: configures root build, compiles core +
# neug_py_bind, installs Python deps. Idempotent — also serves as the
# incremental rebuild entry point.
make python-dev
```

Or directly from `tools/python_bind/`:
```bash
make requirements    # one-time: install Python deps
make dev             # incremental rebuild; auto-bootstraps if root build
                     # is missing or wasn't configured with BUILD_PYTHON=ON
```

For **C++ only** (no Python):
```bash
make cpp-build                     # core + executables, Release
make cpp-test                      # adds -DBUILD_TEST=ON and runs ctest
BUILD_TYPE=Debug make cpp-build    # override build type
EXTRA_CMAKE_FLAGS="-DBUILD_HTTP_SERVER=ON" make cpp-build   # extra flags
```

### Common Build Variables

- `BUILD_TYPE=DEBUG|RELEASE` — default Release
- `BUILD_TEST=ON` — build test suites
- `BUILD_PYTHON=ON` — build `neug_py_bind` target (off by default in pure C++ builds)
- `NEUG_BUILD_DIR=<path>` — override the root build dir consumed by `setup.py`
  and the Python loader (default `<repo>/build`)
- `DEBUG=ON` + `GLOG_v=10` — enable verbose C++ logging

### Running Tests

```bash
# Python tests — loader auto-finds neug_py_bind*.so in the root build dir
cd tools/python_bind
python3 -m pytest -s tests/test_db_query.py

# C++ tests
ctest --test-dir build

# Debugging with verbose logging
GLOG_v=10 lldb -- python3 -m pytest -sv tests/test_db_query.py
```

### Building a Wheel

```bash
cd tools/python_bind
make wheel    # reuses root build; produces dist/neug-*.whl with libneug bundled
```

The wheel ships `neug_py_bind*.so` and `libneug.{dylib,so}` together; they find
each other at runtime via `@loader_path` / `$ORIGIN` RPATH set in
`tools/python_bind/CMakeLists.txt`.

### Pre-commit

```bash
# From repository root
make format-check    # clang-format + isort + black + flake8
```

## Architecture

```
include/neug/    # Public C++ headers (mirrors src/)
src/
├── compiler/    # ANTLR4 Cypher parser → logical plan → physical plan (via gopt/)
├── execution/   # Physical operators: scan, filter, project, join, aggregation
├── storages/    # CSR-based graph storage, schema, property columns
├── main/        # Core DB implementation: neug_db, connection, query processor
└── server/      # HTTP server for Service Mode
tools/python_bind/
├── src/         # pybind11 bindings
├── neug/        # Python API: Database, Connection, Session
└── tests/       # Python test suite
```

### Query Pipeline

```
Cypher → ANTLR Parser → Binder → Logical Plan → gopt Converter → Physical Plan → Execution
```

## Code Style

- **C++**: C++20, clang-format (style=file)
- **Python**: isort, black, flake8

