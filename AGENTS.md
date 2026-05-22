# AGENTS.md

This file provides guidance to LLM tools when working with code in this repository.

## Project Overview

**NeuG** is a C++20 graph database for HTAP workloads with Cypher query support. Two modes: **Embedded** (analytics) and **Service** (transactional).

## Build & Test

### Quick Start

```bash
# Python development (recommended)
cd tools/python_bind
make requirements && make build

# C++ only
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=DEBUG -DBUILD_TEST=ON
make -j$(nproc) && ctest
```

### Common Build Variables

- `BUILD_TYPE=DEBUG|RELEASE` — default Release
- `BUILD_TEST=ON` — build test suites
- `DEBUG=ON` + `GLOG_v=10` — enable verbose C++ logging

### Running Tests

```bash
# Python tests (from tools/python_bind/)
python3 -m pytest -s tests/test_db_query.py

# C++ tests (from build dir)
ctest

# Debugging with verbose logging
GLOG_v=10 lldb -- python3 -m pytest -sv tests/test_db_query.py
```

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

