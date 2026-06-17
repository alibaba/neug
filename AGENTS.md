# AGENTS.md

This file provides guidance to LLM tools when working with code in this repository.

## Project Overview

**NeuG** is a C++20 graph database for HTAP workloads with Cypher query support. Two modes: **Embedded** (analytics) and **Service** (transactional).

## Build & Test

**Parallelism limit**: Always cap `-j` to the number of physical cores. Use `cmake --build build -j$(sysctl -n hw.ncpu)` (macOS) or `cmake --build build -j$(nproc)` (Linux). **Never use bare `-j`** (unlimited parallelism) — it exhausts memory and freezes the machine on this codebase.

All bindings (Python, future Node/Rust) share a single root build tree at `<repo>/build/`. The core library `libneug.dylib`/`libneug.so` is built once; each binding produces its own `.so` that dynamically links to it.

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
BUILD_TEST=ON make cpp-test        # enable tests
BUILD_TYPE=Debug make cpp-build    # override build type
EXTRA_CMAKE_FLAGS="-DBUILD_HTTP_SERVER=ON" make cpp-build   # extra flags
```

### Local Build Command

Prefer this command set when the user asks to build NeuG through the Python binding workflow:

```bash
cd tools/python_bind
export BUILD_TEST=ON
export BUILD_TYPE=DEBUG
export BUILD_EXTENSIONS=parquet  # optional: parquet, httpfs (semicolon-separated, not built by default)
export CMAKE_BUILD_PARALLEL_LEVEL=$(sysctl -n hw.ncpu)
make build
```

### macOS Fallback Build Notes

If the machine does not have `cmake`, `ninja`, or OpenSSL development headers in system paths, keep the workaround local to the repository so other branches can reuse it without changing the host:

```bash
cd tools/python_bind
python3 -m venv .venv
.venv/bin/python -m pip install cmake ninja
```

Install OpenSSL under the repository, then point CMake at it:

```bash
curl -L https://www.openssl.org/source/openssl-3.3.1.tar.gz -o /tmp/openssl-3.3.1.tar.gz
tar -xzf /tmp/openssl-3.3.1.tar.gz -C /tmp
cd /tmp/openssl-3.3.1
./Configure darwin64-arm64-cc --prefix=<repo>/.local/openssl no-tests no-docs
make -j4
make install_sw
```

Known-good core + Python binding build on macOS when Service Mode/brpc or optional parquet extension linkage is blocking:

```bash
cd <repo>/tools/python_bind
export PATH="$PWD/.venv/bin:$PATH"
export BUILD_TEST=ON
export BUILD_TYPE=DEBUG
export BUILD_HTTP_SERVER=OFF
unset BUILD_EXTENSIONS
export CMAKE_BUILD_PARALLEL_LEVEL=4
export CMAKE_ARGS="-DOPENSSL_ROOT_DIR=<repo>/.local/openssl -DOPENSSL_USE_STATIC_LIBS=TRUE -DCRYPTO_LIB=<repo>/.local/openssl/lib/libcrypto.a -DBUILD_EXTENSIONS="
make build
```

This produces `build/src/libneug.dylib` and copies `neug_py_bind*.so` plus `libneug.dylib` into `tools/python_bind/build/lib.*`. On newer AppleClang/CMake 4.x, the downloaded Arrow 18.0.0 source may need local generated-build patches under `build/_deps/arrow-src`: accept `cctools_ld-*` in `cpp/cmake_modules/BuildUtils.cmake` and remove Arrow's `-Wdocumentation` additions in `cpp/cmake_modules/SetupCxxFlags.cmake`. These patches are build-tree workarounds, not source changes.

Current caveats from local macOS builds:
- `BUILD_HTTP_SERVER=ON` can fail in brpc generation because `PROTOC_LIB` is resolved incorrectly before `libprotoc` exists.
- `BUILD_EXTENSIONS=parquet` can fail when linking `libparquet.neug_extension` due to missing absl/protobuf symbols. Disable optional extensions when the goal is to compile NeuG core and Python bindings.
- If `sysctl -n hw.ncpu` is unavailable in a sandbox, use a conservative fixed value such as `CMAKE_BUILD_PARALLEL_LEVEL=4`; never use bare `-j`.

### Common Build Variables

- `BUILD_TYPE=DEBUG|RELEASE` — default Release
- `BUILD_TEST=ON` — build test suites
- `BUILD_PYTHON=ON` — build `neug_py_bind` target (off by default in pure C++ builds)
- `NEUG_BUILD_DIR=<path>` — override the root build dir consumed by `setup.py` and the Python loader (default `<repo>/build`)
- `DEBUG=ON` + `GLOG_v=10` — enable verbose C++ logging

### Running Tests

```bash
# Python tests — loader auto-finds neug_py_bind*.so in the root build dir
cd tools/python_bind
python3 -m pytest -s tests/test_db_query.py

# Python tests requested by path or pytest expression
cd tools/python_bind
python3 -m pytest ...

# C++ unit tests for the Python binding build tree
cd tools/python_bind/build/neug_py_bind
ctest -R ...

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

The wheel ships `neug_py_bind*.so` and `libneug.{dylib,so}` together; they find each other at runtime via `@loader_path` / `$ORIGIN` RPATH set in `tools/python_bind/CMakeLists.txt`.

### Pre-commit

```bash
# From repository root
make format-check    # clang-format + isort + black + flake8
```

### Format

```bash
# Install Python formatting/test dependencies if needed
cd tools/python_bind && make requirements

# C++ format from repository root
./format.sh

# Python format only for changed Python files
black <file>.py
isort <file>.py
```

## Git Workflow

```bash
# Push to the shirly remote branch by default unless the user explicitly specifies another remote.
git push shirly <branch>

# Create PRs from the shirly fork to the alibaba upstream repository.
gh pr create --repo alibaba/neug --base main --head shirly121:<branch> --title "<title>" --body "<body>"
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
