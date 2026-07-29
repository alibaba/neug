# Developer Guide

### Building from Source

To compile NeuG from source, certain dependencies and tools must be installed.


As nearlly all dependencies are also included as third-party libraries in the NeuG repository, you could build NeuG locally by installing only a few essential packages.

#### On Ubuntu

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake git python3-dev python3-pip g++ make libssl-dev openssl
```

#### On macOS

```bash
brew update
brew install cmake git python3 openssl@3
# No need to install g++ since Apple Clang is also supported.
```

#### On CentOS7

```bash
# Update yum repository to use vault.centos.org for CentOS 7, as the main mirrors are no longer available.
sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo && \
    sed -i "s/^#.*baseurl=http/baseurl=http/g" /etc/yum.repos.d/*.repo && \
    sed -i "s/^mirrorlist=http/#mirrorlist=http/g" /etc/yum.repos.d/*.repo
sudo yum -y install centos-release-scl
sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo && \
    sed -i "s/^#.*baseurl=http/baseurl=http/g" /etc/yum.repos.d/*.repo && \
    sed -i "s/^mirrorlist=http/#mirrorlist=http/g" /etc/yum.repos.d/*.repo
sudo yum -y install epel-release
sudo yum -y groupinstall "Development Tools"
sudo yum -y install git python3 python3-pip make cmake3 openssl openssl-devel
sudo ln -sf /usr/bin/cmake3 /usr/local/bin/cmake

# Install newer gcc/g++ via devtoolset-10
sudo yum -y install devtoolset-10
scl enable devtoolset-10 bash
```

#### On CentOS8/CentOS Stream 8

```bash
sudo dnf -y install epel-release dnf-plugins-core
# Enable PowerTools (called PowerTools in CentOS 8; usually powertools in Stream 8)
sudo dnf config-manager --set-enabled powertools || sudo dnf config-manager --set-enabled PowerTools
sudo dnf -y groupinstall "Development Tools"
sudo dnf -y install git python3 python3-pip cmake gcc-c++ make
```

### Building NeuG with Python

With the environment ready, you can proceed to build NeuG with Python and use Python client.

**Build model**: NeuG uses a single root build tree at `<repo>/build/`. The
core engine is compiled once into a shared library `libneug.{dylib,so}`, and
each language binding (currently Python; future Node/Rust) builds its own `.so`
that dynamically links against the same core library. There is no separate
per-binding build of the core.

```
<repo>/build/
├── src/libneug.{dylib,so}             # core, built once
└── tools/python_bind/neug_py_bind*.so # binding, links to core
```

#### For Development Purposes

To configure the root build and compile the Python binding in one shot:

```bash
make python-dev
```

This bootstraps the root build (`cmake -S . -B build -DBUILD_PYTHON=ON`),
compiles the core, and builds the `neug_py_bind` target. Subsequent runs only
rebuild what changed. You can then import `neug` directly:

```bash
cd tools/python_bind
python3
>>> import neug
```

The loader auto-discovers `neug_py_bind*.so` in `<repo>/build/tools/python_bind/`
— no `sys.path` mangling needed. Point at an alternate build tree with
`NEUG_BUILD_DIR=/path/to/build`.

For the tightest incremental loop, run `make dev` directly from
`tools/python_bind/`. It auto-bootstraps if the root build is missing or
wasn't configured with `-DBUILD_PYTHON=ON`, so you can start with it from a
fresh checkout too.

#### Building the Wheel Package

```bash
make python-wheel
```

The wheel is written to `tools/python_bind/dist/`. It bundles both
`neug_py_bind*.so` and `libneug.{dylib,so}` inside the `neug` package — they
find each other at runtime via `@loader_path` (macOS) / `$ORIGIN` (Linux)
RPATH, so the wheel is fully self-contained.

`setup.py` consumes the root build artifacts. In CI (cibuildwheel containers
with no pre-existing build tree), it falls back to running cmake configure +
`--target neug_py_bind` in the container.

#### Building C++ Libraries and Executables Only

To build only the C++ libraries and executables without the Python bindings:

```bash
make cpp-build   # configures with -DBUILD_PYTHON=OFF, then builds
```

Both targets accept `BUILD_TYPE` (default `Release`) and `EXTRA_CMAKE_FLAGS`:

```bash
BUILD_TYPE=Debug make cpp-build
EXTRA_CMAKE_FLAGS="-DBUILD_HTTP_SERVER=ON -DWITH_MIMALLOC=ON" make cpp-build
```

Equivalent raw cmake form:

```bash
cmake -S . -B build -DBUILD_PYTHON=OFF
cmake --build build -j$(nproc)
cmake --install build   # optional: install to the system
```

Check `CMakeLists.txt` for more CMake options.

#### Build Options

You can customize the build process by specifying the following environment variables to set different options:

```bash
export BUILD_EXECUTABLES=ON/OFF # Toggle building utility executables
export BUILD_HTTP_SERVER=ON/OFF # Enable or disable HTTP server support in NeuG
export WITH_MIMALLOC=ON/OFF # Decide whether to use mimalloc instead of the default malloc from glibc
export ENABLE_BACKTRACES=ON/OFF # Link NeuG libraries with cpptrace for detailed stack trace on exceptions
export BUILD_TYPE=DEBUG/RELEASE # Set the CMake build type
export BUILD_TEST=ON/OFF # Toggle the building of test suites
```

Distributable artifacts such as Python wheels, npm tarballs, and release images
are built with `NEUG_PACKAGE_BUILD=ON` and `NEUG_NATIVE_ARCH=OFF`. Do not enable
native CPU tuning for package builds.

For local source builds only, enable host-specific CPU tuning with:

```bash
NEUG_NATIVE_ARCH=ON make cpp-build
NEUG_PACKAGE_BUILD=OFF NEUG_NATIVE_ARCH=ON make -C tools/python_bind build
NEUG_NATIVE_ARCH=ON make -C tools/nodejs_bind dev
```

#### Debugging

By default, C++ logging is disabled. To enable logging, use:

```bash
export DEBUG=ON
```

For more detailed logging, adjust the glog verbosity level with:

```bash
export GLOG_v=10 # Set globally
GLOG_v=10 python3 ... # Set for a single command
```

To further investigate issues like segmentation faults or other complexities, using gdb/lldb is recommended:

```bash
GLOG_v=10 gdb --args python3 -m pytest -sv tests/test_db_query.py
GLOG_v=10 lldb -- python3 -m pytest -sv tests/test_db_query.py
```

For additional debugging techniques, refer to the documentation for gdb and lldb respectively.

### Local Pre-Commit Checks

Before pushing code to GitHub, run local checks to catch issues early and save CI resources:

> **tips**: A Python environment is required for the check.
> Set one up with `python3 -m venv .venv && source .venv/bin/activate` or `conda create -n neug python=3.13 && conda activate neug`.

```bash
# Format check only (fast, recommended before commit)
make format-check

# Full check including build and tests (recommended before creating PR)
make full-check
```

The `format-check` validates C++ (clang-format) and Python (isort, black, flake8) code formatting. Any auto-fixable issues (clang-format, isort, black) will be corrected in-place; only flake8 issues require manual fixes.
The `full-check` additionally compiles the code and runs unit tests.

For more options, see `./scripts/pre_commit_check.sh --help`.

### Building NeuG with NodeJS

We also provide NodeJS client. Both clients share a single root build tree at `<repo>/build/`.
If you have already built NeuG with Python, the library `libneug.{dylib,so}` can be shared.

> **NOTE**: The NodeJS binding only supports **AP mode**. Exposing a raw HTTP port directly from a Node.js process to run TP mode is considered a dangerous practice — it bypasses the typical security layers (authentication, TLS termination, rate limiting, etc.) that a production server should have behind a proper reverse proxy or gateway. Therefore, the `serve()` / `Session` APIs have been intentionally removed from the NodeJS binding.  
If you really need TP mode, please deploy a dedicated NeuG server using the **C++** or **Python** binding, and then connect to it from Node.js via standard HTTP clients.

#### For Development Purposes

From repo root:
```bash
make node-dev
```

Or from `tools/nodejs_bind/`:
```bash
cd tools/nodejs_bind && make dev
```

`make dev` handles everything: installs npm deps (skipped if already installed),
configures cmake (cmake reuses cache internally if unchanged), builds, and stages
artifacts. Safe to run repeatedly.

Then NodeJS can automatically load these modules:
```bash
cd tools/nodejs_bind
const { Database } = require('neug');
```

#### Building NPM Package

The npm package is written to `tools/nodejs_bind/neug-<version>-<platform>-<arch>.tgz`,
which packs `neug_node_bind.node` and `libneug.{dylib, so}`.
Package builds are always configured as portable builds.
```bash
make node-pack
# or:
cd tools/nodejs_bind && make pack
```

### FAQ

#### `ImportError: cannot import 'neug_py_bind'`

The loader could not find `neug_py_bind*.so`. Check in this order:

1. Did you actually build the target? Run `cd tools/python_bind && make dev` —
   the `.so` should appear at `<repo>/build/tools/python_bind/`.
2. Are you pointing at the right build tree? If you maintain multiple, set
   `NEUG_BUILD_DIR=/path/to/build` to override the default `<repo>/build`.
3. Inspect what the loader is looking for:
   `python3 -c "from neug import __init__; print(__init__._find_neug_py_bind_dir())"`

#### `Library not loaded: @rpath/libneug.dylib` or transitive third-party libs

`neug_py_bind*.so` is set up with `BUILD_RPATH=@loader_path/../../src` so it
finds the sibling `libneug.dylib` automatically in the root build tree.
If you see this error:

- **For `libneug` itself**: your build tree is incomplete or you moved files.
  Re-run `cmake --build build --target neug_py_bind`.
- **For a third-party dep** (arrow, openssl, etc.): `libneug.dylib`'s
  transitive deps weren't found. Set `DYLD_LIBRARY_PATH=/opt/neug/lib` (macOS)
  or `LD_LIBRARY_PATH=/opt/neug/lib:/opt/neug/lib64` (Linux) to where they are
  installed, or rebuild with the deps statically linked.

To inspect what a `.so` is asking for:
```bash
otool -L build/tools/python_bind/neug_py_bind*.so   # macOS
ldd     build/tools/python_bind/neug_py_bind*.so    # Linux
```
