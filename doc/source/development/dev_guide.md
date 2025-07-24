# Development Guide

### Building from Source

To compile NeuG from source, certain dependencies and tools must be installed.

#### Building in Development Container

We provide Docker images for x86_64 and arm64 platforms, with all necessary dependencies for building NeuG from source.

```bash
docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug
docker run --it --name neug-dev registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug bash
# Inside the container, set up the environment variables.
source ~/.graphscope_env
```

#### Building Locally

You can also set up all required dependencies in your local environment using the provided script:

```bash
bash scripts/install_deps.sh --brpc --install-prefix /opt/graphscope
```

After installation is complete, set up the environment variables:

```bash
source ~/.graphscope_env
```

### Building NeuG

With the environment ready, you can proceed to build NeuG.

#### For Development Purposes

To build the NeuG Python package in development mode, execute:

```bash
make python-dev
```

This will compile the NeuG engine and Python client in development mode. You can use the NeuG package by running Python scripts from the `tools/python_bind` directory.

```bash
cd tools/python_bind
python3
>>> import neug
```

To use NeuG from other directories, add `tools/python_bind` to `sys.path`:

```python
import sys
sys.path.append('/path/to/neug/tools/python_bind')
```

#### Building the Wheel Package

To compile the wheel package, run:

```bash
make python-wheel
```

Afterwards, the wheel package can be found in `tools/python_bind/dist`.


#### Build Options

You can customize the build process by specifying the following environment variables to set different options:

```bash
export BUILD_EXECUTABLES=ON/OFF # Toggle building utility executables
export BUILD_HTTP_SERVER=ON/OFF # Enable or disable HTTP server support in NeuG
export BUILD_ALL_IN_ONE=ON/OFF # Control whether NeuG libraries are built as a single dynamic library or multiple separate libraries
export WITH_MIMALLOC=ON/OFF # Decide whether to use mimalloc instead of the default malloc from glibc
export ENABLE_BACKTRACES=ON/OFF # Link NeuG libraries with cpptrace for detailed stack trace on exceptions
export BUILD_TYPE=DEBUG/RELEASE # Set the CMake build type
export BUILD_TEST=ON/OFF # Toggle the building of test suites
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