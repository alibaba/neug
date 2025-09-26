# Developer Guide

### Building from Source

To compile NeuG from source, certain dependencies and tools must be installed.

#### Building in Development Container

We provide Docker images for x86_64 and arm64 platforms, with all necessary dependencies for building NeuG from source.

```bash
docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev
docker run --it --name neug-dev registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev bash
# Inside the container, set up the environment variables.
source ~/.neug_env
```

#### Building Locally

You can also set up all required dependencies in your local environment using the provided script:

```bash
bash scripts/install_deps.sh --brpc --install-prefix /opt/neug
```

After installation is complete, set up the environment variables:

```bash
source ~/.neug_env
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

### FAQ

#### Fail to run e2e queries on macos

```
➜  e2e git:(main) ✗ ./scripts/run_embed_test.sh modern_graph /tmp/modern_graph basic_test
Running command: python3 -m pytest -sv -m neug_test --query_dir=/Users/zhanglei/code/nexg/tests/e2e/scripts/../queries/basic_test --dataset modern_graph --db_dir=/tmp/modern_graph --read_only --html="/Users/zhanglei/code/nexg/tests/e2e/scripts/../report/modern_graph/basic_test/test_report.html" --alluredir="/Users/zhanglei/code/nexg/tests/e2e/scripts/../report/modern_graph/basic_test/allure-results"
INFO:neug:Found build directories: ['lib.macosx-15.0-arm64-cpython-313']
INFO:neug:Selected build directory: lib.macosx-15.0-arm64-cpython-313
INFO:neug:Selected build directory: lib.macosx-15.0-arm64-cpython-313
INFO:neug:Build directory: /Users/zhanglei/code/nexg/tests/e2e/../../tools/python_bind/neug/../build/lib.macosx-15.0-arm64-cpython-313
INFO:neug:Adding build directory to sys.path: /Users/zhanglei/code/nexg/tests/e2e/../../tools/python_bind/neug/../build/lib.macosx-15.0-arm64-cpython-313
ImportError while loading conftest '/Users/zhanglei/code/nexg/tests/e2e/conftest.py'.
conftest.py:31: in <module>
    from neug import Session
../../tools/python_bind/neug/__init__.py:149: in <module>
    raise ImportError(
E   ImportError: NeuG is not installed. Please install it using pip or build it from source: dlopen(/Users/zhanglei/code/nexg/tests/e2e/../../tools/python_bind/neug/../build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so, 0x0002): Library not loaded: @rpath/libboost_filesystem.dylib
E     Referenced from: <F7ACB223-7AD2-4427-B5F6-5BA4505CFA4F> /Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so
E     Reason: tried: '/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/opt/homebrew/lib/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/opt/homebrew/lib/libboost_filesystem.dylib' (no such file), '/opt/homebrew/lib/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/opt/homebrew/lib/libboost_filesystem.dylib' (no such file)
```

To fix this issue, install the rpath manually

```bash
cd tools/python_bind/
install_name_tool -add_rpath /opt/neug/lib ./build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so
```