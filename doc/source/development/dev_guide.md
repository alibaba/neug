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

#### Building C++ Libraries and Executables Only

To build only the C++ libraries and executables without the Python bindings, use:

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
# Optional: run tests
ctest
# Optional: install to the system
make install
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