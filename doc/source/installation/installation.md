 # Install NeuG

NeuG is a HTAP graph database designed for high-performance graph analytics and real-time transaction processing. It supports multiple platforms and programming languages.

## System Requirements

### Supported Operating Systems
- **Linux**: Ubuntu 13.10+, Debian 8+, Fedora 19+, RHEL 7+
- **macOS**: macOS 11.0+ (Big Sur and later)

### Supported Architectures
- **x86_64** (Intel/AMD 64-bit)
- **ARM64** (Apple Silicon, ARM-based servers)

### Hardware Requirements
- **CPU**: Multi-core processor recommended for optimal performance

## Python Installation

NeuG Python package is available on PyPI and supports Python 3.8+.

```{note}
**Requirements**: Python >= 3.8
```

### Create a virtual environment

```bash
python3 -m venv neug-env
source neug-env/bin/activate
```

### Install from PyPI

```bash
# Install the latest version
pip install neug

# Install with specific index (if needed)
pip install neug -i https://pypi.org/simple

# Install from Aliyun mirror (China users)
pip install neug -i https://mirrors.aliyun.com/pypi/simple/
```

### Verify Installation

```python
import neug

# Check version
print(neug.__version__)

# Create a simple in-memory database
db = neug.Database(db_path="")
conn = db.connect()
print("NeuG installation successful!")
```


## C++ Installation

### Manual Installation

Currently we only support building from source. Refer to [Development](../development/dev_guide.md) for prepare the building environment. 
When the environment is ready, build and install NeuG using cmake.

```bash
git clone https://github.com/GraphScope/neug.git
cd neug
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/opt/neug # see CMakeLists.txt for more cmake options.
make -j$(proc)
make install
```

### Verify Installation

In your cmake project, find and link NeuG libraries with the following command:

```cmake
cmake_minimum_required (VERSION 3.10)
project (
  NeuGTest
  VERSION 0.1
  LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)


find_package(neug REQUIRED)
add_executable(test test.cc)
include_directories(${NEUG_INCLUDE_DIRS})
target_link_libraries(test ${NEUG_LIBRARIES})
```

A sample test.cc looks like: 

```cpp
#include <neug/main/neug_db.h>
#include <iostream>

int main() {
  gs::NeugDB db("test_db");
  auto conn = db.connect();
  std::cout << "NeuG C++ client installation successful!" << std::endl;
  return 0;
}
```

Build and run the test:

```bash
mkdir build && cd build
cmake .. -DCMAKE_PREFIX_PATH=/opt/neug
./test
```

## Command Line Interface (CLI)

The NeuG CLI tool is provided along with NeuG.

### Installation

```bash
# Install via pip (includes CLI tool)
pip install neug
```

### Usage

```bash
# Start interactive shell with in-memory database
neug-cli

# Connect to an embedded local database
# Will run in embedded mode.
neug-cli /path/to/database

# Connect to a remote database service
# Must start the service first
neug-cli neug://localhost:8080

# Execute a query directly
neug-cli -c "MATCH (n) RETURN count(n)" /path/to/database
```

### Verify Installation

```bash
neug-cli --version
```


## Platform-Specific Notes

### Linux
- Ensure you have the required system libraries installed
- For older distributions, you may need to install a newer version of glibc

### macOS
- Both Intel and Apple Silicon are supported
- Homebrew installation is recommended for development

## Troubleshooting

### Common Issues

#### Python Installation Issues
```bash
# If you encounter permission errors
pip install --user neug

```

#### Missing Dependencies
```bash
# Linux: Install build essentials
sudo apt-get install build-essential

# macOS: Install Xcode command line tools
xcode-select --install
```

#### Version Conflicts
```bash
# Check installed version
pip show neug

# Upgrade to latest version
pip install --upgrade neug
```

### Getting Help

If you encounter issues during installation:
1. Check the [troubleshooting guide](../troubleshooting/index.md)
2. Visit our [GitHub issues](https://github.com/graphscope/neug/issues)

## Next Steps

Once you have NeuG installed, you can:
1. Follow the [Getting Started guide](../getting-started/index.md)
2. Explore [connection modes](../connection-modes/index.md)
3. Try the [tutorials](../tutorials/index.md)
