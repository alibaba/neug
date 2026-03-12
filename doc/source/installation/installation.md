 # Installation

NeuG runs on modern Linux and macOS systems with support for both x86 and ARM architectures. For Windows users, we recommend using [WSL](https://learn.microsoft.com/en-us/windows/wsl/install). 

## Python Setup Details

**Requirements**: Python 3.8 or later

### Option 1: Direct Install
```bash
pip install neug
```

### Option 2: Using Virtual Environment (Recommended)
```bash
python3 -m venv neug-env
source neug-env/bin/activate
pip install neug
```

### Option 3: Alternative Sources
```bash
pip install neug -i https://mirrors.aliyun.com/pypi/simple/
```

### Verify Installation
```python
import neug

# Test with in-memory database
db = neug.Database("")
conn = db.connect()
print("✅ NeuG is ready!")
```


## C++ Installation

### Build from Source

See the [Developer Guide](../../development/dev_guide) for detailed build instructions. Quick overview:

```bash
git clone https://github.com/alibaba/neug.git
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
  gs::NeugDB db;
  db.Open("test_db");
  auto conn = db.Connect();
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

## Command Line Interface

The CLI tool is automatically included with the Python installation:

```bash
# Try it out
neug-cli --version

# Quick start with in-memory database
neug-cli
```

See [CLI Documentation](../../user_clients/cli) for more details.

## Troubleshooting

**Permission errors?**
```bash
pip install --user neug
```

**Import errors?**
```bash
pip install --upgrade neug
```

**Need help?** Visit our [GitHub issues](https://github.com/graphscope/neug/issues)

## Next Steps

🚀 **[Getting Started Guide](../../getting_started/getting_started)** - Your first graph database

📚 **[TinySnb Tutorial](../../tutorials/tinysnb_tutorial)** - Hands-on examples
