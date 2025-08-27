# 安装 NeuG

NeuG 是一个 HTAP 图数据库，专为高性能图分析和实时事务处理而设计。它支持多种平台和编程语言。

## 系统要求

### 支持的操作系统
- **Linux**: Ubuntu 13.10+, Debian 8+, Fedora 19+, RHEL 7+
- **macOS**: macOS 11.0+ (Big Sur 及更高版本)

### 支持的架构
- **x86_64** (Intel/AMD 64-bit)
- **ARM64** (Apple Silicon, ARM-based servers)

### 硬件要求
- **CPU**: 建议使用多核处理器以获得最佳性能

## Python 安装

NeuG Python 包可在 PyPI 上获取，支持 Python 3.8+。

```{note}
**Requirements**: Python >= 3.8
```

### 创建虚拟环境

```bash
python3 -m venv neug-env
source neug-env/bin/activate
```

### 从 PyPI 安装

```bash

# 安装最新版本
pip install neug

# 使用特定 index 安装（如需要）
pip install neug -i https://pypi.org/simple

# 从阿里云镜像安装 (中国用户)
pip install neug -i https://mirrors.aliyun.com/pypi/simple/
```

### 验证安装

```python
import neug

# 检查版本
print(neug.__version__)

# 创建一个简单的内存数据库
db = neug.Database(db_path="")
conn = db.connect()
print("NeuG 安装成功!")
```

## C++ 安装

### 手动安装

目前我们只支持从源码构建。请参考 [Development](../development/dev_guide.md) 准备构建环境。
当环境准备就绪后，使用 cmake 构建并安装 NeuG。

```bash
git clone https://github.com/GraphScope/neug.git
cd neug
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/opt/neug # 更多 cmake 选项请查看 CMakeLists.txt。
make -j$(proc)
make install
```

### 验证安装

在你的 cmake 项目中，使用以下命令查找并链接 Neug 库：

```cmake
cmake_minimum_required (VERSION 3.10)
project (
  NeugTest
  VERSION 0.1
  LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)


find_package(neug REQUIRED)
add_executable(test test.cc)
include_directories(${NEUG_INCLUDE_DIRS})
target_link_libraries(test ${NEUG_LIBRARIES})
```

一个示例 test.cc 文件如下：

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

构建并运行测试：

```bash
mkdir build && cd build
cmake .. -DCMAKE_PREFIX_PATH=/opt/neug
./test
```

## 命令行界面 (CLI)

NeuG CLI 工具提供了一个用于数据库操作的交互式 shell。

### 安装

```bash

# 通过 pip 安装（包含 CLI 工具）
pip install neug

```markdown
# 或者只安装 CLI
pip install neug-cli
```

### 使用方法

```bash

# 启动交互式 shell，使用内存数据库
neug-cli

# 连接到嵌入式本地数据库

# 将以嵌入模式运行
neug-cli /path/to/database

# 连接到远程数据库服务

# 必须先启动服务
neug-cli neug://localhost:8080

# 直接执行查询
neug-cli -c "MATCH (n) RETURN count(n)" /path/to/database
```

### 验证安装

```bash
neug-cli --version
```

## 平台特定说明

### Linux
- 确保已安装所需的系统库
- 对于较旧的发行版，你可能需要安装更新版本的 glibc

### macOS
- 支持 Intel 和 Apple Silicon
- 推荐使用 Homebrew 进行开发环境安装

## 故障排除

### 常见问题

#### Python 安装问题
```bash

# 如果遇到权限错误
pip install --user neug

```

#### 缺少依赖项
```bash

```bash
# Linux: 安装 build essentials
sudo apt-get install build-essential

# macOS: 安装 Xcode command line tools
xcode-select --install
```

#### 版本冲突
```bash
# 检查已安装版本
pip show neug

# 升级到最新版本
pip install --upgrade neug
```

### 获取帮助

如果在安装过程中遇到问题：
1. 查看 [troubleshooting guide](../troubleshooting/index.md)
2. 访问我们的 [GitHub issues](https://github.com/graphscope/neug/issues)

## 下一步

安装完 NeuG 后，你可以：
1. 跟随 [Getting Started guide](../getting-started/index.md)
2. 探索 [connection modes](../connection-modes/index.md)
3. 尝试 [tutorials](../tutorials/index.md)