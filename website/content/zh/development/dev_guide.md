# 开发指南

### 从源码构建

要从源码编译 NeuG，必须先安装某些依赖项和工具。

#### 在开发容器中构建

我们为 x86_64 和 arm64 平台提供了 Docker 镜像，其中包含从源码构建 NeuG 所需的所有依赖项。

```bash
docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev
docker run --it --name neug-dev registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev bash

# 在容器内，设置环境变量。
source ~/.neug_env
```

#### 本地构建

你也可以使用提供的脚本在本地环境中安装所有必需的依赖项：

```bash
bash scripts/install_deps.sh --brpc --install-prefix /opt/neug
```

安装完成后，设置环境变量：

```bash
source ~/.neug_env
```

### 构建 NeuG

环境准备就绪后，即可开始构建 NeuG。

#### 用于开发目的

要以开发模式构建 NeuG Python 包，请执行：

```bash
make python-dev
```

这将编译 NeuG 引擎和 Python 客户端的开发版本。你可以通过在 `tools/python_bind` 目录下运行 Python 脚本来使用 NeuG 包。

```bash
cd tools/python_bind
python3
>>> import neug
```

要从其他目录使用 NeuG，请将 `tools/python_bind` 添加到 `sys.path`：

```python
import sys
sys.path.append('/path/to/neug/tools/python_bind')
```

#### 构建 Wheel 包

要编译 wheel 包，请运行：

```bash
make python-wheel
```

之后，wheel 包可以在 `tools/python_bind/dist` 目录中找到。

#### 构建选项

你可以通过设置以下环境变量来自定义构建过程：

```bash
export BUILD_EXECUTABLES=ON/OFF # 控制是否构建工具可执行文件
export BUILD_HTTP_SERVER=ON/OFF # 启用或禁用 NeuG 中的 HTTP 服务器支持
export BUILD_ALL_IN_ONE=ON/OFF # 控制 NeuG 库是构建为单个动态库还是多个独立库
export WITH_MIMALLOC=ON/OFF # 决定是否使用 mimalloc 替代 glibc 默认的 malloc
export ENABLE_BACKTRACES=ON/OFF # 将 NeuG 库与 cpptrace 链接以在异常时获取详细的堆栈跟踪信息
export BUILD_TYPE=DEBUG/RELEASE # 设置 CMake 构建类型
export BUILD_TEST=ON/OFF # 控制是否构建测试套件
```

#### 调试

默认情况下，C++ 日志记录是禁用的。要启用日志记录，请使用：

```bash
export DEBUG=ON
```

对于更详细的日志记录，可以通过以下方式调整 glog 的详细级别：

```bash
export GLOG_v=10 # 全局设置
GLOG_v=10 python3 ... # 为单个命令设置
```

要进一步排查段错误或其他复杂问题，建议使用 gdb/lldb：

```bash
GLOG_v=10 gdb --args python3 -m pytest -sv tests/test_db_query.py
GLOG_v=10 lldb -- python3 -m pytest -sv tests/test_db_query.py
```

有关更多调试技巧，请参考 gdb 和 lldb 的相应文档。

### FAQ

#### 在 macOS 上运行 e2e 查询失败

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

要解决这个问题，需要手动安装 rpath：

```bash
cd tools/python_bind/
install_name_tool -add_rpath /opt/neug/lib ./build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so
```