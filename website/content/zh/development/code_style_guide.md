# 代码风格指南

本文档提供了 GraphScope 代码库的编码风格指南，涵盖 C++、Python、Rust、Java 和 shell 脚本。
在整个代码库中遵循一致的编码标准有助于提升可维护性、可读性和代码质量。

## C++ 风格

我们在 C++ 编码标准中遵循 [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)。

## Python 风格

我们在 Python 编码标准中遵循 [black](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html) 代码风格。

## 代码风格检查工具

GraphScope 为每种语言使用不同的 linter 和 checker 来强制执行代码风格规范：

- C++: [clang-format-8](https://releases.llvm.org/8.0.0/tools/clang/docs/ClangFormat.html) 和 [cpplint](https://github.com/cpplint/cpplint)
- Python: [Flake8](https://flake8.pycqa.org/en/latest/)

每个 linter 都可以集成到构建过程中，确保代码符合风格指南。
以下是检查各语言代码风格的命令：

对于 C++，通过 MakeFile 命令来格式化和检查代码：

```bash

# 格式化
$ make neug_clformat
```

对于 Python：

- 首先安装依赖：

```bash
$ pushd tools/python_bind
$ pip3 install -r requirements_dev.txt
$ popd
```

- 检查代码风格：

```bash
$ pushd tools/python_bind
$ python3 -m isort --check --diff .
$ python3 -m black --check --diff .
$ python3 -m flake8 .
$ popd
$ pushd tools/shell
$ python3 -m isort --check --diff .
$ python3 -m black --check --diff .
$ python3 -m flake8 .
$ popd
```