# Code Style Guide

This document provides the coding style guidelines for GraphScope's codebase, which includes C++, Python, Rust, Java, and shell scripts.
Adhering to consistent coding standards across the codebase promotes maintainability, readability, and code quality.

## C++ Style

We follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) for coding standards in C++.

## Python Style

We follow the [black](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html) code style for Python coding standards.

## Style Linter and Checker

GraphScope uses different linters and checkers for each language to enforce code style rules:

- C++: [clang-format-8](https://releases.llvm.org/8.0.0/tools/clang/docs/ClangFormat.html) and [cpplint](https://github.com/cpplint/cpplint)
- Python: [Flake8](https://flake8.pycqa.org/en/latest/)

Each linter can be included in the build process to ensure that the code adheres to the style guide.
Below are the commands to check the code style in each language:

For C++, format and lint the code by the MakeFile command:

```bash
# format
$ make neug_clformat
```

For Python:

- Install dependencies first:

```bash
$ pushd tools/python_bind
$ python3 -m pip  install -r requirements_dev.txt
$ popd
```

- Check the style:

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