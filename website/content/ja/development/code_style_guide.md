# Code Style Guide

このドキュメントは、GraphScope のコードベースにおけるコーディングスタイルガイドラインを提供します。コードベースには C++、Python、Rust、Java、および shell スクリプトが含まれます。
コードベース全体で一貫したコーディング標準に従うことで、保守性、可読性、およびコード品質が向上します。

## C++ Style

C++ のコーディング標準については、[Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) に従います。

## Python Style

Python のコーディング標準については、[black](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html) のコードスタイルに従います。

## Style Linter と Checker

GraphScope では、各言語に対して異なる linter と checker を使用して、コードスタイルのルールを強制しています：

- C++: [clang-format-8](https://releases.llvm.org/8.0.0/tools/clang/docs/ClangFormat.html) と [cpplint](https://github.com/cpplint/cpplint)
- Python: [Flake8](https://flake8.pycqa.org/en/latest/)

各 linter はビルドプロセスに組み込むことができ、コードがスタイルガイドに準拠していることを確認できます。
以下は、各言語でのコードスタイルチェック用のコマンドです：

C++ の場合、MakeFile コマンドでコードをフォーマットおよび lint します：

```bash

# format
$ make neug_clformat
```

Python の場合：

- まず依存関係をインストールします：

```bash
$ pushd tools/python_bind
$ pip3 install -r requirements_dev.txt
$ popd
```

- スタイルをチェックします：

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