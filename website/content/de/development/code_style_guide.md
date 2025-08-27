# Code Style Guide

Dieses Dokument enthält die Coding-Standards für die GraphScope-Codebasis, welche C++, Python, Rust, Java und Shell-Skripte umfasst.
Durch die Einhaltung konsistenter Coding-Standards wird die Wartbarkeit, Lesbarkeit und Codequalität gefördert.

## C++ Style

Wir folgen dem [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) für die C++-Entwicklung.

## Python Style

Wir folgen dem [black](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html) Code-Stil für die Python-Entwicklung.

## Style Linter und Checker

GraphScope verwendet für jede Sprache unterschiedliche Linter und Checker, um die Code-Stilregeln durchzusetzen:

- C++: [clang-format-8](https://releases.llvm.org/8.0.0/tools/clang/docs/ClangFormat.html) und [cpplint](https://github.com/cpplint/cpplint)
- Python: [Flake8](https://flake8.pycqa.org/en/latest/)

Jeder Linter kann in den Build-Prozess integriert werden, um sicherzustellen, dass der Code den Style-Guidelines entspricht.
Unten sind die Befehle zum Überprüfen des Code-Stils für jede Sprache:

Für C++ formatierst und lintest du den Code mit dem MakeFile-Befehl:

```bash

# formatieren
$ make neug_clformat
```

Für Python:

- Zuerst Abhängigkeiten installieren:

```bash
$ pushd tools/python_bind
$ pip3 install -r requirements_dev.txt
$ popd
```

- Stil überprüfen:

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