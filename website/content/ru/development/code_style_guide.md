# Руководство по стилю кода

Этот документ содержит рекомендации по стилю кодирования для кодовой базы GraphScope, включая C++, Python, Rust, Java и shell-скрипты.
Соблюдение единых стандартов кодирования в проекте способствует повышению удобства сопровождения, читаемости и качества кода.

## Стиль C++

Мы следуем [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) для стандартов кодирования на C++.

## Стиль Python

Мы следуем стилю кода [black](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html) для стандартов кодирования на Python.

## Style Linter и Checker

GraphScope использует различные linter'ы и checker'ы для каждого языка, чтобы обеспечить соблюдение правил стиля кода:

- C++: [clang-format-8](https://releases.llvm.org/8.0.0/tools/clang/docs/ClangFormat.html) и [cpplint](https://github.com/cpplint/cpplint)
- Python: [Flake8](https://flake8.pycqa.org/en/latest/)

Каждый linter может быть включен в процесс сборки, чтобы убедиться, что код соответствует руководству по стилю.
Ниже приведены команды для проверки стиля кода на каждом языке:

Для C++ отформатируйте и проверьте код с помощью команды MakeFile:

```bash

# format
$ make neug_clformat
```

Для Python:

- Сначала установите зависимости:

```bash
$ pushd tools/python_bind
$ pip3 install -r requirements_dev.txt
$ popd
```

- Проверьте стиль:

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