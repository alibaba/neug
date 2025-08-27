# Guide de Style du Code

Ce document fournit les directives de style de codage pour la base de code de GraphScope, qui inclut C++, Python, Rust, Java et les scripts shell.
Suivre des standards de codage cohérents à travers la base de code favorise la maintenabilité, la lisibilité et la qualité du code.

## Style C++

Nous suivons le [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) pour les standards de codage en C++.

## Style Python

Nous suivons le style de code [black](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html) pour les standards de codage Python.

## Linter et Checker de Style

GraphScope utilise différents linters et checkers pour chaque langage afin d'appliquer les règles de style de code :

- C++ : [clang-format-8](https://releases.llvm.org/8.0.0/tools/clang/docs/ClangFormat.html) et [cpplint](https://github.com/cpplint/cpplint)
- Python : [Flake8](https://flake8.pycqa.org/en/latest/)

Chaque linter peut être intégré dans le processus de build pour s'assurer que le code respecte le style guide.
Voici les commandes pour vérifier le style de code dans chaque langage :

Pour C++, formatez et linte le code avec la commande MakeFile :

```bash

# format
$ make neug_clformat
```

Pour Python :

- Installez d'abord les dépendances :

```bash
$ pushd tools/python_bind
$ pip3 install -r requirements_dev.txt
$ popd
```

- Vérifiez le style :

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