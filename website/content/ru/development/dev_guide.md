# Руководство разработчика

### Сборка из исходного кода

Для компиляции NeuG из исходного кода необходимо установить определенные зависимости и инструменты.

#### Сборка в контейнере разработки

Мы предоставляем Docker-образы для платформ x86_64 и arm64 со всеми необходимыми зависимостями для сборки NeuG из исходного кода.

```bash
docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev
docker run --it --name neug-dev registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev bash

# Внутри контейнера настройте переменные окружения.
source ~/.neug_env
```

#### Локальная сборка

Вы также можете настроить все необходимые зависимости в локальном окружении с помощью предоставленного скрипта:

```bash
bash scripts/install_deps.sh --brpc --install-prefix /opt/neug
```

После завершения установки настройте переменные окружения:

```bash
source ~/.neug_env
```

### Сборка NeuG

Когда окружение готово, вы можете приступить к сборке NeuG.

#### Для целей разработки

Чтобы собрать Python-пакет NeuG в режиме разработки, выполните:

```bash
make python-dev
```

Это приведет к компиляции движка NeuG и Python-клиента в режиме разработки. Вы можете использовать пакет NeuG, запуская Python-скрипты из директории `tools/python_bind`.

```bash
cd tools/python_bind
python3
>>> import neug
```

Чтобы использовать NeuG из других директорий, добавьте `tools/python_bind` в `sys.path`:

```python
import sys
sys.path.append('/path/to/neug/tools/python_bind')
```

#### Сборка wheel-пакета

Чтобы скомпилировать wheel-пакет, выполните:

```bash
make python-wheel
```

После этого wheel-пакет можно найти в `tools/python_bind/dist`.

#### Параметры сборки

Вы можете настроить процесс сборки, указав следующие переменные окружения для установки различных опций:

```bash
export BUILD_EXECUTABLES=ON/OFF # Включить или отключить сборку исполняемых утилит
export BUILD_HTTP_SERVER=ON/OFF # Включить или отключить поддержку HTTP сервера в NeuG
export BUILD_ALL_IN_ONE=ON/OFF # Определяет, будут ли библиотеки NeuG собраны в одну динамическую библиотеку или несколько отдельных библиотек
export WITH_MIMALLOC=ON/OFF # Выбрать, использовать ли mimalloc вместо стандартного malloc из glibc
export ENABLE_BACKTRACES=ON/OFF # Линковать библиотеки NeuG с cpptrace для получения детального стек-трейса при исключениях
export BUILD_TYPE=DEBUG/RELEASE # Установить тип сборки CMake
export BUILD_TEST=ON/OFF # Включить или отключить сборку тестовых наборов
```

#### Отладка

По умолчанию логирование C++ отключено. Чтобы включить логирование, используйте:

```bash
export DEBUG=ON
```

Для более подробного логирования настройте уровень детализации glog с помощью:

```bash
export GLOG_v=10 # Установить глобально
GLOG_v=10 python3 ... # Установить для одной команды
```

Для более глубокого анализа проблем, таких как segmentation faults или других сложностей, рекомендуется использовать gdb/lldb:

```bash
GLOG_v=10 gdb --args python3 -m pytest -sv tests/test_db_query.py
GLOG_v=10 lldb -- python3 -m pytest -sv tests/test_db_query.py
```

Дополнительные методы отладки описаны в документации gdb и lldb соответственно.

### FAQ

#### Не удалось запустить e2e-запросы на macOS

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

Чтобы исправить эту проблему, вручную добавьте rpath:

```bash
cd tools/python_bind/
install_name_tool -add_rpath /opt/neug/lib ./build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so
```