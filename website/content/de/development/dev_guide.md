# Entwicklungshandbuch

### Build aus dem Quellcode

Um NeuG aus dem Quellcode zu kompilieren, müssen bestimmte Abhängigkeiten und Tools installiert sein.

#### Build in einem Development Container

Wir stellen Docker-Images für x86_64- und arm64-Plattformen bereit, die alle notwendigen Abhängigkeiten für den Build von NeuG aus dem Quellcode enthalten.

```bash
docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev
docker run --it --name neug-dev registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev bash

# Innerhalb des Containers die Umgebungsvariablen setzen.
source ~/.neug_env
```

#### Lokaler Build

Alternativ kannst du alle benötigten Abhängigkeiten in deiner lokalen Umgebung mithilfe des bereitgestellten Skripts einrichten:

```bash
bash scripts/install_deps.sh --brpc --install-prefix /opt/neug
```

Nach Abschluss der Installation richtest du die Umgebungsvariablen ein:

```bash
source ~/.neug_env
```

### NeuG bauen

Sobald die Umgebung eingerichtet ist, kannst du mit dem Build von NeuG fortfahren.

#### Für Entwicklungszwecke

Um das NeuG Python-Package im Entwicklungsmodus zu bauen, führe aus:

```bash
make python-dev
```

Dies kompiliert die NeuG-Engine und den Python-Client im Entwicklungsmodus. Du kannst das NeuG-Package verwenden, indem du Python-Skripte aus dem Verzeichnis `tools/python_bind` ausführst.

```bash
cd tools/python_bind
python3
>>> import neug
```

Um NeuG aus anderen Verzeichnissen zu verwenden, füge `tools/python_bind` zum `sys.path` hinzu:

```python
import sys
sys.path.append('/path/to/neug/tools/python_bind')
```

#### Bauen des Wheel-Packages

Um das Wheel-Package zu kompilieren, führe aus:

```bash
make python-wheel
```

Danach findest du das Wheel-Package in `tools/python_bind/dist`.

#### Build-Optionen

Du kannst den Build-Prozess anpassen, indem du die folgenden Umgebungsvariablen setzt, um verschiedene Optionen zu konfigurieren:

```bash
export BUILD_EXECUTABLES=ON/OFF # Schaltet das Bauen von Utility-Executables ein/aus
export BUILD_HTTP_SERVER=ON/OFF # Aktiviert oder deaktiviert HTTP-Server-Support in NeuG
export BUILD_ALL_IN_ONE=ON/OFF # Legt fest, ob NeuG-Bibliotheken als eine einzelne dynamische Library oder mehrere separate Libraries gebaut werden
export WITH_MIMALLOC=ON/OFF # Entscheidet, ob mimalloc anstelle des Standard-malloc aus glibc verwendet wird
export ENABLE_BACKTRACES=ON/OFF # Linkt NeuG-Bibliotheken mit cpptrace für detaillierte Stack-Traces bei Exceptions
export BUILD_TYPE=DEBUG/RELEASE # Setzt den CMake-Build-Typ
export BUILD_TEST=ON/OFF # Schaltet das Bauen der Testsuites ein/aus
```

#### Debugging

Standardmäßig ist das C++ Logging deaktiviert. Um das Logging zu aktivieren, verwende:

```bash
export DEBUG=ON
```

Für detaillierteres Logging kannst du das glog Verbosity-Level anpassen mit:

```bash
export GLOG_v=10 # Global setzen
GLOG_v=10 python3 ... # Für einen einzelnen Befehl setzen
```

Um Probleme wie Segmentation Faults oder andere Komplexitäten weiter zu untersuchen, wird die Verwendung von gdb/lldb empfohlen:

```bash
GLOG_v=10 gdb --args python3 -m pytest -sv tests/test_db_query.py
GLOG_v=10 lldb -- python3 -m pytest -sv tests/test_db_query.py
```

Weitere Debugging-Techniken findest du in der jeweiligen Dokumentation für gdb und lldb.

### FAQ

#### Fehler beim Ausführen von e2e-Abfragen auf macOS

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

Um dieses Problem zu beheben, installiere den rpath manuell:

```bash
cd tools/python_bind/
install_name_tool -add_rpath /opt/neug/lib ./build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so
```