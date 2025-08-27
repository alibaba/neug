# Guide de développement

### Compilation depuis les sources

Pour compiler NeuG depuis les sources, certaines dépendances et outils doivent être installés.

#### Compilation dans un conteneur de développement

Nous fournissons des images Docker pour les plateformes x86_64 et arm64, avec toutes les dépendances nécessaires pour compiler NeuG depuis les sources.

```bash
docker pull registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev
docker run --it --name neug-dev registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:neug-dev bash

# À l'intérieur du conteneur, configurez les variables d'environnement.
source ~/.neug_env
```

#### Compilation en local

Vous pouvez également configurer toutes les dépendances requises dans votre environnement local en utilisant le script fourni :

```bash
bash scripts/install_deps.sh --brpc --install-prefix /opt/neug
```

Une fois l'installation terminée, configurez les variables d'environnement :

```bash
source ~/.neug_env
```

### Compilation de NeuG

Avec l'environnement prêt, vous pouvez procéder à la compilation de NeuG.

#### À des fins de développement

Pour construire le package Python de NeuG en mode développement, exécutez :

```bash
make python-dev
```

Cela compilera le moteur NeuG et le client Python en mode développement. Vous pouvez utiliser le package NeuG en exécutant des scripts Python depuis le répertoire `tools/python_bind`.

```bash
cd tools/python_bind
python3
>>> import neug
```

Pour utiliser NeuG depuis d'autres répertoires, ajoutez `tools/python_bind` au `sys.path` :

```python
import sys
sys.path.append('/path/to/neug/tools/python_bind')
```

#### Construction du package Wheel

Pour compiler le package wheel, exécutez :

```bash
make python-wheel
```

Ensuite, le package wheel peut être trouvé dans `tools/python_bind/dist`.

#### Options de Build

Vous pouvez personnaliser le processus de build en spécifiant les variables d'environnement suivantes pour définir différentes options :

```bash
export BUILD_EXECUTABLES=ON/OFF # Active ou désactive la construction des exécutables utilitaires
export BUILD_HTTP_SERVER=ON/OFF # Active ou désactive le support du serveur HTTP dans NeuG
export BUILD_ALL_IN_ONE=ON/OFF # Contrôle si les bibliothèques NeuG sont construites en une seule bibliothèque dynamique ou en plusieurs bibliothèques séparées
export WITH_MIMALLOC=ON/OFF # Détermine s'il faut utiliser mimalloc au lieu du malloc par défaut de glibc
export ENABLE_BACKTRACES=ON/OFF # Lie les bibliothèques NeuG avec cpptrace pour obtenir des traces de pile détaillées en cas d'exceptions
export BUILD_TYPE=DEBUG/RELEASE # Définit le type de build CMake
export BUILD_TEST=ON/OFF # Active ou désactive la construction des suites de tests
```

#### Débogage

Par défaut, la journalisation C++ est désactivée. Pour activer la journalisation, utilisez :

```bash
export DEBUG=ON
```

Pour une journalisation plus détaillée, ajustez le niveau de verbosité de glog avec :

```bash
export GLOG_v=10 # Définir globalement
GLOG_v=10 python3 ... # Définir pour une seule commande
```

Pour investiguer plus en détail des problèmes comme les erreurs de segmentation ou d'autres complexités, l'utilisation de gdb/lldb est recommandée :

```bash
GLOG_v=10 gdb --args python3 -m pytest -sv tests/test_db_query.py
GLOG_v=10 lldb -- python3 -m pytest -sv tests/test_db_query.py
```

Pour des techniques de débogage supplémentaires, référez-vous respectivement à la documentation de gdb et lldb.

### FAQ

#### Échec de l'exécution des requêtes e2e sur macOS

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

Pour résoudre ce problème, installez le rpath manuellement :

```bash
cd tools/python_bind/
install_name_tool -add_rpath /opt/neug/lib ./build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so
```