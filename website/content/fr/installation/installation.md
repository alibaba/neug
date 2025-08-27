# Installer NeuG

NeuG est une base de données graphe HTAP conçue pour l'analyse de graphes haute performance et le traitement transactionnel en temps réel. Elle prend en charge plusieurs plateformes et langages de programmation.

## Configuration requise

### Systèmes d'exploitation supportés
- **Linux** : Ubuntu 13.10+, Debian 8+, Fedora 19+, RHEL 7+
- **macOS** : macOS 11.0+ (Big Sur et versions ultérieures)

### Architectures supportées
- **x86_64** (Intel/AMD 64-bit)
- **ARM64** (Apple Silicon, serveurs ARM)

### Configuration matérielle requise
- **CPU** : Processeur multi-cœur recommandé pour des performances optimales

## Installation Python

Le package Python de NeuG est disponible sur PyPI et supporte Python 3.8+.

```{note}
**Requirements**: Python >= 3.8
```

### Créer un environnement virtuel

```bash
python3 -m venv neug-env
source neug-env/bin/activate
```

### Installer depuis PyPI

```bash

# Installer la dernière version
pip install neug

# Installer avec un index spécifique (si nécessaire)
pip install neug -i https://pypi.org/simple

# Installation depuis le miroir Aliyun (utilisateurs en Chine)
```bash
pip install neug -i https://mirrors.aliyun.com/pypi/simple/
```

### Vérification de l'installation

```python
import neug

# Vérifier la version
print(neug.__version__)

# Créer une base de données en mémoire
db = neug.Database(db_path="")
conn = db.connect()
print("Installation de NeuG réussie !")
```

## Installation C++

### Installation manuelle

Actuellement, nous ne supportons que la compilation depuis les sources. Référez-vous à [Développement](../development/dev_guide.md) pour préparer l'environnement de build.
Une fois l'environnement prêt, compilez et installez NeuG en utilisant cmake.

```bash
git clone https://github.com/GraphScope/neug.git
cd neug
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/opt/neug # voir CMakeLists.txt pour plus d'options cmake.
make -j$(proc)
make install
```

### Vérifier l'installation

Dans votre projet CMake, trouvez et liez les bibliothèques Neug avec la commande suivante :

```cmake
cmake_minimum_required (VERSION 3.10)
project (
  NeugTest
  VERSION 0.1
  LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)


find_package(neug REQUIRED)
add_executable(test test.cc)
include_directories(${NEUG_INCLUDE_DIRS})
target_link_libraries(test ${NEUG_LIBRARIES})
```

Un exemple de fichier test.cc ressemble à :

```cpp
#include <neug/main/neug_db.h>
#include <iostream>

int main() {
  gs::NeugDB db("test_db");
  auto conn = db.connect();
  std::cout << "NeuG C++ client installation successful!" << std::endl;
  return 0;
}
```

Compilez et exécutez le test :

```bash
mkdir build && cd build
cmake .. -DCMAKE_PREFIX_PATH=/opt/neug
./test
```

## Interface en ligne de commande (CLI)

L'outil CLI NeuG fournit un shell interactif pour les opérations sur la base de données.

### Installation

```bash

# Installer via pip (inclut l'outil CLI)
pip install neug

```markdown
# Ou installer uniquement le CLI
pip install neug-cli
```

### Utilisation

```bash

# Démarrer le shell interactif avec une base de données en mémoire
neug-cli

# Se connecter à une base de données locale embarquée

# S'exécutera en mode embarqué.
neug-cli /path/to/database

# Se connecter à un service de base de données distant

# Vous devez d'abord démarrer le service
neug-cli neug://localhost:8080

# Exécuter une requête directement
neug-cli -c "MATCH (n) RETURN count(n)" /path/to/database
```

### Vérifier l'installation

```bash
neug-cli --version
```

## Notes spécifiques aux plateformes

### Linux
- Assurez-vous d'avoir les bibliothèques système requises installées
- Pour les distributions plus anciennes, vous devrez peut-être installer une version plus récente de glibc

### macOS
- Les architectures Intel et Apple Silicon sont toutes deux supportées
- L'installation via Homebrew est recommandée pour le développement

## Dépannage

### Problèmes courants

#### Problèmes d'installation de Python
```bash

# Si vous rencontrez des erreurs de permission
pip install --user neug

```

#### Dépendances manquantes
```bash
```

```bash
# Linux : Installer les outils de build essentiels
sudo apt-get install build-essential

# macOS : Installer les outils de ligne de commande Xcode
xcode-select --install
```

#### Conflits de Versions
```bash
# Vérifier la version installée
pip show neug

# Mettre à jour vers la dernière version
pip install --upgrade neug
```

### Obtenir de l'Aide

Si vous rencontrez des problèmes pendant l'installation :
1. Consultez le [guide de dépannage](../troubleshooting/index.md)
2. Visitez notre [section issues sur GitHub](https://github.com/graphscope/neug/issues)

## Étapes Suivantes

Une fois NeuG installé, vous pouvez :
1. Suivre le [guide de démarrage](../getting-started/index.md)
2. Explorer les [modes de connexion](../connection-modes/index.md)
3. Essayer les [tutoriels](../tutorials/index.md)