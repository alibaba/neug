# NeuG installieren

NeuG ist eine HTAP-Graphdatenbank, die für hochperformante Graphanalytik und Echtzeit-Transaktionsverarbeitung entwickelt wurde. Sie unterstützt mehrere Plattformen und Programmiersprachen.

## Systemanforderungen

### Unterstützte Betriebssysteme
- **Linux**: Ubuntu 13.10+, Debian 8+, Fedora 19+, RHEL 7+
- **macOS**: macOS 11.0+ (Big Sur und neuer)

### Unterstützte Architekturen
- **x86_64** (Intel/AMD 64-bit)
- **ARM64** (Apple Silicon, ARM-basierte Server)

### Hardware-Anforderungen
- **CPU**: Multicore-Prozessor für optimale Performance empfohlen

## Python-Installation

Das NeuG Python-Package ist auf PyPI verfügbar und unterstützt Python 3.8+.

```{note}
**Anforderungen**: Python >= 3.8
```

### Virtuelle Umgebung erstellen

```bash
python3 -m venv neug-env
source neug-env/bin/activate
```

### Von PyPI installieren

```bash

# Aktuellste Version installieren
pip install neug

# Installation mit spezifischem Index (falls benötigt)
pip install neug -i https://pypi.org/simple

```markdown
# Von Aliyun Mirror installieren (für chinesische Nutzer)
pip install neug -i https://mirrors.aliyun.com/pypi/simple/
```

### Installation verifizieren

```python
import neug

# Version prüfen
print(neug.__version__)

# Eine einfache In-Memory-Datenbank erstellen
db = neug.Database(db_path="")
conn = db.connect()
print("NeuG installation successful!")
```

## C++ Installation

### Manuelle Installation

Derzeit unterstützen wir nur das Bauen aus dem Quellcode. Informationen zum Vorbereiten der Build-Umgebung findest du unter [Development](../development/dev_guide.md).
Wenn die Umgebung bereit ist, kannst du NeuG mit cmake bauen und installieren.

```bash
git clone https://github.com/GraphScope/neug.git
cd neug
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/opt/neug # Weitere cmake-Optionen findest du in CMakeLists.txt.
make -j$(proc)
make install
```
```

### Installation verifizieren

In deinem CMake-Projekt kannst du die Neug-Bibliotheken mit dem folgenden Befehl finden und einbinden:

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

Ein Beispiel für `test.cc` sieht wie folgt aus:

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

Build und führe den Test aus:

```bash
mkdir build && cd build
cmake .. -DCMAKE_PREFIX_PATH=/opt/neug
./test
```

## Command Line Interface (CLI)

Das NeuG CLI-Tool bietet eine interaktive Shell für Datenbankoperationen.

### Installation

```bash

# Installation via pip (inklusive CLI-Tool)
pip install neug
```

```markdown
# Oder nur CLI installieren
pip install neug-cli
```

### Verwendung

```bash

# Interaktive Shell mit In-Memory-Datenbank starten
neug-cli

# Mit einer eingebetteten lokalen Datenbank verbinden

# Wird im Embedded-Modus ausgeführt.
neug-cli /path/to/database

# Mit einem Remote-Datenbankdienst verbinden

# Der Dienst muss zuerst gestartet werden
neug-cli neug://localhost:8080

# Eine Abfrage direkt ausführen
neug-cli -c "MATCH (n) RETURN count(n)" /path/to/database
```

### Installation überprüfen

```bash
neug-cli --version
```

## Plattformspezifische Hinweise

### Linux
- Stelle sicher, dass die erforderlichen Systembibliotheken installiert sind
- Für ältere Distributionen musst du möglicherweise eine neuere Version von glibc installieren

### macOS
- Sowohl Intel- als auch Apple Silicon werden unterstützt
- Die Installation über Homebrew wird für die Entwicklung empfohlen

## Fehlerbehebung

### Häufige Probleme

#### Python-Installationsprobleme
```bash

# Bei Berechtigungsfehlern
pip install --user neug

```

#### Fehlende Abhängigkeiten
```bash
```

```markdown
# Linux: Installiere build essentials
sudo apt-get install build-essential

# macOS: Installiere Xcode command line tools
xcode-select --install
```

#### Versionskonflikte
```bash

# Überprüfe installierte Version
pip show neug

# Upgrade auf die neueste Version
pip install --upgrade neug
```

### Hilfe erhalten

Falls du Probleme bei der Installation hast:
1. Schau dir den [Troubleshooting Guide](../troubleshooting/index.md) an
2. Besuche unsere [GitHub Issues](https://github.com/graphscope/neug/issues)

## Nächste Schritte

Sobald NeuG installiert ist, kannst du:
1. Der [Getting Started Anleitung](../getting-started/index.md) folgen
2. Die [Verbindungsmodi](../connection-modes/index.md) erkunden
3. Die [Tutorials](../tutorials/index.md) ausprobieren
```