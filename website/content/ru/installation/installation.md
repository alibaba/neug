# Установка NeuG

NeuG — это HTAP графовая база данных, предназначенная для высокопроизводительной аналитики графов и обработки транзакций в реальном времени. Поддерживает несколько платформ и языков программирования.

## Системные требования

### Поддерживаемые операционные системы
- **Linux**: Ubuntu 13.10+, Debian 8+, Fedora 19+, RHEL 7+
- **macOS**: macOS 11.0+ (Big Sur и новее)

### Поддерживаемые архитектуры
- **x86_64** (Intel/AMD 64-bit)
- **ARM64** (Apple Silicon, ARM-серверы)

### Требования к оборудованию
- **CPU**: многопроцессорная архитектура рекомендуется для достижения оптимальной производительности

## Установка Python-пакета

Python-пакет NeuG доступен на PyPI и поддерживает Python 3.8+.

```{note}
**Требования**: Python >= 3.8
```

### Создание виртуального окружения

```bash
python3 -m venv neug-env
source neug-env/bin/activate
```

### Установка с PyPI

```bash

# Установить последнюю версию
pip install neug

# Установить с указанием индекса (при необходимости)
pip install neug -i https://pypi.org/simple

# Установка из зеркала Aliyun (для пользователей из Китая)
```bash
pip install neug -i https://mirrors.aliyun.com/pypi/simple/
```

### Проверка установки

```python
import neug

# Проверить версию
print(neug.__version__)

# Создать простую in-memory базу данных
db = neug.Database(db_path="")
conn = db.connect()
print("NeuG успешно установлен!")
```

## Установка C++

### Ручная установка

На данный момент мы поддерживаем только сборку из исходного кода. Подготовку окружения для сборки смотри в разделе [Development](../development/dev_guide.md).  
Когда окружение готово, соберите и установите NeuG с помощью cmake.

```bash
git clone https://github.com/GraphScope/neug.git
cd neug
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/opt/neug # см. CMakeLists.txt для дополнительных опций cmake.
make -j$(proc)
make install
```

### Проверка установки

В вашем CMake проекте найдите и подключите библиотеки Neug с помощью следующей команды:

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

Пример файла test.cc выглядит так:

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

Соберите и запустите тест:

```bash
mkdir build && cd build
cmake .. -DCMAKE_PREFIX_PATH=/opt/neug
./test
```

## Command Line Interface (CLI)

Инструмент CLI для NeuG предоставляет интерактивную оболочку для работы с базой данных.

### Установка

```bash

# Установка через pip (включает CLI инструмент)
pip install neug

```markdown
# Или установите только CLI
pip install neug-cli
```

### Использование

```bash

# Запустить интерактивную оболочку с in-memory database
neug-cli

# Подключиться к встроенной локальной базе данных

# Будет запущен в embedded режиме.
neug-cli /path/to/database

# Подключиться к удаленному сервису базы данных

# Сначала нужно запустить сервис
neug-cli neug://localhost:8080

# Выполнить запрос напрямую
neug-cli -c "MATCH (n) RETURN count(n)" /path/to/database
```

### Проверка установки

```bash
neug-cli --version
```

## Замечания для конкретных платформ

### Linux
- Убедитесь, что у вас установлены необходимые системные библиотеки
- Для старых дистрибутивов может потребоваться установка более новой версии glibc

### macOS
- Поддерживаются как Intel, так и Apple Silicon
- Для разработки рекомендуется установка через Homebrew

## Решение проблем

### Частые проблемы

#### Проблемы с установкой Python
```bash

# Если вы столкнулись с ошибками прав доступа
pip install --user neug

```

#### Отсутствующие зависимости
```bash
```

```markdown
# Linux: Установите build essentials
sudo apt-get install build-essential

# macOS: Установите Xcode command line tools
xcode-select --install
```

#### Конфликты версий
```bash

# Проверьте установленную версию
pip show neug

# Обновите до последней версии
pip install --upgrade neug
```

### Получение помощи

Если у вас возникли проблемы во время установки:
1. Ознакомьтесь с [руководством по устранению неполадок](../troubleshooting/index.md)
2. Посетите [GitHub issues](https://github.com/graphscope/neug/issues)

## Следующие шаги

После установки NeuG вы можете:
1. Пройти [руководство по началу работы](../getting-started/index.md)
2. Изучить [режимы подключения](../connection-modes/index.md)
3. Попробовать [туториалы](../tutorials/index.md)
```