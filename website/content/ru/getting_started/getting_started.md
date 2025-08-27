# Начало работы с NeuG

Это руководство поможет вам создать первую графовую базу данных, выполнить базовые операции и изучить как embedded, так и service режимы.

## Предварительные требования

Перед началом убедитесь, что у вас установлен NeuG. Если вы еще не установили его, следуйте [руководству по установке](../installation/installation.md).

## Понимание архитектуры NeuG

NeuG имеет две отдельные архитектурные концепции, которые работают независимо:

### Режимы хранения базы данных
Как хранятся ваши данные:
- **In-Memory Database**: Данные хранятся только в RAM (самый быстрый, временный)
- **Persistent Database**: Данные хранятся на диске (долговечный, выживает после перезапусков)

### Режимы подключения
Как вы получаете доступ к базе данных:
- **Embedded Mode**: Прямой доступ из процесса (аналитика, однопользовательский режим)
- **Service Mode**: Доступ по сети (многопользовательский режим, параллельная работа)

Эти режимы независимы — вы можете использовать любую комбинацию в зависимости от вашего сценария использования:

**Частые комбинации:**
- **Persistent + Embedded**: Анализ данных в науке о данных, обработка ETL, исследование графов
- **Persistent + Service**: Продакшн веб-приложения, многопользовательские системы, архитектура микросервисов
- **In-Memory + Embedded**: Модульное тестирование, прототипирование, временные вычисления, разработка алгоритмов
- **In-Memory + Service**: Высокоскоростной кэширующий слой, аналитика на основе сессий, временные многопользовательские нагрузки

## Режимы хранения базы данных

### Постоянная база данных (Persistent Database)
- **Сценарий использования**: Продакшн-приложения, анализ данных, долгосрочное хранение
- **Долговечность**: Данные сохраняются после перезапуска приложения

```python

# Примеры использования постоянного режима

# Убедитесь, что каталог базы данных существует и доступен для записи пользователем.
db_persistent = neug.Database(db_path="/path/to/database")
```

### In-Memory Database
- **Use case**: Временные вычисления, тестирование, прототипирование
- **Durability**: Данные теряются при завершении процесса

```python

# Примеры работы в режиме памяти
db_memory = neug.Database(db_path="")
```

```{note}
В настоящее время in-memory режим NeuG создает временный каталог базы данных, который автоматически очищается при завершении процесса.
```

## Режимы подключения

NeuG предоставляет два режима доступа к вашей базе данных:

### Embedded Mode
Прямой доступ из процесса - самый простой вариант для сценариев с одним пользователем:

```python
import neug

# Создание базы данных и прямое подключение
db = neug.Database(db_path="./neug/db")  # или db_path="" для in-memory режима
conn = db.connect()

print("Подключение к NeuG в embedded режиме")

conn.close()
db.close()
```

### Service Mode
Сетевой доступ - идеален для многопользовательских приложений:

**Запуск сервиса:**
```python
import neug

# Запуск NeuG как сервиса
```python
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print("NeuG service started on localhost:10000")
```

**Подключение клиента:**
```python
from neug import Session

# Подключение к сервису
session = Session("http://localhost:10000/")

session.close()
```

## Базовые операции

Следующие операции работают одинаково независимо от выбранного режима базы данных (in-memory или persistent) и режима подключения (embedded или service). Для этого примера предположим, что мы используем persistent базу данных в embedded режиме.

```python
import neug

# Создание базы данных и установление соединения
db = neug.Database(db_path="./neug/db")
conn = db.connect()
```

### Создание узлов и ребер

Перед вставкой данных необходимо определить схему графа с типами узлов и ребер:

```python

# Создание таблиц узлов
conn.execute("""
    CREATE NODE TABLE Person(
        id INT64 PRIMARY KEY,
        name STRING,
        age INT64,
        email STRING
    )
""")

conn.execute("""
    CREATE NODE TABLE Company(
        id INT64 PRIMARY KEY,
        name STRING,
        industry STRING,
        founded_year INT64
    )
""")

# Создание таблиц ребер
conn.execute("""
    CREATE REL TABLE WORKS_FOR(
        FROM Person TO Company,
        position STRING,
        start_date DATE,
        salary DOUBLE
    )
""")

conn.execute("""
    CREATE REL TABLE KNOWS(
        FROM Person TO Person,
        since_year INT64,
        relationship_type STRING
    )
""")

print("Graph schema created successfully!")
```

### Вставка данных

Теперь давайте добавим данные в наш граф:

```python

# Вставка узлов
conn.execute("""
    CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})
""")

conn.execute("""
    CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})
""")

conn.execute("""
    CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})
""")

# Вставка связей
conn.execute("""
    MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 1
    CREATE (p)-[:WORKS_FOR {position: 'Software Engineer', start_date: date('2020-01-15'), salary: 75000.0}]->(c)
""")

conn.execute("""
    MATCH (p1:Person {id: 2}), (p2:Person {id: 1})
    CREATE (p1)-[:KNOWS {since_year: 2018, relationship_type: 'colleague'}]->(p2)
""")

print("Data inserted successfully!")
```

### Запрос данных

Давайте изучим ваш граф с помощью нескольких запросов:

```python

```python
# Простой запрос к node
result = conn.execute("MATCH (p:Person) RETURN p.name, p.age")
for record in result:
    print(record)
    # ['Alice Johnson', 30]
    # ['Bob Smith', 35]

# Запрос к relationship
result = conn.execute("""
    MATCH (p:Person)-[w:WORKS_FOR]->(c:Company)
    RETURN p.name, w.position, c.name
""")
for record in result:
    print(f"{record[0]} works as {record[1]} at {record[2]}")
    # Alice Johnson работает в должности Software Engineer в TechCorp

# Сложный запрос по паттерну
result = conn.execute("""
    MATCH (p1:Person)-[:KNOWS]->(p2:Person)-[:WORKS_FOR]->(c:Company)
    RETURN p1.name as person1, p2.name as person2, c.name as company
""")
for record in result:
    print(f"{record[0]} knows {record[1]} who works at {record[2]}")
    # Bob Smith знает Alice Johnson, которая работает в TechCorp
```

### Закрытие соединения и базы данных

```python
conn.close()
db.close()
```

### Использование встроенных наборов данных

NeuG предоставляет несколько встроенных наборов данных, которые можно использовать для быстрого начала работы с анализом графов, обучением или тестированием. Эти наборы данных готовы к использованию и не требуют настройки.

#### Доступные наборы данных

Вы можете получить список всех доступных встроенных наборов данных:

```python
from neug.datasets import get_available_datasets

# Получить список всех доступных наборов данных
datasets = get_available_datasets()
for dataset in datasets:
    print(f"{dataset.name}: {dataset.description}")
```

Текущие доступные встроенные наборы данных:
- modern_graph

#### Загрузка встроенных наборов данных

Существует несколько способов работы со встроенными наборами данных:

**Метод 1: Создание новой базы данных из набора данных**

```python
from neug import Database

# Создать базу данных напрямую из встроенного набора данных
db = Database.from_builtin_dataset(dataset_name="modern_graph")
conn = db.connect()
```

# Исследуем загруженный датасет
result = conn.execute("MATCH (n) RETURN count(n) as node_count")
print(f"Всего узлов: {result.__next__()[0]}") # должно быть 6
```

**Метод 2: Загрузка датасета в существующую базу данных**

```python
import neug

# Создаем пустую базу данных
db = neug.Database("./my_analysis.db")

# Загружаем встроенный датасет
db.load_builtin_dataset(dataset_name="modern_graph")
```

Обратите внимание, что при импорте встроенного датасета в существующую базу данных убедитесь, что нет конфликта схем, т.е. не должно быть вершин с метками `person` и `software`, не должно быть ребер с метками `knows` и `created`.

**Метод 3: Использование вспомогательной функции**

```python
from neug.datasets.loader import load_dataset

# Или загрузка в конкретный путь
db = load_dataset("modern_graph", "/tmp/modern_graph.db")

conn = db.connect()

# ... работа с вашим датасетом
conn.close()
db.close()
```

## Следующие шаги

Поздравляем! Вы изучили основы NeuG. Вот что можно изучить дальше:

1. **[Импорт/Экспорт данных](import_graph.md)**: Узнайте, как импортировать большие наборы данных
2. **[Продвинутые Cypher запросы](cypher_query.md)**: Освойте сложные графовые паттерны
3. **[Моделирование данных](data_model.md)**: Разрабатывайте эффективные графовые схемы
4. **[Оптимизация производительности](../performance/index.md)**: Настройте вашу базу данных для максимальной производительности