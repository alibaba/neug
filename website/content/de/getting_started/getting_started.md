# Erste Schritte mit NeuG

Dieser Leitfaden führt dich durch die Erstellung deiner ersten Graphdatenbank, grundlegende Operationen und die Erkundung beider Modi: Embedded und Service.

## Voraussetzungen

Bevor du beginnst, stelle sicher, dass NeuG installiert ist. Falls du es noch nicht installiert hast, folge bitte der [Installationsanleitung](../installation/installation.md).

## NeuG-Architektur verstehen

NeuG hat zwei verschiedene Architekturkonzepte, die unabhängig voneinander funktionieren:

### Datenbankspeichermodi
Wie deine Daten gespeichert werden:
- **In-Memory Database**: Daten werden nur im RAM gespeichert (schnellste, temporäre Speicherung)
- **Persistent Database**: Daten werden auf der Festplatte gespeichert (dauerhaft, überlebt Neustarts)

### Verbindungsmodi
Wie du auf die Datenbank zugreifst:
- **Embedded Mode**: Direkter In-Process-Zugriff (Analytics, Single-User)
- **Service Mode**: Netzwerkbasiert (Multi-User, Concurrent)

Diese Modi sind unabhängig - du kannst jede Kombination verwenden, je nach Anwendungsfall:

**Häufige Kombinationen:**
- **Persistent + Embedded**: Data Science-Analysen auf großen Datensätzen, ETL-Verarbeitung, Graph-Forschung
- **Persistent + Service**: Produktions-Webanwendungen, Multi-User-Systeme, Microservices-Architekturen
- **In-Memory + Embedded**: Unit-Testing, Prototyping, temporäre Berechnungen, Algorithmus-Entwicklung
- **In-Memory + Service**: Hochgeschwindigkeits-Caching-Layer, Session-basierte Analytics, temporäre Multi-User-Workloads

## Datenbankspeichermodi

### Persistente Datenbank
- **Use case**: Produktionsanwendungen, Datenanalyse, Langzeitspeicherung
- **Durability**: Daten bleiben bei Neustarts der Anwendung erhalten

```python

# Beispiele für den Persistent-Modus

# Stelle sicher, dass das Datenbankverzeichnis existiert und vom Benutzer beschreibbar ist.
db_persistent = neug.Database(db_path="/path/to/database")
```

### In-Memory-Datenbank
- **Anwendungsfall**: Temporäre Berechnungen, Testing, Prototyping
- **Persistenz**: Daten gehen verloren, wenn der Prozess beendet wird

```python

# Beispiele für den Memory-Modus
db_memory = neug.Database(db_path="")
```

```{note}
Derzeit erstellt NeuG im In-Memory-Modus ein temporäres Datenbankverzeichnis, das automatisch bereinigt wird, wenn der Prozess beendet wird.
```

## Verbindungsmodi

NeuG bietet zwei Modi für den Datenbankzugriff:

### Embedded Mode
Direkter In-Prozess-Zugriff – am einfachsten für Single-User-Szenarien:

```python
import neug

# Datenbank erstellen und direkt verbinden
db = neug.Database(db_path="./neug/db")  # oder db_path="" für In-Memory
conn = db.connect()

print("Connect to NeuG in embedded mode")

conn.close()
db.close()
```

### Service Mode
Netzwerkbasiert – ideal für Multi-User-Anwendungen:

**Service starten:**
```python
import neug

# NeuG als Service starten
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print("NeuG service started on localhost:10000")
```

**Vom Client verbinden:**
```python
from neug import Session

# Mit dem Service verbinden
session = Session("http://localhost:10000/")

session.close()
```

## Grundlegende Operationen

Die folgenden Operationen funktionieren unabhängig vom gewählten Datenbankmodus (in-memory oder persistent) und Verbindungsmodus (embedded oder service) auf dieselbe Weise. Für dieses Beispiel gehen wir davon aus, dass wir eine persistente Datenbank im embedded Modus verwenden.

```python
import neug

# Datenbank erstellen und Verbindung herstellen
db = neug.Database(db_path="./neug/db")
conn = db.connect()
```

### Nodes und Edges erstellen

Bevor du Daten einfügst, musst du dein Graph-Schema mit Node- und Edge-Typen definieren:

```python

# Knotentabellen erstellen
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

# Kanten-Tabellen erstellen
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

print("Graph schema erfolgreich erstellt!")
```

### Daten einfügen

Fügen wir nun einige Daten zu unserem Graphen hinzu:

```python

# Knoten einfügen
conn.execute("""
    CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})
""")

conn.execute("""
    CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})
""")

conn.execute("""
    CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})
""")

# Beziehungen einfügen
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

### Daten abfragen

Lass uns deinen Graphen mit einigen Queries erkunden:

```python

# Einfache Node-Abfrage
result = conn.execute("MATCH (p:Person) RETURN p.name, p.age")
for record in result:
    print(record)
    # ['Alice Johnson', 30]
    # ['Bob Smith', 35]

# Beziehungsabfrage
result = conn.execute("""
    MATCH (p:Person)-[w:WORKS_FOR]->(c:Company)
    RETURN p.name, w.position, c.name
""")
for record in result:
    print(f"{record[0]} works as {record[1]} at {record[2]}")
    # Alice Johnson works as Software Engineer at TechCorp

# Komplexe Pattern-Abfrage
result = conn.execute("""
    MATCH (p1:Person)-[:KNOWS]->(p2:Person)-[:WORKS_FOR]->(c:Company)
    RETURN p1.name as person1, p2.name as person2, c.name as company
""")
for record in result:
    print(f"{record[0]} knows {record[1]} who works at {record[2]}")
    # Bob Smith knows Alice Johnson who works at TechCorp
```

### Verbindung und Datenbank schließen

```python
conn.close()
db.close()
```

### Verwendung von builtin Datasets

NeuG bietet mehrere builtin Datasets, die du verwenden kannst, um schnell mit der Graph-Analyse, dem Lernen oder dem Testen zu beginnen. Diese Datasets sind sofort einsatzbereit und erfordern keine zusätzliche Einrichtung.

#### Verfügbare Datasets

Du kannst alle verfügbaren builtin Datasets auflisten:

```python
from neug.datasets import get_available_datasets

# Liste alle verfügbaren Datasets auf
datasets = get_available_datasets()
for dataset in datasets:
    print(f"{dataset.name}: {dataset.description}")
```

Derzeit verfügbare builtin Datasets sind:
- modern_graph

#### Laden von Builtin Datasets

Es gibt mehrere Möglichkeiten, mit builtin Datasets zu arbeiten:

**Methode 1: Erstelle eine neue Database aus einem Dataset**

```python
from neug import Database

# Erstelle eine Database direkt aus einem builtin Dataset
db = Database.from_builtin_dataset(dataset_name="modern_graph")
conn = db.connect()
```

```markdown
# Untersuche das geladene Dataset
result = conn.execute("MATCH (n) RETURN count(n) as node_count")
print(f"Total nodes: {result.__next__()[0]}") # sollte 6 sein
```

**Methode 2: Dataset in eine bestehende Datenbank laden**

```python
import neug

# Erstelle eine leere Datenbank
db = neug.Database("./my_analysis.db")

# Lade ein builtin Dataset hinein
db.load_builtin_dataset(dataset_name="modern_graph")
```

Beachte: Wenn du ein builtin Dataset in eine bestehende Datenbank importierst, stelle sicher, dass es keine Schema-Konflikte gibt, d.h. es dürfen keine Knoten mit den Label-Namen `person` und `software` sowie keine Kanten mit den Label-Namen `knows` und `created` existieren.

**Methode 3: Verwende die Convenience-Funktion**

```python
from neug.datasets.loader import load_dataset

# Oder lade in einen bestimmten Pfad
db = load_dataset("modern_graph", "/tmp/modern_graph.db")

conn = db.connect()

# ... arbeite mit deinem Dataset
conn.close()
db.close()
```

## Nächste Schritte

Glückwunsch! Du hast die Grundlagen von NeuG gelernt. Hier sind die nächsten Themen, die du erkunden kannst:

1. **[Datenimport/Export](import_graph.md)**: Erfahre, wie du große Datensätze importierst
2. **[Fortgeschrittene Cypher Queries](cypher_query.md)**: Meistere komplexe Graph-Muster
3. **[Datenmodellierung](data_model.md)**: Entwirf effiziente Graph-Schemas
4. **[Performance-Optimierung](../performance/index.md)**: Optimiere deine Datenbank für maximale Performance