# DML Clause

DML (Data Manipulation Language) bietet Operationen für das Einfügen, Löschen und Ändern von Daten in Graphdatenbanken. Neug unterstützt sowohl Massendatenoperationen (wie COPY FROM) als auch einzelne Datenoperationen (wie CREATE, SET und DELETE). Dieses Dokument enthält Beispiele und Erklärungen für jeden Operationstyp.

## COPY FROM

Der COPY FROM Befehl ermöglicht es dir, Daten aus externen Datenquellen zu laden und Vertices sowie Edges im Graphspeicher zu erstellen. Derzeit unterstützte Datenquellen umfassen CSV-Formate.

### Laden von Vertex-Daten

Lade Person-Vertex-Daten aus einer CSV-Datei. Jede Zeile in der CSV entspricht einem Vertex, wobei die Spalten den im Person-Schema definierten Vertex-Eigenschaften entsprechen.

**person.csv:**
```
name,age
marko,39
vadas,27
josh,32
peter,35
```

**Befehl:**
```cypher
COPY person FROM "person.csv"
```

### Laden von Edge-Daten

Load liest Edge-Daten aus einer CSV-Datei. Die ersten beiden Spalten geben die Primary Keys der Source- und Target-Vertex an, während zusätzliche Spalten Edge-Eigenschaften definieren.

**knows.csv:**
```
src_name,dst_name,weight
marko,josh,1.0
marko,vadas,0.5
josh,peter,0.8
```

**Command:**
```cypher
COPY knows FROM "knows.csv"
```

## CREATE

Die CREATE-Klausel wird verwendet, um neue Vertices und Relationships in den Graphen einzufügen.

### Erstellen von Vertices

Erstelle neue Vertices mit spezifizierten Eigenschaften. Wenn bereits ein Vertex mit demselben Primary Key existiert, wird ein Fehler gemeldet.

```cypher
CREATE (a:person {name: 'taylor', age: 25}), (b:person {name: 'julie', age: 30})
```

### Erstellen von Vertices und Relationships

Erstelle Vertices und Relationships in einem einzigen Statement. Dies ist nützlich, wenn du sowohl die Vertices als auch die Relationship zwischen ihnen erstellen musst.

```cypher
CREATE (a:person {name: 'mars', age: 28})-[:knows {weight: 16.0}]->(b:person {name: 'jennie', age: 26})
```

### Beziehungen zwischen existierenden Vertices erstellen

Zuerst existierende Vertices matchen, dann eine Beziehung zwischen ihnen erstellen.

```cypher
MATCH (a:person {name: 'taylor'}), (b:person {name: 'julie'})
CREATE (a)-[:knows {weight: 20.0}]->(b)
```

## SET

Die SET-Klausel wird verwendet, um Properties von existierenden Vertices und Relationships zu aktualisieren.

### Vertex-Properties aktualisieren

Properties eines spezifischen Vertex aktualisieren.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
SET a.age = 37, a.city = 'New York'
RETURN a.*
```

### Relationship-Properties aktualisieren

Properties einer spezifischen Relationship aktualisieren.

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
SET k.weight = 10.0, k.since = '2023-01-01'
RETURN k.*
```

## DELETE

Die DELETE-Klausel wird verwendet, um Vertices und Relationships aus dem Graphen zu entfernen.

### Löschen von Vertices

Lösche einen Vertex aus dem Graphen. Standardmäßig kannst du nur Vertices löschen, die keine Relationships haben, um das Erstellen von "dangling edges" zu vermeiden.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DELETE a
```

### Löschen von Vertices mit Relationships (DETACH DELETE)

Verwende DETACH DELETE, um einen Vertex und alle seine Relationships zwangsweise zu löschen. Dies verhindert Fehler beim Löschen von Vertices, die bereits bestehende Relationships haben.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DETACH DELETE a
```

### Löschen von Relationships

Lösche spezifische Relationships zwischen Vertices, während die Vertices selbst erhalten bleiben.

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
DELETE k
```