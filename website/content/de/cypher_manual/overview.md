# Syntax-Übersicht

## Was ist Cypher?

Cypher ist eine deklarative Graph-Query-Language, die speziell für Graphdatenbanken entwickelt wurde. Sie bietet eine intuitive und expressive Möglichkeit, Graphdaten abzufragen, zu manipulieren und zu verwalten. Unsere Implementierung basiert auf der [OpenCypher](https://opencypher.org/)-Spezifikation, einem offenen Standard für Graph-Query-Languages.

## Wichtige Unterschiede zu SQL

Während SQL für relationale Datenbanken mit Tabellen und Zeilen konzipiert ist, ist Cypher für Graphdatenbanken mit Nodes, Relationships und Properties optimiert:

- **Struktur**: SQL verwendet Tabellen und Joins; Cypher verwendet Nodes, Relationships und Patterns
- **Pattern Matching**: SQL benötigt explizite Joins; Cypher verwendet Pattern-Matching-Syntax
- **Traversal**: SQL benötigt komplexe Joins für Multi-Hop-Queries; Cypher unterstützt Pfadtraversierung nativ
- **Lesbarkeit**: Cypher's ASCII-Art-Syntax macht Graphpatterns visuell intuitiv

## Was kannst du mit Cypher in Neug machen?

In Neug bezeichnen wir eine Cypher-Abfrage als **Statement**. Ein Statement besteht aus mehreren **Clauses**. Zum Beispiel in der folgenden Abfrage:

```cypher
MATCH (p:person)
WHERE p.age = '29'
RETURN p.name as name;
```

Die Komponenten `MATCH`, `WHERE` und `RETURN` werden Clauses genannt, welche die grundlegenden logischen Einheiten für Graphdatenbank-Operationen darstellen.

Basierend auf OpenCypher haben wir eine Reihe von Statement-Syntax-Regeln für die Verwaltung der Neug-Graphdatenbank definiert, einschließlich:

### Schema Management (DDL)

Neug zielt primär auf Schema-Strict Graphdatenszenarien ab, bei denen jedes Datenelement den vordefinierten Schema-Spezifikationen entsprechen muss. Dies ähnelt traditionellen SQL-Szenarien, allerdings beinhalten Graphdaten komplexere Knoten- und Kantenstrukturen, die ebenfalls den vordefinierten Schema-Anforderungen entsprechen müssen.

Betrachten wir beispielsweise den folgenden Schema-Graphen:

<img src="figures/modern_schema.png" alt="Modern Schema Graph" style="display: block; margin: 2em auto; max-width: 500px;">

Der obige Schema-Graph kann mit den folgenden Statements erstellt werden:

```cypher
// Beispiel Schema-Definition
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);

CREATE NODE TABLE software (
    name STRING,
    lang STRING,
    PRIMARY KEY (name)
);

CREATE REL TABLE knows (
    FROM person TO person,
    weight DOUBLE
);

CREATE REL TABLE created (
    FROM person TO software,
    weight DOUBLE
);
```

**Schema-konforme Abfrage:**
In der folgenden Abfrage entsprechen sowohl das Vertex-Label `person` als auch das Kanten-Label `(person-knows->person)` den oben definierten Schema-Beschränkungen. Der `person`-Knoten enthält die Eigenschaften `age` und `name`, wobei die Eigenschaft `age` vom Typ INT32 ist und somit mit der Konstante 18 vergleichbar ist. Daher erfüllt diese Abfrage alle Schema-Beschränkungen und ist gültig:

```cypher
MATCH (p:person)-[:knows]->(f:person)
WHERE p.age > 18
RETURN p.name, f.name;
```

**Nicht schema-konforme Abfrage (würde fehlschlagen):**
Das in dieser Abfrage angegebene Kanten-Label `(person-follows->person)` existiert nicht im Schema, wodurch die Abfrage ungültig ist und einen Fehler der Form "Table `follows` does not exist" verursacht.

```cypher
MATCH (p:person)-[:follows]->(m:person)
RETURN p.name;
```

Wir definieren eine Reihe von Syntaxelementen zum Erstellen von Schema-Graphen wie oben gezeigt, die wir DDL (Data Definition Language) nennen. Alle nachfolgenden Datenaktualisierungs- und Abfrageoperationen müssen den durch das aktuelle DDL definierten Schema-Spezifikationen entsprechen. Wir werden dies im Detail im [DDL-Abschnitt](ddl_clause.md) einführen.

### Data Query (DQL)

Wir definieren auch eine Reihe von Abfragesyntaxen, die sowohl die Anforderungen an Transactional Processing (TP) als auch Analytical Processing (AP) erfüllen können.

Beispielsweise kannst du alle Dreiecksmuster in der Graphdatenbank mit der folgenden Query abfragen:

```cypher
MATCH (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(c:person)
WHERE a.name < c.name
RETURN a.name, b.name, c.name;
```

Wir bezeichnen jedes `MATCH`, `WHERE` und `RETURN` als Clause, welche die grundlegenden Einheiten der Graphdatenoperationen darstellen. Hier matched die `MATCH`-Operation hauptsächlich alle Daten, die Dreiecksmuster bilden, `WHERE` filtert die Musterdaten weiter, um Deduplizierung zu gewährleisten, und `RETURN` führt eine Projektion der Namen durch und gibt die finalen Ergebnisse aus. Die `MATCH`-Operation vervollständigt hauptsächlich das Graphmuster-Matching, während die `WHERE`/`RETURN`-Operationen primär relationale Operationen ähnlich zu SQL durchführen. Diese Clauses werden detailliert im [DQL-Abschnitt](query_clauses.md) vorgestellt.

Um weiterhin die Gültigkeit der Clause-Operationen auf den Daten zu gewährleisten, haben wir die Datentypgrenzen definiert, die Neug unterstützt, sowie Ausdrucksoperationen basierend auf diesen Datentypen. Diese werden ausführlich in den Abschnitten [Datentypen](data_types.md) und [Ausdrücke](expression.md) behandelt.

### Datenverwaltung (DML)

Neben DQL und DDL unterstützt Neug auch Funktionen zur Datenaktualisierung, die wir als DML (Data Manipulation Language) bezeichnen. DML-Operationen können entweder durch Bulk-Import oder durch inkrementelle Updates durchgeführt werden.

**Beispiel für Bulk-Import:**
```cypher
COPY person FROM `person.csv`;
COPY knows FROM `knows.csv`;
```

Die beiden obigen Statements laden zunächst alle Knotendaten mit dem Label `person` aus der Datei person.csv und anschließend alle Kanten mit dem Label `person-[knows]->person` aus knows.csv.

**Beispiel für inkrementelle Aktualisierung:**

Wir bieten auch eine Syntax für inkrementelle Schreiboperationen an, um Graphdaten schrittweise zu aktualisieren.

**Beispiel für Knotenerstellung:**
```cypher
CREATE (p:person {name: 'Bob', age: 30});
```

**Beispiel für Kantenerstellung:**
```cypher
MATCH (a:person {name: 'Bob'}), (b:person {name: 'marko'})
CREATE (a)-[:knows {weight: 3.0}]->(b);
```

**Beispiel für Knotenlöschung:**
```cypher
MATCH (p:person {name: 'Bob'})
DELETE p;
```

Diese DML-Operationen werden im Detail im [DML-Abschnitt](dml_clause.md) erläutert.