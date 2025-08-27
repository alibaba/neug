# Match Clause

Die `MATCH`-Clause wird verwendet, um nach Mustern in der Graphdatenbank zu suchen. Sie ermöglicht es, Nodes, Relationships und Pfade zu finden, die bestimmten Kriterien entsprechen.

## Match Nodes

### Match Nodes mit einem einzelnen Label

Finde alle Nodes mit einem bestimmten Label. Diese Query gibt alle Nodes zurück, die mit `person` gelabelt sind.

```cypher
MATCH (p:person) RETURN p;
```

output:
```
+-------------------------------------------------------+
| p                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29} |
+-------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27} |
+-------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}  |
+-------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35} |
+-------------------------------------------------------+
```

### Nodes mit mehreren Labels finden

Finde Nodes mit einem der angegebenen Labels. Diese Query gibt alle Nodes zurück, die entweder mit `person` oder `software` gelabelt sind.

**Hinweis**: Im Gegensatz zu Neo4j unterstützt Neug keine Nodes mit mehreren Labels. In Neo4j steht `(p:person:software)` für Nodes, die gleichzeitig beide Labels `person` und `software` haben. In Neug steht diese Syntax für eine Vereinigung von Nodes mit entweder dem Label `person` oder `software`.

```cypher
MATCH (p:person:software) RETURN p;
```

Ausgabe:
```
+-----------------------------------------------------------------------------+
| p                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: software, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
```

### Match Nodes mit beliebigem Label

Matche Nodes ohne Angabe eines Labels. Neug unterstützt Queries ohne explizite Labels und leitet unbekannte Labels automatisch während der Kompilierung basierend auf den definierten Schema-Constraints ab.

```cypher
MATCH (p) RETURN p;
```

Ausgabe:
```
+-----------------------------------------------------------------------------+
| p                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: software, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
```

## Nodes mit Bedingungen matchen

Zusätzlich zu Label-Einschränkungen kannst du auch property-basierte Filterbedingungen angeben.

```cypher
MATCH (p:person {name: 'marko'}) RETURN p;
```

Ausgabe:
```
+-------------------------------------------------------+
| p                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29} |
+-------------------------------------------------------+
```

## Relationships matchen

### Beziehungen mit einem Label abgleichen

```cypher
MATCH (p:person)-[k:knows]->(f:person) RETURN k;
```

Ausgabe:
```
+------------------------------------------------------------------------------------------------------+
| k                                                                                                    |
+======================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5} |
+------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0} |
+------------------------------------------------------------------------------------------------------+
```

### Match Relationships mit mehreren Labels

```cypher
MATCH (p:person)-[k:knows|created]->(f) RETURN k;
```

Ausgabe:
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

### Beziehungen mit beliebigem Label matchen

```cypher
MATCH (p:person)-[k]->(f) RETURN k;
```

Ausgabe:
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

## Beziehungen mit Bedingungen matchen

Filtere Beziehungen basierend auf ihren Eigenschaften.

```cypher
MATCH (p:person)-[k:knows {weight: 1.0}]->(f:person) RETURN k;
```

Ausgabe:
```
+------------------------------------------------------------------------------------------------------+
| k                                                                                                    |
+======================================================================================================+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0} |
+------------------------------------------------------------------------------------------------------+
```

## Wiederholte Pfade matchen

Neug unterstützt die Erkundung von Pfaden variabler Länge, was eine gängige Funktion in Graph-Abfragen ist.

### Pfade mit variabler Länge matchen

Finde Pfade mit einer variablen Anzahl von hops. Diese Query gibt alle Pfade zurück, die aus 1 oder 2 Relationships bestehen.

```cypher
MATCH (p:person)-[k*1..2]->(f) RETURN k;
```

<!-- todo: output is incorrect -->

### Wiederholte Pfade mit Quellbedingungen matchen

Gib Filterbedingungen basierend auf den Eigenschaften des Quellknotens an. Diese Query findet 1-hop oder 2-hop Pfade, die vom Knoten mit dem Namen 'marko' ausgehen.

```cypher
MATCH (p:person {name: 'marko'})-[k*1..2]->(f) RETURN k;
```

### Übereinstimmung mit wiederholten Pfaden unter Verwendung von Zielbedingungen

Gib Filterbedingungen basierend auf den Eigenschaften des Zielknotens an. Diese Query findet Pfade, die am Knoten mit dem Namen 'josh' enden.

```cypher
MATCH (p:person {name: 'marko'})-[k*1..2]->(f {name: 'josh'}) RETURN k;
```

Ausgabe:
```
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                                 |
+===================================================================================================================================================+
| {_ID: 2, _LABEL: person}, {_ID: 2097152, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 2, _DST_ID: 0}, {_ID: 0, _LABEL: person} |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Pfade mit Kantenbedingungen abgleichen

Gemäß [Kuzu's Spezifikation](https://docs.kuzudb.com/cypher/query-clauses/match/#filter-recursive-relationships) unterstützt Neug auch das Filtern von Properties auf jeder Kante entlang des Pfades.

Diese Query erfordert, dass jede Kante im Pfad die Bedingung `r.weight < 1.0` erfüllt.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows*1..2 (r, _ | WHERE r.weight <= 1.0)]->(f:person)
Return k;
```

<!-- todo: output is incorrect -->

### Trail-Pfad abgleichen

Mithilfe der `TRAIL`-Option kannst du wiederholte Pfade weiter einschränken, um sicherzustellen, dass keine Kanten wiederholt werden. Dadurch ist garantiert, dass die Pfad-Erweiterungsiterationen ohne Endlosschleifen terminieren.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows* TRAIL 1..2]->(f:person)
Return k;
```

### Match Simple Path

Mit der Option `SIMPLE` kannst du wiederholte Pfade weiter einschränken, um sicherzustellen, dass keine Knoten wiederholt werden, wodurch die Ausgabe von einfachen Pfaden garantiert wird.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows* ACYCLIC 1..2]->(f:person)
Return k;
```

### Match Unweighted Shortest Path

Gib die Option `SHORTEST` an, um den ungewichteten kürzesten Pfad zwischen zwei gegebenen Knoten auszugeben.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows* SHORTEST 1..2]->(f:person {name: 'josh'})
RETURN k;
```

## Match Patterns

Die `MATCH`-Klausel unterstützt komplexe Pattern-Matching-Abfragen, die Nodes, Relationships und Bedingungen auf verschiedene Arten kombinieren, um anspruchsvolle Graph-Abfragen auszudrücken.

Im Folgenden sind einige klassische Graph-Abfrage-Muster aufgeführt, die in verschiedenen Graph-Query-Benchmarks weit verbreitet sind:

- **Triangle Pattern**
```
Match (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(c:person)
Where a.name <> b.name AND b.name <> c.name
Return count(*);
```

- **Square Pattern**
```
Match (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(d:person),
      (c:person)<-[:knows]-(d:person)
Where a.name <> b.name AND b.name <> c.name AND c.name <> d.name
Return count(*);
```

- **Long Path**
```
Match (a:person)-[:knows]->(b:person),
      (b:person)-[:knows]->(c:person),
      (c:person)-[:created]->(d:software),
      (d:software)<-[:created]-(e:person),
      (e:person)-[:knows]->(f:person)
Where a.name <> b.name AND b.name <> c.name 
    AND c.name <> d.name AND d.name <> e.name
Return count(*);
```

- **Clique Path**
```
Match (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(d:person),
      (c:person)<-[:knows]-(d:person),
      (a:person)-[:knows]->(c:person),
      (d:person)-[:created]->(b:software)
Where a.name <> b.name AND b.name <> c.name AND c.name <> d.name
Return count(*);
```

## Optional Match

Die `OPTIONAL MATCH`-Klausel ermöglicht es dir, Muster zu matchen, die im Graphen vorhanden oder auch nicht vorhanden sein können. Für Teile des Musters, die nicht matchen, wird `null` zurückgegeben.

So verwendest du Optional Match:

```cypher
MATCH (a:person)-[:knows]->(b:person)
OPTIONAL MATCH (b:person)-[:created]->(c:software)
RETURN a.name, b.name, c.name
```

<!-- todo: the feature is not included in current pip package -->

In den obigen Ausgabeergebnissen gilt für jedes (a, b)-Paar:
- Wenn b mit Knoten c verbunden ist, werden alle (a, b, c)-Tripel zurückgegeben. Zum Beispiel für ('marko', 'josh') sind die entsprechenden Tripel {('marko', 'josh', 'lop'), ('marko', 'josh', 'ripple')}.
- Wenn b nicht mit Knoten c verbunden ist, wird eine Zeile mit c=null für das aktuelle (a,b)-Paar beibehalten. Zum Beispiel für ('marko', 'vadas') ist das Ausgabetripel {('marko', 'vadas', null)}.

Das ist der Hauptzweck der `OPTIONAL MATCH`-Klausel – Zeilen aus dem Haupt-`MATCH` zu erhalten, selbst wenn das optionale Muster nicht matched.