# Return Clause

Return und With bieten ähnliche Funktionalität, beide für weitere Aggregation oder Projektion von Daten. Der Unterschied ist, dass Return die verarbeiteten Ergebnisse zurückgeben und anzeigen muss. Hier gehen wir nicht zu sehr ins Detail über die Funktionalität von Return selbst, da du dich auf die Verwendung im Abschnitt [With Clause](with_clause.md) beziehen kannst. Wir konzentrieren uns hauptsächlich auf die Ergebnisausgabe und einige gängige Return-Verwendungsmuster.

## Return Nodes

### Knoten mit einem einzelnen Label zurückgeben
```
Match (a:person) Return a;
```

Die Ausgabe zeigt die interne ID jedes Person-Knotens (zugewiesen von der Graphdatenbank), das Label und alle Eigenschaften:
```
+-------------------------------------------------------+
| a                                                     |
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

### Knoten mit mehreren Labels zurückgeben
```
Match (a) Return a;
```

Die Ausgabe zeigt die interne ID, das Label und alle Properties jedes Knotens in seinem eigenen Knotentyp:
```
+-----------------------------------------------------------------------------+
| a                                                                           |
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

## Rückgabe von Relationships

```
Match (a:person)-[k]->(b)
Return k;
```

Die Ausgabe enthält die interne ID der Relationship, das Label, alle Properties sowie die Labels und IDs der Quell- und Zielknoten:

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

## Return Paths

### Return Repeated Paths

```
Match (a:person)-[k*1..2]->(c)
Return k;
```

<!-- todo: add output here. -->

### Return All Nodes/Rels in Paths

```
Match (a:person)-[k*1..2]->(c)
Return nodes(k) as nodes, rels(k) as rels;
```

<!-- todo: nodes or rels are unsupported yet -->

### Return Properties of Node/Rels in Paths
```
Match (a:person)-[k*1..2]->(c)
Return properties(nodes(k), 'name') as names, properties(rels(k), 'weight') as weights;
```

<!-- todo: properties is unsupported yet -->

## Return with TopK

Kombination aus Return, OrderBy und Limit zur Ausgabe der TopK-Query-Ergebnisse
```
Match (a:person)-[:knows]->(b:person)
Return a.name, b.name
Order By a.name ASC, b.name ASC
Limit 2;
```

Ausgabe:
```
+-------------+-------------+
| _0_a.name   | _2_b.name   |
+=============+=============+
| marko       | josh        |
+-------------+-------------+
| marko       | vadas       |
+-------------+-------------+
```

## Return mit Aggregation
Ausgabe von Aggregations-Ergebnissen
```
Match (a:person)-[:knows]->(b:person)
Return label(a) as a_label, label(b) as b_label, count(*) as cnt;
```

<!-- todo: output is incorrect -->

## Return mit Distinct
Ausgabe von nicht-duplizierten Ergebnissen
```
Match (a)
Return distinct label(a);
```

<!-- todo: label is not included in current pip package -->