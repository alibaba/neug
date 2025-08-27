# With-Klausel

WITH wird hauptsächlich für die weitere Projektion oder Aggregation der aktuellen Daten verwendet. Im Folgenden werden wir die entsprechenden Anwendungsfälle aus diesen beiden Aspekten vorstellen.

## Aggregation

Aggregation ist vergleichbar mit der GROUP BY-Operation in SQL, bei der die aktuellen Daten nach Eigenschaften gruppiert und entsprechende Operationen mithilfe von Aggregatfunktionen auf die Daten jeder Gruppe angewendet werden. Neug unterstützt derzeit die gängigen Aggregatfunktionen, darunter:
- COUNT
- COUNT_STAR
- COLLECT
- SUM
- MIN
- MAX
- AVG

Diese Funktionen werden im Detail im Abschnitt [Aggregatfunktionen](../expression/agg_func.md) erläutert.

### Aggregation nach einer einzelnen Eigenschaft
```
Match (a) With label(a) as label, count(a.name) as cnt Return label, cnt;
```

<!-- todo: label ist im aktuellen Paket nicht enthalten -->

### Aggregation nach mehreren Eigenschaften
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt Return a_label, c_label, cnt;
```

### Mehrere Aggregatfunktionen anwenden
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt, sum(b.weight) as weights Return a_label, c_label, cnt, weights;
```

### Filtern basierend auf Aggregations-Ergebnissen

```
Match (a:person) With label(a) as label, count(a.name) as cnt Where cnt > 2 Return label, cnt;
```

<!-- todo: label ist null in der Ausgabe -->

## Projektion

Eine weitere häufige Verwendung von WITH besteht darin, die aktuellen Ergebnisse weiter nach Spalten zu projizieren, was dem Column Pruning in SQL entspricht, wobei nur die Spalten ausgegeben werden, die für nachfolgende Abfragen benötigt werden.

## Vertex-Daten projizieren

```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With b
Match (b)-[:created]->(c)
Return c.name;
```

<!-- todo: multiple match ist im aktuellen Paket nicht enthalten -->

## Vertex-/Edge-Daten projizieren

```
Match (a:person {name: 'marko'})-[k:knows]->(b:person)
With b, k
Match (b)-[:created]->(c)
Return k.weight, c.name;
```

## Projekteigenschaften
```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With a, b.age as b_age
Match (a)-[:created]->(c)
Where b_age > 20
Return c.name;
```

Projiziere die Eigenschaften der Daten b, die durch das erste Match generiert wurden, filtere die Eigenschaften durch Filter und gib schließlich alle c.name aus, die die Bedingungen erfüllen;