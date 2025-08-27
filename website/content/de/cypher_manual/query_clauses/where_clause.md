# WHERE-Klausel

Die WHERE-Klausel wird verwendet, um die von vorherigen Query-Operationen erzeugten Ergebnisse basierend auf Prädikaten oder Subqueries weiter zu filtern. Die Filterung basiert hauptsächlich auf logischen Ausdrücken, die wir im [Expression-Abschnitt](../expression.md) detailliert einführen werden. Sie gibt nur Daten aus, die die angegebenen Bedingungen erfüllen.

## Filtern nach Properties

Im vorherigen Kapitel haben wir eingeführt, wie man Node- und Relationship-Property-Key-Value-Paare durch Ausdrücke wie `(a:person {name: 'marko'})` einschränken kann. Hier ergänzen wir, wie man den gleichen Effekt durch die WHERE-Klausel ausdrücken kann.

### Filtern nach Knoteneigenschaften
```cypher
MATCH (a:person) 
WHERE a.name = 'marko' OR a.age > 27
RETURN a.name, a.age;
```

Ausgabe:
```
+-------------+------------+
| _0_a.name   |   _0_a.age |
+=============+============+
| marko       |         29 |
+-------------+------------+
| josh        |         32 |
+-------------+------------+
| peter       |         35 |
+-------------+------------+
```

### Filtern nach Knoten-/Beziehungseigenschaften
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name = 'marko' AND b.weight = 1.0
RETURN a.name, b.weight;
```

Ausgabe:
```
+-------------+---------------+
| _0_a.name   |   _4_b.weight |
+=============+===============+
| marko       |             1 |
+-------------+---------------+
```

### Filtern nach korrelierten Eigenschaften
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name <> c.name AND a.age > c.age 
RETURN a.name, a.age, c.name, c.age;
```

Ausgabe:
```
+-------------+------------+-------------+------------+
| _0_a.name   |   _0_a.age | _2_c.name   |   _2_c.age |
+=============+============+=============+============+
| marko       |         29 | vadas       |         27 |
+-------------+------------+-------------+------------+
```

## Filtern mit NULL

NULL-Werte sind in Graphdatenspeicher- und Berechnungsprozessen unvermeidlich. Um diese NULL-Werte zu erhalten oder zu entfernen, können wir `IS NULL` oder `IS NOT NULL` in der WHERE-Klausel verwenden.

### Filtern von Eigenschaftsdaten mit NULL
```cypher
MATCH (a) 
WHERE a.age IS NULL 
RETURN a.name;
```

```
+-------------+
| _0_a.name   |
+=============+
| lop         |
+-------------+
| ripple      |
+-------------+
```

### Optionale Daten mit NULL filtern
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NULL 
RETURN a.name;
```

<!-- todo: optional match is not included in current pip package -->

### Optionale Daten mit IS NOT NULL herausfiltern
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NOT NULL 
RETURN a.name;
```

## WHERE mit Subquery

Die WHERE-Klausel kann auch mit Subqueries verwendet werden, um komplexere Filteroperationen durchzuführen.

### Exists-Muster
```cypher
MATCH (a) 
WHERE (a)-[:knows]->(b) 
RETURN a.name;
```
Diese Query gibt alle `a.name`-Werte zurück, die eine `knows`-Beziehung haben.

### Not Exists-Muster
```cypher
MATCH (a) 
WHERE NOT (a)-[:knows]->(b) 
RETURN a.name;
```
Diese Query gibt alle `a.name`-Werte zurück, bei denen keine `knows`-Beziehungen existieren – entspricht der ANTI_JOIN-Semantik in SQL.

<!-- todo: where subquery is unsupported yet -->