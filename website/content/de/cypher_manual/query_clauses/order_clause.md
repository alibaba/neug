# Order-Klausel

Order wird verwendet, um die aktuellen Ergebnisse basierend auf Eigenschaften zu sortieren und so eine deterministische Ausgabe zu gewährleisten. Wir unterstützen derzeit zwei Sortieroptionen: `ASC` für aufsteigende Reihenfolge und `DESC` für absteigende Reihenfolge. Wenn nicht explizit angegeben, ist die Standardreihenfolge aufsteigend. Beachte, dass Order in Kombination mit Limit verwendet werden kann, was einer TopK-Operation entspricht. Im Folgenden werden wir die gängigen Anwendungsmuster vorstellen.

## Sortieren nach einer einzelnen Eigenschaft

```
Match (a)
Return a.name
ORDER BY a.name ASC;
```

Ausgabe:
```
+-------------+
| _0_a.name   |
+=============+
| josh        |
+-------------+
| lop         |
+-------------+
| marko       |
+-------------+
| peter       |
+-------------+
| ripple      |
+-------------+
| vadas       |
+-------------+
```

## Sortierung nach mehreren Properties

```
Match (a)-[b]->(c)
Return a.name, b.weight
ORDER BY a.name ASC, b.weight ASC;
```

Ausgabe:
```
+-------------+---------------+
| _0_a.name   |   _4_b.weight |
+=============+===============+
| josh        |           0.4 |
+-------------+---------------+
| josh        |           1   |
+-------------+---------------+
| marko       |           0.4 |
+-------------+---------------+
| marko       |           0.5 |
+-------------+---------------+
| marko       |           1   |
+-------------+---------------+
| peter       |           0.2 |
+-------------+---------------+
```

## Sortierung nach Ausdrücken

Neben der direkten Sortierung nach Properties kann der ORDER BY Schlüssel auch komplexere Ausdrücke enthalten, wie z.B. Ergebnisse arithmetischer Operationen, Rückgabewerte von Skalarfunktionen, etc.

### Sortierung nach vorberechnetem Ausdruck

```
Match (a)-[b]->(c)
Return a.age, c.name
ORDER BY a.age + 10 ASC, c.name;
```

Ausgabe:
```
+------------+-------------+
|   _0_a.age | _2_c.name   |
+============+=============+
|         29 | josh        |
+------------+-------------+
|         29 | lop         |
+------------+-------------+
|         29 | vadas       |
+------------+-------------+
|         32 | lop         |
+------------+-------------+
|         32 | ripple      |
+------------+-------------+
|         35 | lop         |
+------------+-------------+
```

### Sortierung nach Ergebnissen von Skalarfunktionen

```
Match (a)-[b]->(c)
Return a.name, c.name
Order BY label(a);
```

<!-- todo: label function ist im aktuellen pip Package nicht enthalten -->

Weitere Informationen zu Skalarfunktionen findest du im [Function Section](../expression.md).

## Order with Limit

Zusätzlich unterstützt Neug in BI (Business Intelligence) Query-Szenarien auch die TopK-Operation, bei der nur die wichtigsten Ergebnisse abgeschnitten und ausgegeben werden.

```
Match (a)-[b]->(c)
Return a.age, c.name
ORDER BY a.age + 10 ASC, c.name ASC
Limit 2;
``` 

Ausgabe:
```
+------------+-------------+
|   _0_a.age | _2_c.name   |
+============+=============+
|         29 | josh        |
+------------+-------------+
|         29 | lop         |
+------------+-------------+
```