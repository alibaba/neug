# Union-Klausel

Der Union-Operator in Neug wird verwendet, um die Ergebnisse mehrerer Subqueries zu einem einzigen Resultset zu kombinieren. Alle beteiligten Subqueries müssen ein konsistentes Ausgabeschema liefern – d.h. dieselbe Anzahl an Spalten mit übereinstimmenden Namen und Datentypen.

Aktuell unterstützt Neug die `UNION ALL`-Variante, welche die Ergebnisse einfach zusammenfügt, ohne eine Deduplizierung durchzuführen. Es stehen zwei Syntaxformen zur Verfügung:

- **Standard Union**: Ähnlich zur Standard-Syntax in [Kùzu](https://docs.kuzudb.com/cypher/query-clauses/union/).
- **Call Union**: Eine erweiterte Form, inspiriert von [Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union), die eine flexiblere Query-Zusammenstellung ermöglicht.

## Standard Union

In der Standardverwendung wird `UNION ALL` verwendet, um die Ausgabe mehrerer Subqueries zu mergen. Die Union muss als terminaler Operator erscheinen und die Ausgaben aller vorhergehenden Branches kombinieren.

```cypher
MATCH (n {name: 'marko'}) RETURN n.age
UNION ALL
MATCH (n {name: 'josh'}) RETURN n.age;
```

## Call Union

Inspiriert von [Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union) erweitert Neug die Union-Semantik durch einen `CALL {}`-Block mit parametrisierter Eingabe, wodurch eine ausdrucksstärkere und modularere Query-Zusammenstellung möglich wird. Dieses Konstrukt erlaubt:
- Zusätzliche Logik nach der Union auszuführen.
- Vorab berechneten Kontext (z. B. gebundene Variablen) über Union-Zweige hinweg zu teilen.

Beispiel:
```cypher
MATCH (person:person {id: 123})
WITH person
CALL (person) {
  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight > 1.0
  RETURN friend

  UNION ALL

  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight < 1.0
  RETURN friend
}
RETURN friend.id, friend.name
```

Diese Query lässt sich in drei Phasen unterteilen:
- **PreQuery**: Wird vor dem `CALL {}`-Block ausgeführt (z. B. `MATCH (person)`), um einen vorberechneten Kontext zu erzeugen, der in den Union-Subqueries geteilt wird.
- **Union Subqueries**: Werden innerhalb des `CALL {}`-Blocks definiert. Jeder Zweig hat Zugriff auf den gemeinsamen Kontext (z. B. `person`).
- **PostQuery**: Wird nach dem `CALL {}` ausgeführt und verarbeitet das vereinigte Ergebnis (z. B. `RETURN friend.id, friend.name`).

Die Syntax `CALL (person)` injiziert externe Variablen in den Union-Scope, sodass jede Subquery auf einen gemeinsamen Kontext zugreifen und damit arbeiten kann. Dieses Muster ist besonders nützlich, wenn mehrere Filter- oder Traversierungsstrategien auf dieselbe Eingabe-Entität angewandt werden sollen.