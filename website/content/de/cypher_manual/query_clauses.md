# Query Clauses
In diesem Kapitel stellen wir hauptsächlich die Neug query-bezogenen Clauses Operationen vor. Die folgende Tabelle fasst die Typen und allgemeinen Zwecke dieser Operationen zusammen:

Clause | Beschreibung
-------|------------
[MATCH](query_clauses/match_clause.md) | Finde Graph Pattern
[WHERE](query_clauses/where_clause.md) | Filtere basierend auf Eigenschaften
[WITH](query_clauses/with_clause.md) | Projektion oder Aggregation basierend auf Eigenschaften
[RETURN](query_clauses/return_clause.md) | Ausgabe von Projektion oder Aggregations-Ergebnissen
[ORDER](query_clauses/order_clause.md) | Weitere Sortierung von Zwischen- oder Ausgabe-Ergebnissen
[SKIP](query_clauses/limit_clause.md) | Überspringe den oberen Teil der Ergebnisse, bestimme die untere Grenze der Ausgabe-Ergebnisse
[LIMIT](query_clauses/limit_clause.md) | Kürze Ergebnisse ab, bestimme die obere Grenze der Ausgabe-Ergebnisse
[UNION](query_clauses/union_clause.md) | Führe mehrere Branch-Ergebnisse mit konsistentem Schema zusammen
<!-- [UNWIND](query_clauses/unwind_clause.md) | Entpacke ein List Result -->

Wir werden den [modern graph](https://tinkerpop.apache.org/docs/current/reference/#graph-computing) als Beispiel verwenden, um einzuführen, was jede Clause spezifisch bewirkt. 
<!-- Unten ist das Schema- und Datendiagramm, das dem modern graph entspricht.  -->