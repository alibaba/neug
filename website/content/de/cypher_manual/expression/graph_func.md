# Graph Pattern Function

Neben den verschiedenen zuvor eingeführten funktionsbasierten Operationen auf relationalen Daten unterstützt Neug auch speziell eine Reihe von Funktionen zum Arbeiten mit Knoten-, Kanten- und Pfaddaten, die durch Graph Patterns generiert werden.

## Node Function

Funktion | Beschreibung | Beispiel
---------|-------------|--------
ID() | Gibt die interne ID eines Knotens/einer Kante zurück | Return (a) Return ID(a)
LABEL() | Gibt das Label eines Knotens/einer Kante zurück | Match (a) Return LABEL(a)

## Rel Function

Zusätzlich zu den ID- und LABEL-Funktionen gibt es die folgenden kantenbasierten Funktionsoperationen:

Funktion | Beschreibung | Beispiel
---------|-------------|--------
START_NODE() | Gibt den Startknoten der Kantendaten zurück | Match ()-[b]->() Return START_NODE(b);
END_NODE() | Gibt den Endknoten der Kantendaten zurück | Match ()-[b]->() Return END_NODE(b);

## Repeated Path Function

Funktion | Beschreibung | Beispiel
---------|-------------|--------
NODES | Gibt alle Knoten aus einer rekursiven Beziehung zurück | Match (a)-[b*2..3]->() Return NODES(b);
RELS | Gibt alle Beziehungen aus einer rekursiven Beziehung zurück | Match (a)-[b*2..3]->() Return RELS(b);
PROPERTIES | Gibt die angegebene Eigenschaft von Knoten/Beziehungen zurück | Match (a)-[b*2..3]->() Return PROPERTIES(nodes(b), 'name'), PROPERTIES(rels(b), 'weight');
IS_TRAIL | Prüft, ob der Pfad wiederholte Beziehungen enthält | Match (a)-[b*2..3]->() Return IS_TRAIL(b);
IS_ACYCLIC | Prüft, ob der Pfad wiederholte Knoten enthält | Match (a)-[b*2..3]->() Return IS_ACYCLIC(b);
LENGTH | Gibt die Anzahl der Beziehungen (Pfadlänge) in einer rekursiven Beziehung zurück | Match (a)-[b*2..3]->() Return LENGTH(b);