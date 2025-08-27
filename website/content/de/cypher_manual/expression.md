# Expression

## Überblick

Expressions sind grundlegende Komponenten in Cypher, mit denen du Werte berechnen, Berechnungen durchführen und Daten innerhalb von Queries manipulieren kannst. Im Gegensatz zu traditionellem SQL, das sich auf relationale Datenoperationen konzentriert, sind Cypher Expressions speziell für Graphdatenstrukturen entwickelt und bieten leistungsstarke Möglichkeiten für das Traversieren von Beziehungen, Pattern Matching und graphspezifische Berechnungen.

Cypher Expressions erfüllen mehrere wichtige Funktionen:

- **Werteberechnung**: Berechnung abgeleiteter Werte aus Knoteneigenschaften, Beziehungsattributen oder berechneten Ergebnissen
- **Bedingungsauswertung**: Erstellung boolescher Ausdrücke zum Filtern von Knoten, Beziehungen oder Pfaden
- **Datenumwandlung**: Konvertierung und Formatierung von Datentypen für die Anzeige oder weitere Verarbeitung
- **Graphtraversierung**: Navigation durch Graphstrukturen mithilfe von Pfadausdrücken und Pattern Matching
- **Aggregation**: Durchführung von Berechnungen über Sammlungen von Knoten oder Beziehungen
- **String- und Textverarbeitung**: Manipulation von Textdaten für Suche, Anzeige oder Analyse

Cypher Expressions setzen sich aus zwei Hauptkategorien von Komponenten zusammen:

1. **Operatoren**: Symbole und Schlüsselwörter, die grundlegende Operationen auf Operanden ausführen
2. **Funktionen**: Vordefinierte Operationen, die komplexe Logik kapseln und Parameter entgegennehmen

Diese Komponenten arbeiten zusammen, um leistungsstarke Expressions zu erstellen, die sowohl einfache Berechnungen als auch komplexe Graphoperationen handhaben können.

## Operatoren

Operatoren in Neug sind Symbole oder Keywords, die Operationen auf Operanden ausführen. Sie sind grundlegende Bausteine für die Erstellung von Ausdrücken und Abfragen.

### Unterstützte Operatortypen

| Typ | Beschreibung |
|------|-------------|
| [Vergleich](./expression/comparison_op.md) | Operatoren zum Vergleichen von Werten (z. B. `=`, `<>`, `<`, `>`, `<=`, `>=`) |
| [Logisch](./expression/logical_op.md) | Boolesche Operatoren zum Kombinieren von Bedingungen (z. B. `AND`, `OR`, `NOT`) |
| [Arithmetisch](./expression/arithmetic_op.md) | Mathematische Operationen (z. B. `+`, `-`, `*`, `/`, `%`) |
<!-- | Bit | Bitweise Operationen (z. B. `&`, `|`, `^`, `<<`, `>>`) | -->
| [Null](./expression/null_op.md) | Operationen zur Behandlung von Nullwerten (z. B. `IS NULL`, `IS NOT NULL`) |
| [Liste](./expression/list_op.md) | Operationen für die Arbeit mit Listendatenstrukturen (z. B. `IN`) |
<!-- | Case When | Bedingte Ausdrücke mit `CASE WHEN`-Syntax | -->

## Funktionen

Funktionen in Neug sind vordefinierte Operationen, die Eingabeparameter entgegennehmen und berechnete Werte zurückgeben. Sie bieten spezialisierte Funktionalität für die Datenmanipulation und -analyse.

### Unterstützte Funktionskategorien

| Kategorie | Beschreibung |
|----------|-------------|
| [Aggregat](./expression/agg_func.md) | Funktionen, die auf Sammlungen von Werten operieren und ein einzelnes Ergebnis zurückgeben (z.B. `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`) |
| [Cast](./expression/cast_func.md) | Funktionen zum Konvertieren von Datentypen zwischen verschiedenen Formaten |
| [Temporal](./expression/temporal_func.md) | Funktionen für die Arbeit mit Datums- und Zeitdaten |
| [Graph Pattern](./expression/graph_func.md) | Funktionen, die speziell für Knoten, Beziehungen oder Pfade entwickelt wurden |
<!-- | Text | Funktionen zur String-Manipulation und Textverarbeitung |
| Numeric | Mathematische und numerische Berechnungsfunktionen | -->