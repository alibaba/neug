# Temporal Functions

Temporal Functions sind eine spezialisierte Gruppe von Funktionen, die für Datums- und Intervall-Datentypen entwickelt wurden. Sie bieten Möglichkeiten, Datums-/Intervall-Typen aus String-Literalen zu konstruieren und spezifische Felder aus temporalen Werten zu extrahieren. Die folgende Tabelle listet die verfügbaren Funktionen und deren Verwendung auf.

## Funktionsreferenz

| Funktion | Beschreibung | Beispiel | Rückgabetyp |
|----------|-------------|---------|-------------|
| `DATE()` | Erstellt einen Datumswert aus einem String-Literal | `DATE('2012-01-01')` | `DATE` |
| `TIMESTAMP()` | Erstellt einen Timestamp-Wert aus einem String-Literal | `TIMESTAMP('1926-11-21 13:22:19')` | `TIMESTAMP` |
| `INTERVAL()` | Erstellt einen Intervallwert aus einem String-Literal | `INTERVAL('3 DAYS')` | `INTERVAL` |
| `date_part(part, date)` | Extrahiert einen bestimmten Teil aus einem Datumswert | `date_part('year', DATE('1995-11-02'))` | `INTEGER` |
| `date_part(part, timestamp)` | Extrahiert einen bestimmten Teil aus einem Timestamp-Wert | `date_part('month', TIMESTAMP('1926-11-21 13:22:19'))` | `INTEGER` |
| `date_part(part, interval)` | Extrahiert einen bestimmten Teil aus einem Intervallwert | `date_part('days', INTERVAL('1 days'))` | `INTEGER` |