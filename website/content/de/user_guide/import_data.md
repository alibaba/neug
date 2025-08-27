# Daten importieren

NeuG unterstützt das Einfügen von Daten in eine Datenbank aus einer angegebenen Datei. Die Voraussetzung für das Einfügen von Daten in eine Datenbank ist, dass Sie zuerst ein Graph-Schema erstellen, d.h. die Struktur Ihrer Knoten- und Beziehungstabellen.

Generell können Sie den `COPY FROM` Befehl verwenden, um Daten aus einer Datei in die Datenbank zu importieren.

## Kopieren aus CSV

### Import in Node-Tabelle

Erstelle eine Node-Tabelle `person` wie folgt:

```cypher
CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));
```

Die CSV-Datei `person.csv` enthält die folgenden Spalten:

```csv
id|name|age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
...
```

Die folgende Anweisung importiert `person.csv` in die Person-Tabelle:

```cypher
COPY User FROM "user.csv" (header=true);
```

**Hinweis:**
- Die Anzahl der Spalten in der CSV-Datei muss der Anzahl der für den Node definierten Properties entsprechen.
- Die Reihenfolge der Spalten in der CSV-Datei muss mit der Reihenfolge der für den Node definierten Properties übereinstimmen. Zum Beispiel hat der obige Node Properties in der Reihenfolge: id, age, name, was den Spalten in der CSV-Datei entspricht.

### Import in Beziehungstabelle

Erstelle eine Beziehungstabelle `knows` mit der folgenden Query:

```cypher
CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);
```

Die CSV-Datei `person_knows_person.csv` enthält die folgenden Spalten:

```csv
person.id|person.id|weight
1|2|0.5
1|4|1.0
...
```

Die folgende Anweisung importiert die Datei `person_knows_person.csv` in die Tabelle `knows`:

```cypher
COPY knows from "person_knows_person.csv" (from="person", to="person", header=true);
```

**Hinweis**
- Beim Import in eine Beziehungstabelle geht NeuG davon aus, dass die ersten beiden Spalten in der Datei die Primary Keys der `FROM`- und `TO`-Nodes sind; Die restlichen Spalten entsprechen den Beziehungseigenschaften.
- Die Parameter `FROM` und `TO` müssen in den CSV-Konfigurationen enthalten sein, um die Labels der `FROM`- und `TO`-Nodes anzugeben.

### CSV-Konfigurationen
Die folgenden Konfigurationsparameter werden unterstützt:

|Parameter|Beschreibung|Standardwert|
|---|---|---|
|`HEADER`|Gibt an, ob die erste Zeile der CSV-Datei der Header ist. Kann `true` oder `false` sein.|`false`|
|`DELIM` oder `DELIMITER`|Zeichen, das verschiedene Spalten in einer Zeile trennt.|`\|`|
|`QUOTE`|Zeichen, mit dem ein String-Literal beginnt.|`"`|
|`ESCAPE`|Zeichen innerhalb von String-Literalen, um `QUOTE` und andere Zeichen (z. B. Zeilenumbrüche) zu escapen.|`\`|
|`SKIP`|Anzahl der Zeilen, die am Anfang der Eingabedatei übersprungen werden.|`0`|
|`PARALLEL`|Gibt an, ob CSV-Dateien parallel gelesen werden.|`true`|
|`IGNORE_ERRORS`|Überspringt fehlerhafte Zeilen in CSV-Dateien, wenn auf `true` gesetzt.|`false`|
|`NULL_STRINGS`|Zeichenketten, die als `null` in der CSV-Datei behandelt werden sollen.|`""` (leerer String)|
|`FROM`|Name des Quell-Vertex-Labels, nur bei Kanten-Import verwendet|`-`|
|`TO`|Name des Ziel-Vertex-Labels, nur bei Kanten-Import verwendet|`-`|

**Hinweis**
- Parameter müssen als kommagetrennte Schlüssel-Wert-Paare angegeben werden, z. B.: `param1=value1, param2=value2, ...`
- Parameter sind nicht case-sensitiv, d. h. `Header`, `HEADER` und `header` sind alle gültig und werden korrekt erkannt.
- Für boolesche Parameter (z. B. `header`) gelten folgende Regeln:
  1. Verwende `True`, `true` oder `1`, um „aktiviert“ anzugeben.
  2. Verwende `False`, `false` oder `0`, um „deaktiviert“ anzugeben.
  3. Der Wert kann weggelassen werden, wenn er `true` ist. Beispiel: `header=true` kann verkürzt als `header` geschrieben werden.
- `FROM` und `TO` sind ein spezielles Paar von Parametern. Sie sind nur wirksam, wenn Kanten aus einer CSV-Datei importiert werden. Wenn mehrere Label-Kombinationen für Quell- und Ziel-Vertex einer Kante existieren (z. B. Comment-replyOf->Post, Comment-replyOf->Comment), müssen `FROM` und `TO` angegeben werden. Die Verwendung dieser Parameter in anderen Szenarien führt zu einer Exception.