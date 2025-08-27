# Daten exportieren
Der `COPY TO`-Befehl ermöglicht den direkten Export von Abfrageergebnissen in ein angegebenes Dateiformat. Dies ist nützlich, um das Ergebnis einer Abfrage dauerhaft zu speichern, um es in anderen Systemen zu verwenden oder um Abfrageausgaben für Archivierungszwecke zu sichern.

## In CSV kopieren
Die `COPY TO`-Klausel kann Abfrageergebnisse in eine CSV-Datei exportieren und wird wie folgt verwendet:
```cypher
COPY (MATCH (p:person) RETURN p.*) TO 'person.csv' (header=true);
```

Die CSV-Datei besteht aus den folgenden Feldern:
```csv
p.id|p.name|p.age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
```
Komplexe Typen wie Vertices und Edges werden im JSON-Format als Strings ausgegeben.

Verfügbare Parameter sind:
|Parameter|Beschreibung|Standardwert|
|---|---|---|
|`HEADER`|Ob eine Header-Zeile ausgegeben werden soll.|`false`|
|`DELIM` oder `DELIMITER`|Zeichen, das die Felder in der CSV trennt.|`\|`|

Ein weiteres Beispiel ist unten gezeigt.
```cypher
COPY (MATCH (:person)-[e:knows]->(:person) RETURN e) TO 'person_knows_person.csv' (header=true);
```
Dies gibt die folgenden Ergebnisse in `person_knows_person.csv` aus:
```csv
e
{'_SRC': '0:0', '_DST': '0:1', '_LABEL': 'knows', 'weight': 0.500000}
{'_SRC': '0:0', '_DST': '0:2', '_LABEL': 'knows', 'weight': 1.000000}
```