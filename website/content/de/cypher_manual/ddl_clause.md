# DDL Clause

DDL (Data Definition Language) ist eine Reihe von Operationen, die speziell für das Schema-Management entwickelt wurden. Neug unterstützt Operationen zum Hinzufügen, Löschen und Ändern von Schema-Knoten, -Kanten und -Eigenschaften. Bitte beachte die folgenden Anwendungsbeispiele.

## Knotentyp erstellen

Erstelle einen Knoten mit dem Label-Typ "person" und gib die Eigenschaftsnamen, -typen und den Primärschlüssel für die Person an.

```
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

Standardmäßig wird ein Fehler gemeldet, wenn ein Person-Typ bereits in der Datenbank existiert. Verwende `IF NOT EXISTS`, um Fehler zu vermeiden – es wird nur erstellt, wenn der Typ noch nicht in der Datenbank vorhanden ist, andernfalls wird nichts unternommen.

```
CREATE NODE TABLE IF NOT EXISTS person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

## Rel-Type erstellen

Erstelle eine Kante vom Typ "knows" von Person zu Person und gib dabei die Property-Namen und -Typen für "knows" an. Aktuell unterstützen Kanten keine Primärschlüssel.

```
CREATE REL TABLE IF NOT EXISTS knows (
    FROM person TO person,
    weight DOUBLE
);
```

## Node-Type löschen

Lösche einen angegebenen Node-Typ. Verwende `IF EXISTS`, um Fehler zu vermeiden, wenn der Typ nicht existiert.

```
DROP TABLE IF EXISTS person;
```

## Rel-Type löschen

Lösche einen angegebenen Relationship-Typ. Verwende `IF EXISTS`, um Fehler zu vermeiden, wenn der Typ nicht existiert.

```
DROP TABLE IF EXISTS knows;
```

## Node- oder Rel-Type umbenennen

Benenne einen Node- oder Relationship-Typ mit `RENAME TO` um.

```
ALTER TABLE person RENAME TO person2;
ALTER TABLE knows RENAME TO knows2;
```

## Property hinzufügen

Füge Properties zu einem Node- oder Relationship-Typ hinzu.

```
ALTER TABLE person ADD IF NOT EXISTS gender INT32;
ALTER TABLE knows ADD IF NOT EXISTS info STRING;
```

## Drop Property

Entfernt Properties von einem Knoten- oder Beziehungstyp.

```
ALTER TABLE person DROP IF EXISTS gender;
ALTER TABLE knows DROP IF EXISTS info;
```

## Rename Property

Benennt Properties eines Knoten- oder Beziehungstyps um.

```
ALTER TABLE person RENAME age TO age2;
ALTER TABLE knows RENAME weight TO weight2;
```