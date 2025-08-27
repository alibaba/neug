# Neug Data Types

Dieses Dokument bietet einen umfassenden Überblick über alle von Neug unterstützten Data Types.

## Datentypen-Übersichtstabelle

Die folgende Tabelle zeigt alle von Neug unterstützten Datentypen und ihre Unterschiede zu Neo4j.

| Kategorie | Typ | Neug Beispiel | Neo4j Beispiel |
|----------|------|--------------|---------------|
| Primitive | INT32 | `Return CAST(42, 'INT32')` | `Return 42` |
| Primitive | UINT32 | `Return CAST(42, 'UINT32')` | nicht unterstützt |
| Primitive | INT64 | `Return 9223372036854775807` | `Return 9223372036854775807` |
| Primitive | UINT64 | `Return CAST(9223372036854775807, 'UINT64')` | nicht unterstützt |
| Primitive | FLOAT | `Return CAST(3.14, 'FLOAT')` | `Return 3.14f` |
| Primitive | DOUBLE | `Return 3.14159265359` | `Return 3.14159265359d` |
| Primitive | BOOL | `Return true` | `Return true` |
| Primitive | NULL | `Return null` | `Return null` |
| String | VARCHAR | `Return 'Hello World'` | `Return 'Hello World'` |
| Temporal | DATE | `Return date('2022-06-06')` | `Return date('2022-06-06')` |
| Temporal | DATETIME | `Return timestamp('2022-06-06 12:00:00')` | `Return datetime('2022-06-06T12:00:00')` |
| Temporal | INTERVAL | `Return interval('1 year 2 days')` | `Return duration('P1Y2D')` |
| Composite | LIST | `Return [1, 2, 3]` | `Return [1, 2, 3]` |
| Pattern | NODE | `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}` | `(:person {name: 'Alice', age: 30})` |
| Pattern | REL | `{_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}` | `[:knows {weight: 1.0}]` |
| Pattern | REPEATED PATH | `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}` | `(:Person {name: "Kiefer", id: 4, age: 1992})-[:FOLLOWS]->(:Person {name: "Jack", id: 3, age: 1979})-[:FOLLOWS]->(:Person {name: "Kevin", id: 5, age: 1997})` |

## Detaillierte Einführung

### Primitive Typen

#### INT32
- **Beschreibung**: 32-bit vorzeichenbehafteter Integer-Typ
- **Wertebereich**: [2,147,483,648, 2,147,483,647]
- **Query-Beispiel**: `Return CAST(42, 'INT32') as int32_value;`

#### UINT32
- **Beschreibung**: 32-bit vorzeichenloser Integer-Typ
- **Wertebereich**: [0, 4,294,967,295]
- **Query-Beispiel**: `RETURN CAST(42, 'UINT32') AS uint32_value;`

#### INT64
- **Beschreibung**: 64-bit vorzeichenbehafteter Integer-Typ, Standardtyp für Integer-Werte
- **Wertebereich**: [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]
- **Query-Beispiel**: `RETURN 9223372036854775807 AS int64_value;`

#### UINT64
- **Beschreibung**: 64-bit vorzeichenloser Integer-Typ
- **Wertebereich**: [0, 18,446,744,073,709,551,615]
- **Query-Beispiel**: `RETURN CAST(18446744073709551615, 'UINT64') AS uint64_value;`

#### FLOAT
- **Beschreibung**: Gleitkommazahl mit einfacher Genauigkeit
- **Genauigkeit**: ~7 Dezimalstellen
- **Query-Beispiel**: `RETURN CAST(3.14, 'FLOAT') AS float_value;`

#### DOUBLE
- **Beschreibung**: Double-precision Gleitkommazahl, Standardtyp für Float-Werte
- **Präzision**: ~15-17 Dezimalstellen
- **Query-Beispiel**: `RETURN 3.14159265359 AS double_value;`

#### BOOL
- **Beschreibung**: Boolean-Typ zur Darstellung von wahr oder falsch Werten
- **Werte**: `true`, `false`
- **Query-Beispiel**: `RETURN true AS bool_value;`

#### NULL
- **Beschreibung**: Repräsentiert fehlende oder undefinierte Werte
- **Query-Beispiel**: `RETURN null AS null_value;`

### String-Typen

#### VARCHAR
- **Beschreibung**: Zeichenkette variabler Länge mit UTF-8 Encoding
- **Query-Beispiel**: `RETURN 'Hello World' AS string_value;`
- **Länge**: Variabel, begrenzt durch Systemeinschränkungen, Standard ist `65536`

### Temporale Typen

#### DATE
- **Beschreibung**: Datumstyp zur Speicherung von Kalenderdaten
- **Format**: YYYY-MM-DD
- **Query-Beispiel**: `RETURN date('2022-06-06') AS date_value;`

#### DATETIME
- **Beschreibung**: Kombination aus Datums- und Zeit-Typ
- **Format**: YYYY-MM-DD HH:MM:SS
- **Query Beispiel**: `RETURN timestamp('2022-06-06 12:00:00') AS datetime_value;`

#### INTERVAL
- **Beschreibung**: Dauer oder Zeitintervall-Typ, folgt [kuzu's Spezifikation]().
- **Query Beispiel**: `RETURN interval('1 year 2 days') AS interval_value;`

### Composite Types

#### LIST
- **Beschreibung**: Geordnete Sammlung von Werten mit heterogenen Typen
- **Query-Beispiel**: `RETURN [1, 2, 3] AS list_value;`

Die folgende Tabelle zeigt alle Komponententypen, die LIST unterstützt:

| Kategorie | Typ | Beispiel |
|----------|------|---------|
| Numerisch | INT32, INT64, UINT32, UINT64, DOUBLE, FLOAT | `RETURN [1, 2, 3.0];` |
| String | VARCHAR | `RETURN ['marko', 'josh'];` |
| Datum | DATE, DATETIME | `RETURN [date('2011-01-25'), timestamp('2011-01-25 11:20:33')];` |
| BOOL | BOOL | `RETURN [true, false];` |
| Composite | LIST | `RETURN [[1, 2], [4, 5]];` |

**Wichtiger Hinweis zu LIST-Komponententypen**: 

Neug unterstützt Listen durch Tupel-Datentypen, was bedeutet, dass zusammengesetzte Typen heterogen sein können. Hier sind einige Beispiele:

Verschiedene primitive Typen in einer einzelnen Liste mischen:
```cypher
RETURN ['marko', 2];
```

Verschiedene Eigenschaftstypen von Knoten in einer Liste kombinieren:
```cypher
MATCH (n:person) RETURN [n.name, n.age];
```

Verschachtelte Listenstrukturen unterstützen:
```cypher
MATCH (n:person) RETURN [["name", n.name], ["age", n.age]];
```

**Wichtige technische Details:**
- Listen in Neug können Elemente verschiedener Datentypen enthalten (heterogene Listen)
- Dies wird durch interne Unterstützung von Tupel-Datentypen erreicht
- Typumwandlung wird automatisch durchgeführt, wenn möglich
- Verschachtelte Listen werden vollständig für komplexe Datenstrukturen unterstützt
- Das System gewährleistet Typsicherheit und gleichzeitig Flexibilität in der Listen-Zusammensetzung

### Pattern-Typen

#### NODE
- **Beschreibung**: Repräsentiert einen Knoten im Graphen
- **Interne Struktur**: Enthält `_ID` (interne ID), `_LABEL` (Knoten-Label) und Eigenschaftsfelder
- **Query-Beispiel**: `MATCH (n:person) RETURN n AS node_value;`
- **Neug-Format**: `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}`

#### REL (Relationship)
- **Beschreibung**: Repräsentiert eine Kante im Graphen
- **Interne Struktur**: Enthält `_SRC` (Quellknoten-ID und -LABEL), `_DST` (Zielknoten-ID und -LABEL), `_ID` (interne Beziehungs-ID), `_LABEL` (Beziehungstyp) und Eigenschaftsfelder
- **Query-Beispiel**: `MATCH ()-[r:knows]->() RETURN r AS rel_value;`
- **Neug-Format**: `{_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}`

#### REPEATED PATH
- **Beschreibung**: Repräsentiert einen Pfad, der aus wiederholten Kanten im Graphen besteht
- **Interne Struktur**: Enthält jeden Knoten und jede Beziehung entlang des Pfads, einschließlich Start- und Zielknoten
- **Query-Beispiel**: `MATCH (a:person)-[p*1..2]->(c) RETURN p AS path_value;`
- **Neug-Format**: `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}`