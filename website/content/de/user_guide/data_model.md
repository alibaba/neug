# Datenmodell

## Datentypen

`Neug` bietet robuste Unterstützung für eine breite Palette von Datentypen, die sowohl in Vertex- als auch in Edge-Eigenschaften verwendet werden. Diese Datentypen sind in fünf verschiedene Kategorien unterteilt:

- **Primitive Typen:** Dazu gehören grundlegende Datentypen wie Integer, Floats und Booleans, die für die Darstellung von einfachen numerischen und logischen Werten unerlässlich sind.

- **String-Typen:** Diese Kategorie umfasst Typen zur Verwaltung von Textdaten und ermöglicht die effiziente Speicherung und Manipulation von Strings innerhalb von Vertices und Edges.

- **Temporal-Typen:** Diese Typen sind für die Handhabung von Datums- und Zeitinformationen konzipiert und unterstützen verschiedene Zeitdarstellungen, um zeitbasierte Analysen und Abfragen zu ermöglichen.

- **Array-Typen:** Array-Typen erlauben die Speicherung von Elementsammlungen und ermöglichen so die Darstellung von Listen oder Mengen von Werten, die einem Graph-Element zugeordnet sind.

- **Komplexe Typen:** (Derzeit nicht unterstützt) Diese können benutzerdefinierte Objekte oder zusammengesetzte Strukturen enthalten, die mehrere primitive Typen kombinieren, um anspruchsvollere Datenmodellierungen und Beziehungen innerhalb des Graphen zu ermöglichen.

### Primitive Types

Natürlich! Hier ist eine Markdown-Tabelle mit dem angegebenen Header und dem ersten Eintrag:

| Type   | Size    | Description                    |
|--------|---------|--------------------------------|
| BOOL   | 1 bit   | One-bit boolean                |
| INT8   | 1 byte  | One-byte integer               |
| UINT8  | 1 byte  | Unsigned one-byte integer      |
| INT16  | 2 bytes | Two-byte integer               |
| UINT16 | 2 bytes | Unsigned two-byte integer      |
| INT32  | 4 bytes | Four-byte integer              |
| UINT32 | 4 bytes | Unsigned four-byte integer     |
| INT64  | 8 bytes | Eight-byte integer             |
| UINT64 | 8 bytes | Unsigned eight-byte integer    |
| FLOAT  | 4 bytes | Single-precision float         |
| DOUBLE | 8 bytes | Double-precision float         |

### String Types

| Type   | Size    | Description                    |
|--------|---------|--------------------------------|
| STRING | -       | Variable length string         |

### Temporale Typen

#### DATE

`DATE` bezeichnet einen Kalendertag, der durch die Komponenten Jahr, Monat und Tag gekennzeichnet ist und im ISO-8601-Standard formatiert ist (YYYY-MM-DD). Es benötigt 4 Bytes Speicherplatz.

```cypher
RETURN date('2022-06-06') as x;
```

#### DATE32

`DATE32` repräsentiert einen Kalendertag durch Zählen der Anzahl von Tagen seit dem 1. Januar 1970. Tage vor diesem Datum werden durch negative Werte angegeben. Es benötigt 4 Bytes Speicherplatz. (TODO: (xiaoli): sicherstellen, dass es funktioniert)

```cypher
RETURN date32('2022-06-06') as x;
```

#### TIMESTAMP

`TIMESTAMP` kombiniert Datum und Zeitkomponenten – Stunde, Minute, Sekunde und Millisekunde – im ISO-8601-Standardformat (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]]). Dieses Format gibt das Datum (YYYY-MM-DD), die Zeit (hh:mm:ss[.zzzzzz]) und optional den Zeitversatz [+-TT[:tt]] an. Der Datumsteil ist obligatorisch, während die Zeitkomponente optional ist und bei Bedarf Millisekunden [.zzzzzz] sowie einen Zeitversatz enthalten kann. In der Speicherung belegt es 8 Bytes. Beispiel: "1970-01-01 00:00:00.004666-10"

```cypher
RETURN timestamp("1970-01-01 00:00:00.004666-10") as x;
```

#### INTERVAL

`INTERVAL` repräsentiert einen Zeitraum, der durch die Komponenten Jahr, Monat, Tag, Stunde, Minute und Mikrosekunde charakterisiert werden kann. Wir folgen der Definition für Interval in [kuzu](https://docs.kuzudb.com/cypher/data-types/#interval).

```cypher
RETURN interval("1 year 2 days") as x;
```

### Array-Typen

Array-Typen werden derzeit nicht unterstützt, aber in naher Zukunft geplant.
Einmal unterstützt, müssen allerdings alle Elemente innerhalb des Arrays einem der zuvor erwähnten primitiven Typen entsprechen.

### Komplexe Typen

Komplexe Typen werden derzeit nicht unterstützt, einschließlich `STRUCT`, `MAP`, `UNION`, `BLOB`.

## Typumwandlung

Wir unterstützen [Typumwandlungen zwischen numerischen Typen](https://github.com/GraphScope/neug/issues/416).

- INT32
- UINT32
- INT64
- UINT64
- FLOAT
- DOUBLE

**Alle Kombinationen dieser Typen dürfen untereinander konvertiert werden**. Während der Typumwandlung führt der Compiler Überlaufprüfungen durch, um sicherzustellen, dass die Konvertierungen sicher sind. Die folgende Tabelle fasst die potenziellen Überlauf-Szenarien zusammen, die bei diesen Konvertierungen behandelt werden:


| Von \ Nach | INT32  | UINT32 | INT64  | UINT64 | FLOAT  | DOUBLE |
|--------|--------|--------|--------|--------|--------|--------|
| **INT32**  | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT32** | ⚠️ May Overflow (if value > INT32_MAX)         | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe |
| **INT64**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT64** | ⚠️ May Overflow (if value > INT32_MAX)         | ⚠️ May Overflow (if value > UINT32_MAX)       | ⚠️ May Overflow (if value > INT64_MAX) | ✅ Safe | ✅ Safe | ✅ Safe |
| **FLOAT**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ✅ Safe | ✅ Safe |
| **DOUBLE** | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ⚠️ May Overflow (if value < -FLT_MAX or > FLT_MAX) | ✅ Safe |