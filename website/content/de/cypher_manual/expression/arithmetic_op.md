# Arithmetic Operator

Neug unterstützt gängige arithmetische Operationen, darunter Addition (+), Subtraktion (-), Multiplikation (*), Division (/) und Modulo (%).

## Ergebnistyp

Die Datentypen, die an arithmetischen Operationen teilnehmen können, sind: INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE. Der Ergebnistyp einer arithmetischen Operation wird durch die Typen der Operanden bestimmt und folgt diesen Regeln:
- Wenn die Breite des linken und rechten Operanden unterschiedlich ist, ist das Ergebnis der Typ mit der größeren Breite
- Wenn die Breite der Operanden gleich ist, aber unterschiedliche Vorzeichen haben, ist das Ergebnis der nächstgrößere vorzeichenbehaftete Typ
- Der maximale Typ für Gleitkommazahlen ist DOUBLE, für Ganzzahlen ist es INT64

Die folgende Tabelle zeigt detailliert die Ergebnistypen für verschiedene Kombinationen von Operanden:

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| INT32    | INT32    | INT32  |
| INT32    | UINT32   | INT64  |
| INT32    | INT64    | INT64  |
| INT32    | UINT64   | UINT64 |
| INT32    | FLOAT    | FLOAT  |
| INT32    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| UINT32   | INT32    | INT64  |
| UINT32   | UINT32   | UINT32 |
| UINT32   | INT64    | INT64  |
| UINT32   | UINT64   | UINT64 |
| UINT32   | FLOAT    | FLOAT  |
| UINT32   | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| INT64    | INT32    | INT64  |
| INT64    | UINT32   | INT64  |
| INT64    | INT64    | INT64  |
| INT64    | UINT64   | UINT64 |
| INT64    | FLOAT    | FLOAT  |
| INT64    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| UINT64   | INT32    | UINT64 |
| UINT64   | UINT32   | UINT64 |
| UINT64   | INT64    | UINT64 |
| UINT64   | UINT64   | UINT64 |
| UINT64   | FLOAT    | FLOAT  |
| UINT64   | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| FLOAT    | INT32    | FLOAT  |
| FLOAT    | UINT32   | FLOAT  |
| FLOAT    | INT64    | FLOAT  |
| FLOAT    | UINT64   | FLOAT  |
| FLOAT    | FLOAT    | FLOAT  |
| FLOAT    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| DOUBLE   | INT32    | DOUBLE |
| DOUBLE   | UINT32   | DOUBLE |
| DOUBLE   | INT64    | DOUBLE |
| DOUBLE   | UINT64   | DOUBLE |
| DOUBLE   | FLOAT    | DOUBLE |
| DOUBLE   | DOUBLE   | DOUBLE |

## Error Handling

Neben den Ergebnistypen können arithmetische Operationen je nach Operandenwerten auch Overflow-, Underflow- oder Divide-by-Zero-Fehler verursachen. Für diese Fehler bietet Neug unterschiedliche Behandlungen je nach Datentyp und Deployment-Modus:

1. **Floating-Point-Typen**: Es erfolgt keine spezielle Fehlerbehandlung; das System verlässt sich auf das standardmäßige [IEEE 754]() Verhalten, das Infinity, -Infinity oder NaN-Werte zurückgibt.

2. **Ganzzahl-Typen (Integer types)**: Das Verhalten unterscheidet sich zwischen Debug- und Release-Modus:
   - **Debug-Modus**: Weniger strenge Leistungsanforderungen erlauben eine Eingabevalidierung zur Laufzeit mit explizitem Werfen von Exceptions
   - **Release-Modus**: Hohe Leistungsanforderungen bedeuten, dass bei Overflow/Underflow undefinierte Werte zurückgegeben werden, außer beim Teilen durch Null (Divide-by-Zero), was zu einem Core Dump führen kann


Die folgende Tabelle zeigt detailliert, welche Fehlertypen jeder Operator verursachen kann:

| Operator | Overflow | Underflow | DivideByZero | Beispiel |
|----------|----------|-----------|--------------|---------|
| +        | YES      | YES       | NO           | Return CAST(2147483647, 'int32') + CAST(1, 'int32') |
| -        | YES      | YES       | NO           | Return CAST(-2147483648, 'int32') - CAST(1, 'int32') |
| *        | YES      | YES       | NO           | Return CAST(2147483647, 'int32') * CAST(2, 'int32') |
| /        | NO       | NO        | YES          | Return 5 / 0 |
| %        | NO       | NO        | YES          | Return 5 % 0 |

## Datumsarithmetik

Neben numerischen Typen können auch auf datetime- und interval-Typen arithmetische Operationen angewendet werden. Neug unterstützt Datumsarithmetik-Operationen, mit denen du Intervalle zu Datumsangaben und Zeitstempeln addieren oder subtrahieren sowie Differenzen zwischen zeitlichen Werten berechnen kannst.

### Unterstützte Operationen

Die folgende Tabelle zeigt die unterstützten Datumsarithmetik-Operationen:

| Operation | Beschreibung | Beispiel | Ergebnis |
|-----------|-------------|---------|--------|
| DATE + INTERVAL | Intervall zu Datum addieren | `DATE('2011-02-15') + INTERVAL('5 DAYS')` | `DATE('2011-02-20')` |
| DATE - INTERVAL | Intervall von Datum subtrahieren | `DATE('2011-02-15') - INTERVAL('5 DAYS')` | `DATE('2011-02-10')` |
| TIMESTAMP + INTERVAL | Intervall zu Zeitstempel addieren | `TIMESTAMP('2011-10-21 14:25:13') + INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-22 20:25:33')` |
| TIMESTAMP - INTERVAL | Intervall von Zeitstempel subtrahieren | `TIMESTAMP('2011-10-21 14:25:13') - INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-20 08:24:53')` |
| INTERVAL + INTERVAL | Zwei Intervalle addieren | `INTERVAL('5 DAYS') + INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('6 DAYS 6 HOURS 20 SECONDS')` |
| INTERVAL - INTERVAL | Intervalle subtrahieren | `INTERVAL('5 DAYS') - INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('3 DAYS 17 HOURS 39 MINUTES 40 SECONDS')` |
| TIMESTAMP - TIMESTAMP | Zeitdifferenz berechnen | `TIMESTAMP('2011-10-21 14:25:13') - TIMESTAMP('1989-10-21 14:25:13')` | `INTERVAL('8035 DAYS')` |