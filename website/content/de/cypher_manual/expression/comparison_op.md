# Vergleichsoperatoren

Wir demonstrieren die verschiedenen Vergleichsoperatoren, die von Neug unterstützt werden, in der folgenden Tabelle.

Operator | Beschreibung | Beispiel | Ergebnis
---------|-------------|---------|--------
`=` | Gleich | `1 = 1` | `true`
`<>` | Ungleich | `1 <> 2` | `true`
`<` | Kleiner als | `1 < 2` | `true`
`<=` | Kleiner oder gleich | `1 <= 1` | `true`
`>` | Größer als | `3 > 2` | `true`
`>=` | Größer oder gleich | `3 >= 3` | `true`

## Umgang mit NULL-Werten

Neug behandelt NULL-Werte in Vergleichen gemäß den SQL-Standards. Wenn ein Operand in einer Vergleichsoperation NULL ist, ist das Ergebnis immer NULL, unabhängig vom verwendeten Vergleichsoperator. Dieses Verhalten wird als „dreiwertige Logik“ bezeichnet, bei der Vergleiche zu `true`, `false` oder `NULL` führen können.

Die folgende Tabelle zeigt, wie NULL-Werte in Vergleichsoperationen behandelt werden:

Operator | Beispiel | Ergebnis
---------|---------|--------
`=` | `NULL = NULL`, `NULL = 1` | `NULL`
`<>` | `NULL <> NULL`, `NULL <> 1` | `NULL`
`<` | `NULL < NULL`, `NULL < 1` | `NULL`
`<=` | `NULL <= NULL`, `NULL <= 1` | `NULL`
`>` | `NULL > NULL`, `NULL > 1` | `NULL`
`>=` | `NULL >= NULL`, `NULL >= 1` | `NULL`