# Logische Operatoren

Neug unterstützt derzeit drei logische Operatoren: AND, OR, NOT; und behandelt NULL-Werte speziell gemäß SQL's „Three-Valued Logic“. Die spezifischen Wahrheitstabellen sind unten dargestellt.

## AND-Wahrheitstabelle

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| TRUE     | TRUE     | TRUE   |
| TRUE     | FALSE    | FALSE  |
| TRUE     | NULL     | NULL   |
| FALSE    | TRUE     | FALSE  |
| FALSE    | FALSE    | FALSE  |
| FALSE    | NULL     | FALSE  |
| NULL     | TRUE     | NULL   |
| NULL     | FALSE    | FALSE  |
| NULL     | NULL     | NULL   |

## OR-Wahrheitstabelle

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| TRUE     | TRUE     | TRUE   |
| TRUE     | FALSE    | TRUE   |
| TRUE     | NULL     | TRUE   |
| FALSE    | TRUE     | TRUE   |
| FALSE    | FALSE    | FALSE  |
| FALSE    | NULL     | NULL   |
| NULL     | TRUE     | TRUE   |
| NULL     | FALSE    | NULL   |
| NULL     | NULL     | NULL   |

## NOT Wahrheitstabelle

| Operand | Ergebnis |
|---------|----------|
| TRUE    | FALSE    |
| FALSE   | TRUE     |
| NULL    | NULL     |