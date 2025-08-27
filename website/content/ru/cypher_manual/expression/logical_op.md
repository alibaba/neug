# Логические операторы

Neug в настоящее время поддерживает три логических оператора: AND, OR, NOT; и обрабатывает значения NULL специально в соответствии с «трехзначной логикой» SQL. Конкретные таблицы истинности показаны ниже.

## Таблица истинности AND

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

## Таблица истинности OR

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

## Таблица истинности оператора NOT

| Операнд | Результат |
|---------|-----------|
| TRUE    | FALSE     |
| FALSE   | TRUE      |
| NULL    | NULL      |