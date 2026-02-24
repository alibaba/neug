# Logical Operators

NeuG currently supports three Logical Operators: AND, OR, NOT; and handles NULL values specially according to SQL's "three-valued logic". The specific truth tables are shown below.

## AND Truth Table

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

## OR Truth Table

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

## NOT Truth Table

| Operand | Result |
|---------|--------|
| TRUE    | FALSE  |
| FALSE   | TRUE   |
| NULL    | NULL   |