# Comparison Operators

We demonstrate the various Comparison Operators supported by Neug through the following table.

Operator | Description | Example | Result
---------|-------------|---------|--------
`=` | Equal to | `1 = 1` | `true`
`<>` | Not equal to | `1 <> 2` | `true`
`<` | Less than | `1 < 2` | `true`
`<=` | Less than or equal to | `1 <= 1` | `true`
`>` | Greater than | `3 > 2` | `true`
`>=` | Greater than or equal to | `3 >= 3` | `true`

## NULL Value Handling

Neug handles NULL values in comparisons according to SQL standards. When any operand in a comparison operation is NULL, the result is always NULL, regardless of the comparison operator used. This behavior is known as "three-valued logic" where comparisons can result in `true`, `false`, or `NULL`.

The following table demonstrates how NULL values are handled in comparison operations:

Operator | Example | Result
---------|---------|--------
`=` | `NULL = NULL`, `NULL = 1` | `NULL`
`<>` | `NULL <> NULL`, `NULL <> 1` | `NULL`
`<` | `NULL < NULL`, `NULL < 1` | `NULL`
`<=` | `NULL <= NULL`, `NULL <= 1` | `NULL`
`>` | `NULL > NULL`, `NULL > 1` | `NULL`
`>=` | `NULL >= NULL`, `NULL >= 1` | `NULL`
