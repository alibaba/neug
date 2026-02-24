# Comparison Operators

We demonstrate the various Comparison Operators supported by NeuG through the following table.

Operator | Description | Example | Result
---------|-------------|---------|--------
`=` | Equal to | `1 = 1` | `true`
`<>` | Not equal to | `1 <> 2` | `true`
`<` | Less than | `1 < 2` | `true`
`<=` | Less than or equal to | `1 <= 1` | `true`
`>` | Greater than | `3 > 2` | `true`
`>=` | Greater than or equal to | `3 >= 3` | `true`

## NULL Value Handling

NeuG handles `NULL` values in comparisons according to SQL "three-valued logic" standards, where results of comparisons can be one of `true`, `false`, and `NULL`. When any operand in a comparison operation is NULL, the result is always NULL, regardless of the comparison operator used. 

The following table demonstrates how NULL values are handled in comparison operations:

Operator | Example | Result
---------|---------|--------
`=` | `NULL = NULL`, `NULL = 1` | `NULL`
`<>` | `NULL <> NULL`, `NULL <> 1` | `NULL`
`<` | `NULL < NULL`, `NULL < 1` | `NULL`
`<=` | `NULL <= NULL`, `NULL <= 1` | `NULL`
`>` | `NULL > NULL`, `NULL > 1` | `NULL`
`>=` | `NULL >= NULL`, `NULL >= 1` | `NULL`
