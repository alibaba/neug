# Aggregate Function

Aggregate Functions are primarily used to group current data and perform aggregation operations on elements within each group, ultimately producing only a single value for each group. The Aggregate Functions supported by NeuG are as follows:

Function | Description | Can be used with DISTINCT | Example
---------|-------------|---------------------------|--------
count | return the row counts | YES | RETURN count(a.name);
collect | collect the elements in a single list | YES | RETURN collect(a.name);
min | return the minimum value | NO | RETURN min(a.age);
max | return the maximum value | NO | RETURN max(a.age);
sum | sum up the value | NO | RETURN sum(a.age);
avg | return the average value | NO | RETURN avg(a.age);

## NULL Value Handling

- When the input is empty, `count()` and `sum` return `0`, `collect` returns
  `[]`, and `avg`, `min`, and `max` return `NULL`.
- When the input contains `NULL` values, all aggregate functions ignore them.
  In particular, `count(*)` counts every input row, including rows containing
  `NULL` values.
- When the input consists entirely of `NULL` values, the aggregate results
  after ignoring `NULL` are the same as for empty input: `count()` and `sum`
  return `0`, `collect` returns `[]`, and `avg`, `min`, and `max` return
  `NULL`. `count(*)` still returns the total number of input rows.

Input | `count(expr)` | `count(*)` | `collect` | `min` | `max` | `sum` | `avg`
------|---------------|------------|-----------|-------|-------|-------|------
`[]` | `0` | `0` | `[]` | `NULL` | `NULL` | `0` | `NULL`
`[1, NULL, 2]` | `2` | `3` | `[1, 2]` | `1` | `2` | `3` | `1.5`
`[NULL, NULL]` | `0` | `2` | `[]` | `NULL` | `NULL` | `0` | `NULL`
