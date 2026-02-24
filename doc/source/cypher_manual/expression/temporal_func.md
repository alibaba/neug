# Temporal Functions

Temporal functions are a specialized set of functions designed for date and interval data types. They provide capabilities for constructing date/interval types from string literals and extracting specific fields from temporal values. The following table lists the available functions and their usage.

## Function Reference

| Function | Description | Example | Return Type |
|----------|-------------|---------|-------------|
| `DATE()` | Constructs a date value from a string literal | `DATE('2012-01-01')` | `DATE` |
| `TIMESTAMP()` | Constructs a timestamp value from a string literal | `TIMESTAMP('1926-11-21 13:22:19')` | `TIMESTAMP` |
| `INTERVAL()` | Constructs an interval value from a string literal | `INTERVAL('3 DAYS')` | `INTERVAL` |
| `DATE_PART(part, date)` | Extracts a specific part from a date value | `DATE_PART('year', DATE('1995-11-02'))` | `INTEGER` |
| `DATE_PART(part, timestamp)` | Extracts a specific part from a timestamp value | `DATE_PART('month', TIMESTAMP('1926-11-21 13:22:19'))` | `INTEGER` |
| `DATE_PART(part, interval)` | Extracts a specific part from an interval value | `DATE_PART('days', INTERVAL('1 days'))` | `INTEGER` |

