# Temporal Functions

Temporal functions construct temporal values, extract fields from them, and group dates or timestamps into time buckets. The following table lists the available functions and their usage.

## Function Reference

> For `TIME_BUCKET`, a **fixed-duration width** is an interval expressed in weeks, days, or smaller units that can be converted to a fixed number of milliseconds. For example, one day is always 86,400,000 milliseconds. Month/year widths use calendar months instead, whose lengths vary with the calendar.

| Function | Description | Example | Return Type |
|----------|-------------|---------|-------------|
| `DATE()` | Constructs a date value from a string literal | `DATE('2012-01-01')` | `DATE` |
| `TIMESTAMP()` | Constructs a timestamp value from a string literal | `TIMESTAMP('1926-11-21 13:22:19')` | `TIMESTAMP` |
| `INTERVAL()` | Constructs an interval value from a string literal | `INTERVAL('3 DAYS')` | `INTERVAL` |
| `DATE_PART(part, date)` | Extracts a specific part from a date value | `DATE_PART('year', DATE('1995-11-02'))` | `INTEGER` |
| `DATE_PART(part, timestamp)` | Extracts a specific part from a timestamp value | `DATE_PART('month', TIMESTAMP('1926-11-21 13:22:19'))` | `INTEGER` |
| `DATE_PART(part, interval)` | Extracts a specific part from an interval value | `DATE_PART('days', INTERVAL('1 days'))` | `INTEGER` |
| `TIME_BUCKET(width, date/timestamp)` | Returns the start of the bucket containing the date or timestamp. Buckets are aligned to `2000-01-01 00:00:00` for month/year widths, or `2000-01-03 00:00:00` (Monday) for week, day, and finer fixed-duration widths. | `TIME_BUCKET(INTERVAL('1 week'), DATE('2024-03-20'))` | Same as the input: `DATE` or `TIMESTAMP` |
| `TIME_BUCKET(width, date/timestamp, offset)` | Returns the bucket start with an `INTERVAL` offset from the default alignment: `2000-01-01 00:00:00 + offset` for month/year widths, or `2000-01-03 00:00:00 + offset` for fixed-duration widths. Calendar offsets containing months follow the calendar rules described below. | `TIME_BUCKET(INTERVAL('10 minutes'), TIMESTAMP('2024-03-20 15:26:12.345'), INTERVAL('5 minutes'))` | Same as the input: `DATE` or `TIMESTAMP` |
| `TIME_BUCKET(width, date/timestamp, origin)` | Returns the bucket start using a supplied `DATE` or `TIMESTAMP` origin instead of the default alignment. Fixed-duration widths use the complete origin; month/year widths use only its year and month. | `TIME_BUCKET(INTERVAL('2 months'), DATE('2024-03-20'), DATE('2024-02-15'))` | `DATE` if both temporal arguments are `DATE`; otherwise `TIMESTAMP` |

## Time Bucketing

`TIME_BUCKET` groups temporal values into intervals of a given `width` and returns the start of the bucket containing each value. Use the returned value to group events into periods such as ten minutes, a week, or a calendar month. A value exactly on a bucket boundary belongs to the bucket starting at that boundary. Buckets also extend backwards before the alignment origin.

### Supported Widths and Limits

The following rules apply to `width` in all three forms:

- `width` must be an `INTERVAL` greater than zero.
- Calendar widths may contain months or years, such as `INTERVAL('2 months')`, `INTERVAL('1 year')`, or `INTERVAL('1 year 2 months')`. A year is twelve calendar months; months are not treated as thirty days.
- **A width cannot combine months or years with days or time components.** For example, `INTERVAL('1 month 1 hour')`, `INTERVAL('1 month 1 day')`, and `INTERVAL('1 year 1 week')` are rejected.
- Fixed-duration widths may combine weeks, days, hours, minutes, seconds, and milliseconds. For example, `INTERVAL('1 week 1 hour')` and `INTERVAL('1 day 2 hours 30 minutes')` are supported. These widths use the `2000-01-03 00:00:00` default origin.
- Zero and negative widths are rejected. Fixed-duration components may have different signs if their total duration is positive, such as `INTERVAL('1 day -1 hour')`.

The temporal arguments support `DATE` and millisecond-precision `TIMESTAMP`. `TIME` and timezone-aware values are not supported. Construct typed values with `INTERVAL(...)`, `DATE(...)`, `TIMESTAMP(...)`, or an explicit `CAST`; plain strings are not accepted directly as `TIME_BUCKET` arguments. Calculations outside the supported temporal range raise an error.

### `TIME_BUCKET(width, date/timestamp)`

The two-argument form uses the default alignment origin:

| Parameter | Supported type | Meaning |
|-----------|----------------|---------|
| `width` | `INTERVAL` | Bucket width, subject to the limits above |
| `date/timestamp` | `DATE` or `TIMESTAMP` | Value to place in a bucket |

Month/year buckets start on the first day of a month at midnight and align to `2000-01-01 00:00:00`. Fixed-duration buckets align to `2000-01-03 00:00:00`, so a one-week bucket starts on Monday.

The return type matches the temporal input. A `DATE` is treated as midnight for bucketing, and the returned `DATE` contains only the calendar date of the bucket start. To retain hours, minutes, seconds, or milliseconds, convert the input to `TIMESTAMP` first.

```cypher
RETURN TIME_BUCKET(INTERVAL('10 minutes'),
                   TIMESTAMP('2024-03-20 15:26:12.345'));
// ---
// 2024-03-20 15:20:00.000
```

```cypher
RETURN TIME_BUCKET(INTERVAL('1 week'), DATE('2024-03-20'));
// ---
// 2024-03-18
```

A two-month bucket uses calendar months:

```cypher
RETURN TIME_BUCKET(INTERVAL('2 months'), DATE('2024-03-20'));
// ---
// 2024-03-01
```

Mixed fixed-duration units are also supported. Here, each bucket is seven days and one hour long:

```cypher
RETURN TIME_BUCKET(INTERVAL('1 week 1 hour'),
                   TIMESTAMP('2000-01-10 02:30:00'));
// ---
// 2000-01-10 01:00:00.000
```

### `TIME_BUCKET(width, date/timestamp, offset)`

The offset form shifts the default bucket alignment by an interval:

| Parameter | Supported type | Meaning |
|-----------|----------------|---------|
| `width` | `INTERVAL` | Bucket width, subject to the limits above |
| `date/timestamp` | `DATE` or `TIMESTAMP` | Value to place in a bucket |
| `offset` | `INTERVAL` | Calendar or duration offset from the default alignment |

The alignment starts at `2000-01-01 00:00:00 + offset` for month/year widths, or `2000-01-03 00:00:00 + offset` for fixed-duration widths. For example, a five-minute offset changes ten-minute bucket boundaries from `:00`, `:10`, `:20`, ... to `:05`, `:15`, `:25`, ... .

Unlike `width`, `offset` may be zero, negative, larger than the bucket width, or a mixture of month, day, and time units. Its time component must still be an exact whole number of milliseconds.

When `offset` contains months, calendar arithmetic applies: subtract the offset from the input, bucket that value using the default alignment, then add the offset to the bucket start. Moving to a month with fewer days adjusts an invalid day to that month's last day. Consequently, an offset containing months is not always equivalent to supplying `default origin + offset` as an explicit origin.

The return type matches `date/timestamp`. For a `DATE` input, the complete offset is applied before returning only the date of the bucket start.

```cypher
RETURN TIME_BUCKET(INTERVAL('10 minutes'),
                   TIMESTAMP('2024-03-20 15:26:12.345'),
                   INTERVAL('5 minutes'));
// ---
// 2024-03-20 15:25:00.000
```

A fifteen-day offset makes monthly buckets start on the sixteenth:

```cypher
RETURN TIME_BUCKET(INTERVAL('1 month'), DATE('2024-03-20'),
                   INTERVAL('15 days'));
// ---
// 2024-03-16
```

With a six-hour offset, midnight on March 20 belongs to the daily bucket starting at `2024-03-19 06:00:00`. A `DATE` result returns only that boundary's date:

```cypher
RETURN TIME_BUCKET(INTERVAL('1 day'), DATE('2024-03-20'),
                   INTERVAL('6 hours'));
// ---
// 2024-03-19
```

### `TIME_BUCKET(width, date/timestamp, origin)`

The origin form replaces the default alignment with a temporal value:

| Parameter | Supported type | Meaning |
|-----------|----------------|---------|
| `width` | `INTERVAL` | Bucket width, subject to the limits above |
| `date/timestamp` | `DATE` or `TIMESTAMP` | Value to place in a bucket |
| `origin` | `DATE` or `TIMESTAMP` | Custom bucket alignment origin |

For fixed-duration widths, the complete origin determines the bucket boundaries. A `DATE` origin represents midnight on that date.

For month/year widths, **only the origin's year and month are used**. Its day and time are ignored, and bucket starts are always the first day of a month at midnight. For example, a two-month width with an origin in February produces buckets starting on February 1, April 1, June 1, and so on, even if the origin is February 15 at noon.

The return type depends on both temporal arguments:

| `date/timestamp` type | `origin` type | Return type |
|-----------------------|---------------|-------------|
| `DATE` | `DATE` | `DATE` |
| `DATE` | `TIMESTAMP` | `TIMESTAMP` |
| `TIMESTAMP` | `DATE` | `TIMESTAMP` |
| `TIMESTAMP` | `TIMESTAMP` | `TIMESTAMP` |

When either temporal argument is a `TIMESTAMP`, a `DATE` argument is treated as midnight and the result preserves the timestamp precision.

```cypher
RETURN TIME_BUCKET(INTERVAL('10 minutes'),
                   TIMESTAMP('2024-03-20 15:26:12.345'),
                   TIMESTAMP('2024-03-20 15:03:00'));
// ---
// 2024-03-20 15:23:00.000
```

```cypher
RETURN TIME_BUCKET(INTERVAL('2 months'),
                   TIMESTAMP('2024-03-20 15:26:12.345'),
                   TIMESTAMP('2024-02-15 12:00:00'));
// ---
// 2024-02-01 00:00:00.000
```

Mixing `DATE` and `TIMESTAMP` returns a `TIMESTAMP`:

```cypher
RETURN TIME_BUCKET(INTERVAL('1 day'), DATE('2024-03-20'),
                   TIMESTAMP('2024-03-01 06:00:00'));
// ---
// 2024-03-19 06:00:00.000
```

### NULL Values

All arguments in all three forms may be `NULL`, provided their types can be determined. If any argument is `NULL`, the result is `NULL` with the return type described for that form. A `NULL` third argument does not request the default alignment; omit the third argument to use the default.

| Form | Nullable arguments | Result when any argument is `NULL` |
|------|--------------------|------------------------------------|
| `TIME_BUCKET(width, date/timestamp)` | `width`, `date/timestamp` | `NULL` |
| `TIME_BUCKET(width, date/timestamp, offset)` | `width`, `date/timestamp`, `offset` | `NULL` |
| `TIME_BUCKET(width, date/timestamp, origin)` | `width`, `date/timestamp`, `origin` | `NULL` |

Use `CAST(NULL, 'INTERVAL')`, `CAST(NULL, 'DATE')`, or `CAST(NULL, 'TIMESTAMP')` to specify the intended type. A bare `NULL` third argument is rejected because it could mean either an offset or an origin. In the two-argument and offset forms, a bare `NULL` temporal input also needs a cast to determine whether the result is `DATE` or `TIMESTAMP`. A bare `NULL` width can be inferred as `INTERVAL`; in the origin form, a bare `NULL` temporal input can take its type from a typed origin.

#### Two-Argument Form

Either argument may be `NULL`:

```cypher
RETURN TIME_BUCKET(CAST(NULL, 'INTERVAL'), DATE('2024-03-20')) AS null_width,
       TIME_BUCKET(INTERVAL('1 day'), CAST(NULL, 'TIMESTAMP')) AS null_timestamp;
// ---
// null_width: NULL, null_timestamp: NULL
```

#### Offset Form

Any of the three arguments may be `NULL`. Cast a `NULL` offset to `INTERVAL`:

```cypher
RETURN TIME_BUCKET(CAST(NULL, 'INTERVAL'), DATE('2024-03-20'), INTERVAL('6 hours')) AS null_width,
       TIME_BUCKET(INTERVAL('1 day'), CAST(NULL, 'DATE'), INTERVAL('6 hours')) AS null_date,
       TIME_BUCKET(INTERVAL('1 day'), DATE('2024-03-20'), CAST(NULL, 'INTERVAL')) AS null_offset;
// ---
// null_width: NULL, null_date: NULL, null_offset: NULL
```

#### Origin Form

Any of the three arguments may be `NULL`. Cast a `NULL` origin to `DATE` or `TIMESTAMP`:

```cypher
RETURN TIME_BUCKET(CAST(NULL, 'INTERVAL'), DATE('2024-03-20'), DATE('2024-03-01')) AS null_width,
       TIME_BUCKET(INTERVAL('1 day'), CAST(NULL, 'DATE'), DATE('2024-03-01')) AS null_date,
       TIME_BUCKET(INTERVAL('1 day'), DATE('2024-03-20'), CAST(NULL, 'TIMESTAMP')) AS null_origin;
// ---
// null_width: NULL, null_date: NULL, null_origin: NULL
```

Once argument types are valid, a `NULL` argument produces `NULL` even if the width would otherwise be invalid:

```cypher
RETURN TIME_BUCKET(INTERVAL('0 seconds'), CAST(NULL, 'TIMESTAMP'));
// ---
// NULL
```

