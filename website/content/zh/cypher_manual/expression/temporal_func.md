# Temporal Functions

Temporal functions 是一组专门用于处理日期和时间间隔数据类型的函数。它们提供了从字符串字面量构造日期/时间间隔类型，以及从时间值中提取特定字段的功能。下表列出了可用的函数及其用法。

## 函数参考

| 函数 | 描述 | 示例 | 返回类型 |
|------|------|------|----------|
| `DATE()` | 从字符串字面量构造一个日期值 | `DATE('2012-01-01')` | `DATE` |
| `TIMESTAMP()` | 从字符串字面量构造一个时间戳值 | `TIMESTAMP('1926-11-21 13:22:19')` | `TIMESTAMP` |
| `INTERVAL()` | 从字符串字面量构造一个时间间隔值 | `INTERVAL('3 DAYS')` | `INTERVAL` |
| `date_part(part, date)` | 从日期值中提取指定部分 | `date_part('year', DATE('1995-11-02'))` | `INTEGER` |
| `date_part(part, timestamp)` | 从时间戳值中提取指定部分 | `date_part('month', TIMESTAMP('1926-11-21 13:22:19'))` | `INTEGER` |
| `date_part(part, interval)` | 从时间间隔值中提取指定部分 | `date_part('days', INTERVAL('1 days'))` | `INTEGER` |