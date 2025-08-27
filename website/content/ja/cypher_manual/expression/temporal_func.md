# Temporal Functions

Temporal Functions は、日付とインターバルのデータ型向けに設計された特殊な関数セットです。これらの関数は、文字列リテラルから日付/インターバル型を構築したり、Temporal 値から特定のフィールドを抽出したりする機能を提供します。以下のテーブルに、利用可能な関数とその使用方法を示します。

## 関数リファレンス

| 関数 | 説明 | 例 | 戻り値の型 |
|----------|-------------|---------|-------------|
| `DATE()` | 文字列リテラルから日付値を構築する | `DATE('2012-01-01')` | `DATE` |
| `TIMESTAMP()` | 文字列リテラルからタイムスタンプ値を構築する | `TIMESTAMP('1926-11-21 13:22:19')` | `TIMESTAMP` |
| `INTERVAL()` | 文字列リテラルからインターバル値を構築する | `INTERVAL('3 DAYS')` | `INTERVAL` |
| `date_part(part, date)` | 日付値から特定の部分を抽出する | `date_part('year', DATE('1995-11-02'))` | `INTEGER` |
| `date_part(part, timestamp)` | タイムスタンプ値から特定の部分を抽出する | `date_part('month', TIMESTAMP('1926-11-21 13:22:19'))` | `INTEGER` |
| `date_part(part, interval)` | インターバル値から特定の部分を抽出する | `date_part('days', INTERVAL('1 days'))` | `INTEGER` |