# 算術演算子

Neug は、加算 (+)、減算 (-)、乗算 (*)、除算 (/)、および剰余 (%) を含む一般的な算術演算をサポートしています。

## Result Type

算術演算に参加できるデータ型は以下の通りです：INT32、UINT32、INT64、UINT64、FLOAT、DOUBLE。算術演算の結果の型は、オペランドの型に従って次のように決定されます：
- 左オペランドと右オペランドのビット幅が異なる場合、結果はビット幅が大きい方の型になります
- 左オペランドと右オペランドのビット幅が同じで符号が異なる場合、結果は次の大きな符号付き型になります
- 浮動小数点数の最大ビット幅の型は DOUBLE、整数の最大ビット幅の型は INT64 です

以下の表は、異なるオペランドの組み合わせにおける結果の型を詳細に示しています：

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| INT32    | INT32    | INT32  |
| INT32    | UINT32   | INT64  |
| INT32    | INT64    | INT64  |
| INT32    | UINT64   | UINT64 |
| INT32    | FLOAT    | FLOAT  |
| INT32    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| UINT32   | INT32    | INT64  |
| UINT32   | UINT32   | UINT32 |
| UINT32   | INT64    | INT64  |
| UINT32   | UINT64   | UINT64 |
| UINT32   | FLOAT    | FLOAT  |
| UINT32   | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| INT64    | INT32    | INT64  |
| INT64    | UINT32   | INT64  |
| INT64    | INT64    | INT64  |
| INT64    | UINT64   | UINT64 |
| INT64    | FLOAT    | FLOAT  |
| INT64    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| UINT64   | INT32    | UINT64 |
| UINT64   | UINT32   | UINT64 |
| UINT64   | INT64    | UINT64 |
| UINT64   | UINT64   | UINT64 |
| UINT64   | FLOAT    | FLOAT  |
| UINT64   | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| FLOAT    | INT32    | FLOAT  |
| FLOAT    | UINT32   | FLOAT  |
| FLOAT    | INT64    | FLOAT  |
| FLOAT    | UINT64   | FLOAT  |
| FLOAT    | FLOAT    | FLOAT  |
| FLOAT    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| DOUBLE   | INT32    | DOUBLE |
| DOUBLE   | UINT32   | DOUBLE |
| DOUBLE   | INT64    | DOUBLE |
| DOUBLE   | UINT64   | DOUBLE |
| DOUBLE   | FLOAT    | DOUBLE |
| DOUBLE   | DOUBLE   | DOUBLE |

## エラーハンドリング

結果型に加えて、算術演算ではオペランドの値によってオーバーフロー、アンダーフロー、またはゼロ除算エラーが発生する可能性があります。これらのエラーに対して、Neug ではデータ型とデプロイメントモードに応じて異なるハンドリングを提供しています：

1. **浮動小数点型**: 特別な処理は行われず、システムは標準の [IEEE 754]() の動作に依存し、Infinity、-Infinity、または NaN 値を返します。

2. **整数型**: デバッグモードとリリースモードで動作が異なります：
   - **デバッグモード**: パフォーマンス要件が低いため、実行時にインプットの検証を行い、明示的に例外をスローできます。
   - **リリースモード**: パフォーマンス要件が高いため、オーバーフロー／アンダーフローの場合は未定義の値を返します。ただし、ゼロ除算の場合はコアダンプが発生する可能性があります。

以下の表は、各演算子が遭遇する可能性のあるエラーの種類を詳細に示しています：

| Operator | Overflow | Underflow | DivideByZero | Example |
|----------|----------|-----------|--------------|---------|
| +        | YES      | YES       | NO           | Return CAST(2147483647, 'int32') + CAST(1, 'int32') |
| -        | YES      | YES       | NO           | Return CAST(-2147483648, 'int32') - CAST(1, 'int32') |
| *        | YES      | YES       | NO           | Return CAST(2147483647, 'int32') * CAST(2, 'int32') |
| /        | NO       | NO        | YES          | Return 5 / 0 |
| %        | NO       | NO        | YES          | Return 5 % 0 |

## 日付演算

数値型に加えて、日時型とインターバル型でも算術演算を実行できます。Neugは日時演算をサポートしており、日付やタイムスタンプからインターバルを加算または減算したり、時間値間の差を計算したりできます。

### サポートされる演算

以下の表に、サポートされる日付演算を詳細に示します：

| 演算 | 説明 | 例 | 結果 |
|-----------|-------------|---------|--------|
| DATE + INTERVAL | 日付にインターバルを加算 | `DATE('2011-02-15') + INTERVAL('5 DAYS')` | `DATE('2011-02-20')` |
| DATE - INTERVAL | 日付からインターバルを減算 | `DATE('2011-02-15') - INTERVAL('5 DAYS')` | `DATE('2011-02-10')` |
| TIMESTAMP + INTERVAL | タイムスタンプにインターバルを加算 | `TIMESTAMP('2011-10-21 14:25:13') + INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-22 20:25:33')` |
| TIMESTAMP - INTERVAL | タイムスタンプからインターバルを減算 | `TIMESTAMP('2011-10-21 14:25:13') - INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-20 08:24:53')` |
| INTERVAL + INTERVAL | 2つのインターバルを加算 | `INTERVAL('5 DAYS') + INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('6 DAYS 6 HOURS 20 SECONDS')` |
| INTERVAL - INTERVAL | インターバルを減算 | `INTERVAL('5 DAYS') - INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('3 DAYS 17 HOURS 39 MINUTES 40 SECONDS')` |
| TIMESTAMP - TIMESTAMP | 時間差を計算 | `TIMESTAMP('2011-10-21 14:25:13') - TIMESTAMP('1989-10-21 14:25:13')` | `INTERVAL('8035 DAYS')` |