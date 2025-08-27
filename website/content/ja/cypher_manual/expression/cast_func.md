# CAST 関数

CAST 関数は、主に異なるデータ型間の変換に使用されます。現在のところ、リテラル定数値の型変換のみをサポートしており、将来的には変数、プロパティ、さらに複雑な式のサポートを予定しています。

## Cast Literal

Neug では現在、以下の型変換が可能です：
- Numeric 型同士の相互変換。Numeric 型には INT32、UINT32、INT64、UINT64、FLOAT、DOUBLE が含まれます。任意の 2 つの Numeric 型は変換可能です。オーバーフローが発生した場合は例外がスローされます。以下の表に、発生する可能性のある例外を示します。
- String 型と Numeric 型の間の変換。String は任意の Numeric 型との間で変換可能です。特定の値が変換できない場合、例外がスローされます。

| From \ To | INT32  | UINT32 | INT64  | UINT64 | FLOAT  | DOUBLE |
|--------|--------|--------|--------|--------|--------|--------|
| **INT32**  | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT32** | ⚠️ May Overflow (if value > INT32_MAX)         | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe |
| **INT64**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT64** | ⚠️ May Overflow (if value > INT32_MAX)         | ⚠️ May Overflow (if value > UINT32_MAX)       | ⚠️ May Overflow (if value > INT64_MAX) | ✅ Safe | ✅ Safe | ✅ Safe |
| **FLOAT**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ✅ Safe | ✅ Safe |
| **DOUBLE** | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ⚠️ May Overflow (if value < -FLT_MAX or > FLT_MAX) | ✅ Safe |