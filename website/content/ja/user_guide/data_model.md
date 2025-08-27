# データモデル

## データ型

`Neug` は、vertex および edge のプロパティで使用される多様なデータ型を包括的にサポートしています。これらのデータ型は、以下の5つのカテゴリに分類されます：

- **Primitive Types:** 整数、浮動小数点数、ブール値など、基本的な数値および論理値を表現するために不可欠な基本データ型を含みます。

- **String Types:** テキストデータを扱う型のカテゴリで、vertex および edge 内での文字列の効率的な格納および操作を可能にします。

- **Temporal Types:** 日付および時刻情報を扱うために設計された型で、さまざまな時間表現に対応し、時間軸に沿った分析およびクエリを実現します。

- **Array Types:** 複数の要素からなるコレクションを格納できる型で、グラフ要素に関連付けられた値のリストまたはセットを表現することを可能にします。

- **Complex Types:**（現在は未サポート）ユーザー定義オブジェクトや複数のプリミティブ型を組み合わせた複合構造を含み、より高度なデータモデリングおよびグラフ内でのリレーションを実現します。

### Primitive Types

もちろん！以下は指定されたヘッダーと最初の行のエントリを持つ Markdown テーブルです：

| Type   | Size    | Description                    |
|--------|---------|--------------------------------|
| BOOL   | 1 bit   | 1ビットのboolean値             |
| INT8   | 1 byte  | 1バイトの整数                  |
| UINT8  | 1 byte  | 符号なし1バイト整数            |
| INT16  | 2 bytes | 2バイトの整数                  |
| UINT16 | 2 bytes | 符号なし2バイト整数            |
| INT32  | 4 bytes | 4バイトの整数                  |
| UINT32 | 4 bytes | 符号なし4バイト整数            |
| INT64  | 8 bytes | 8バイトの整数                  |
| UINT64 | 8 bytes | 符号なし8バイト整数            |
| FLOAT  | 4 bytes | 単精度浮動小数点数             |
| DOUBLE | 8 bytes | 倍精度浮動小数点数             |

### String Types

| Type   | Size    | Description                    |
|--------|---------|--------------------------------|
| STRING | -       | 可変長文字列                   |

### Temporal型

#### DATE

`DATE`は、年、月、日という構成要素によって特徴付けられるカレンダー上の日付を表し、ISO-8601標準（YYYY-MM-DD）に準拠したフォーマットで表現されます。ストレージ上では4バイトを消費します。

```cypher
RETURN date('2022-06-06') as x;
```

#### DATE32

`DATE32`は、1970年1月1日からの日数をカウントすることでカレンダー上の日付を表現します。この日付より前の日付は負の値で表されます。ストレージ上では4バイトを消費します。（TODO: (xiaoli): 動作確認をしてください）

```cypher
RETURN date32('2022-06-06') as x;
```

#### TIMESTAMP

`TIMESTAMP`は、日付と時刻の両方の情報を統合したデータ型で、ISO-8601標準（YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]]）に従ってフォーマットされます。この形式では、日付（YYYY-MM-DD）、時刻（hh:mm:ss[.zzzzzz]）、およびオプションのタイムゾーンオフセット[+-TT[:tt]]を指定します。日付部分は必須ですが、時刻部分はオプションであり、ミリ秒[.zzzzzz]やタイムゾーンオフセットを含めることもできます。ストレージ上では8バイトを消費します。例：「1970-01-01 00:00:00.004666-10」

```cypher
RETURN timestamp("1970-01-01 00:00:00.004666-10") as x;
```

#### INTERVAL

`INTERVAL`は時間の期間を表し、年、月、日、時、分、マイクロ秒のコンポーネントで特徴付けることができます。我々は[kuzu](https://docs.kuzudb.com/cypher/data-types/#interval)におけるIntervalの定義に従っています。

```cypher
RETURN interval("1 year 2 days") as x;
```

### 配列型

配列型は現在サポートされていませんが、近い将来サポートされる予定です。
サポートされた際には、配列内のすべての要素が前述のプリミティブ型のいずれかに従う必要があります。

### 複合型

複合型は現在サポートされていません。これには `STRUCT`、`MAP`、`UNION`、`BLOB` が含まれます。

## 型変換

数値型同士の[型キャスト](https://github.com/GraphScope/neug/issues/416)をサポートしています。

- INT32
- UINT32
- INT64
- UINT64
- FLOAT
- DOUBLE

**これらの型はすべて、お互いに変換可能です**。キャスト時には、コンパイラがオーバーフローをチェックし、安全な変換であることを確認します。以下の表に、変換中に発生する可能性のあるオーバーフローのシナリオをまとめます：

| From \ To | INT32  | UINT32 | INT64  | UINT64 | FLOAT  | DOUBLE |
|--------|--------|--------|--------|--------|--------|--------|
| **INT32**  | ✅ Safe | ⚠️ May Overflow (値が負の場合)        | ✅ Safe | ⚠️ May Overflow (値が負の場合)        | ✅ Safe | ✅ Safe |
| **UINT32** | ⚠️ May Overflow (値が INT32_MAX を超える場合)         | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe |
| **INT64**  | ⚠️ May Overflow (値が INT32_MIN 未満または INT32_MAX を超える場合) | ⚠️ May Overflow (値が負または UINT32_MAX を超える場合) | ✅ Safe | ⚠️ May Overflow (値が負の場合)        | ✅ Safe | ✅ Safe |
| **UINT64** | ⚠️ May Overflow (値が INT32_MAX を超える場合)         | ⚠️ May Overflow (値が UINT32_MAX を超える場合)       | ⚠️ May Overflow (値が INT64_MAX を超える場合) | ✅ Safe | ✅ Safe | ✅ Safe |
| **FLOAT**  | ⚠️ May Overflow (値が INT32_MIN 未満または INT32_MAX を超える場合) | ⚠️ May Overflow (値が負または UINT32_MAX を超える場合) | ⚠️ May Overflow (値が INT64_MIN 未満または INT64_MAX を超える場合) | ⚠️ May Overflow (値が負または UINT64_MAX を超える場合) | ✅ Safe | ✅ Safe |
| **DOUBLE** | ⚠️ May Overflow (値が INT32_MIN 未満または INT32_MAX を超える場合) | ⚠️ May Overflow (値が負または UINT32_MAX を超える場合) | ⚠️ May Overflow (値が INT64_MIN 未満または INT64_MAX を超える場合) | ⚠️ May Overflow (値が負または UINT64_MAX を超える場合) | ⚠️ May Overflow (値が -FLT_MAX 未満または FLT_MAX を超える場合) | ✅ Safe |