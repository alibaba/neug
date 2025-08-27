# データのインポート

NeuG は、指定されたファイルからデータベースにデータを挿入する機能をサポートしています。データベースにデータを挿入する前提条件は、まずグラフスキーマ、つまりノードテーブルとリレーションシップテーブルの構造を作成することです。

通常、`COPY FROM` コマンドを使用してファイルからデータベースにデータをインポートできます。

## CSV からのコピー

### Nodeテーブルへのインポート

以下のようにしてノードテーブル`person`を作成します：

```cypher
CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));
```

CSVファイル`person.csv`には以下の列が含まれています：

```csv
id|name|age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
...
```

以下のステートメントで`person.csv`をpersonテーブルにインポートできます。

```cypher
COPY User FROM "user.csv" (header=true);
```

**注意:**
- CSVファイルの列数は、ノードに定義されたプロパティの数と一致する必要があります。
- CSVファイルの列の順序は、ノードに定義されたプロパティの順序と一致している必要があります。例えば、上記のノードではid、age、nameの順にプロパティが定義されており、これはCSVファイルの列と対応しています。

### リレーションシップテーブルへのインポート

以下のクエリを使用して、リレーションシップテーブル `knows` を作成します：

```cypher
CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);
```

CSVファイル `person_knows_person.csv` には以下のカラムが含まれています：

```csv
person.id|person.id|weight
1|2|0.5
1|4|1.0
...
```

以下のステートメントは、`person_knows_person.csv` ファイルを `knows` テーブルにインポートします。

```cypher
COPY knows from "person_knows_person.csv" (from="person", to="person", header=true);
```

**Note**
- リレーションシップテーブルにインポートする際、NeuGはファイル内の最初の2つのカラムを `FROM` および `TO` ノードの主キーとみなします。残りのカラムはリレーションシップのプロパティに対応します。
- `FROM` および `TO` パラメータは、`FROM` および `TO` ノードのラベルを指定するために、CSV設定に含める必要があります。

### CSV 設定
以下の設定パラメータがサポートされています：

|パラメータ|説明|デフォルト値|
|---|---|---|
|`HEADER`|CSVファイルの最初の行がヘッダーかどうか。trueまたはfalseを指定可能。|`false`|
|`DELIM` または `DELIMITER`|行内で異なるカラムを区切る文字。|`\|`|
|`QUOTE`|文字列のクォートを開始する文字。|`"`|
|`ESCAPE`|クォート内でのQUOTE文字や改行などのエスケープに使用される文字。|`\`|
|`SKIP`|入力ファイルからスキップする行数。|`0`|
|`PARALLEL`|CSVファイルを並列で読み込むかどうか。|`true`|
|`IGNORE_ERRORS`|trueに設定すると、不正な形式の行をスキップする。|`false`|
|`NULL_STRINGS`|CSVファイル内でnullとして扱うべき文字列。|`""`（空文字列）|
|`FROM`|ソースとなるvertexラベル名。エッジインポート時のみ使用。|`-`|
|`TO`|ターゲットとなるvertexラベル名。エッジインポート時のみ使用。|`-`|

**注意**
- パラメータは `param1=value1, param2=value2, ...` のようにカンマ区切りのキー・値ペアで指定してください。
- パラメータ名は大文字・小文字を区別しません。つまり、`Header`、`HEADER`、`header` はすべて有効で、正しく認識されます。
- boolean型のパラメータ（例：`header`）については以下のように指定します：
  1. 有効にする場合は `True`、`true`、または `1` を使用。
  2. 無効にする場合は `False`、`false`、または `0` を使用。
  3. 値が `true` の場合は値部分を省略可能です。例えば、`header=true` は `header` と短縮して記述できます。
- `FROM` と `TO` は特別なペアのパラメータです。CSVファイルからエッジをインポートする場合にのみ有効です。一つのエッジに対して複数のラベル組み合わせが存在する場合（例：Comment-replyOf->Post、Comment-replyOf->Comment）、`FROM` と `TO` の指定が必須となります。他のシナリオでこれらのパラメータを使用すると例外が発生します。