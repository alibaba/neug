# データのエクスポート
`COPY TO` コマンドを使用すると、クエリ結果を指定したファイル形式に直接エクスポートできます。これは、クエリ結果を他のシステムで使用したり、アーカイブ目的でクエリ出力を保存したりする際に便利です。

## CSV へのコピー

`COPY TO` 句を使用すると、クエリ結果を CSV ファイルにエクスポートできます。使用方法は以下の通りです：

```cypher
COPY (MATCH (p:person) RETURN p.*) TO 'person.csv' (header=true);
```

CSV ファイルは以下のフィールドで構成されます：

```csv
p.id|p.name|p.age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
```

vertex や edge などの複雑な型は、JSON 形式の文字列として出力されます。

利用可能なパラメータは以下の通りです：

|パラメータ|説明|デフォルト|
|---|---|---|
|`HEADER`|ヘッダー行を出力するかどうか。|`false`|
|`DELIM` または `DELIMITER`|CSV のフィールドを区切る文字。|`\|`|

別の例を以下に示します。

```cypher
COPY (MATCH (:person)-[e:knows]->(:person) RETURN e) TO 'person_knows_person.csv' (header=true);
```

これは `person_knows_person.csv` に以下の結果を出力します：

```csv
e
{'_SRC': '0:0', '_DST': '0:1', '_LABEL': 'knows', 'weight': 0.500000}
{'_SRC': '0:0', '_DST': '0:2', '_LABEL': 'knows', 'weight': 1.000000}
```