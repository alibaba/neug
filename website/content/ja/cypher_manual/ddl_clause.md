# DDL Clause

DDL (Data Definition Language) はスキーマ管理専用に設計された操作のセットです。Neug はスキーマのノード、エッジ、プロパティの追加、削除、変更操作をサポートしています。以下の使用例を参考にしてください。

## ノードタイプの作成

Label タイプが "person" のノードを作成し、person のプロパティ名、型、プライマリキーを指定します。

```
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

デフォルトでは、データベースに person タイプが既に存在する場合、エラーが報告されます。IF NOT EXISTS を使用するとエラーを回避でき、データベースにタイプが存在しない場合にのみ作成し、そうでない場合は何も実行しません。

```
CREATE NODE TABLE IF NOT EXISTS person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

## Rel Type の作成

"knows" タイプのエッジを person から person へ作成し、knows のプロパティ名と型を指定します。現在のところ、エッジは主キーの指定をサポートしていません。

```
CREATE REL TABLE IF NOT EXISTS knows (
    FROM person TO person,
    weight DOUBLE
);
```

## Node Type の削除

指定した Node タイプを削除します。型が存在しない場合にエラーを回避するために `IF EXISTS` を使用します。

```
DROP TABLE IF EXISTS person;
```

## Rel Type の削除

指定した Relationship タイプを削除します。型が存在しない場合にエラーを回避するために `IF EXISTS` を使用します。

```
DROP TABLE IF EXISTS knows;
```

## Node または Rel Type の名前変更

`RENAME TO` を使用してノードまたはリレーションシップタイプの名前を変更します。

```
ALTER TABLE person RENAME TO person2;
ALTER TABLE knows RENAME TO knows2;
```

## プロパティの追加

ノードまたはリレーションシップタイプにプロパティを追加します。

```
ALTER TABLE person ADD IF NOT EXISTS gender INT32;
ALTER TABLE knows ADD IF NOT EXISTS info STRING;
```

## Drop Property

ノードまたはリレーションシップ型からプロパティを削除します。

```
ALTER TABLE person DROP IF EXISTS gender;
ALTER TABLE knows DROP IF EXISTS info;
```

## Rename Property

ノードまたはリレーションシップ型のプロパティ名を変更します。

```
ALTER TABLE person RENAME age TO age2;
ALTER TABLE knows RENAME weight TO weight2;
```