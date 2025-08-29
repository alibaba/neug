# DDL Clause

DDL (Data Definition Language) 是一组专门用于 schema 管理的操作。NeuG 支持对 schema 节点、边和属性进行添加、删除和修改操作。请参考以下使用示例。

## Create Node Type

创建一个 Label 类型为 "person" 的节点，指定 person 的属性名、类型和主键。

```
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

默认情况下，如果数据库中已存在 person 类型，则会报错。使用 IF NOT EXISTS 可以避免错误——只有当类型在数据库中不存在时才会创建，否则不执行任何操作。

```
CREATE NODE TABLE IF NOT EXISTS person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

## 创建 Edge Type

创建一个从 person 到 person 的 "knows" 类型的 edge，并指定 knows 的属性名和类型。目前，edge 不支持指定主键。

```
CREATE REL TABLE IF NOT EXISTS knows (
    FROM person TO person,
    weight DOUBLE
);
```

## 删除 Node Type

删除指定的 Node type。使用 IF EXISTS 可以避免在类型不存在时报错。

```
DROP TABLE IF EXISTS person;
```

## 删除 Edge Type

删除指定的 Edge type。使用 IF EXISTS 可以避免在类型不存在时报错。

```
DROP TABLE IF EXISTS knows;
```

## 重命名 Node 或 Edge Type

通过 `RENAME TO` 重命名 node 或 edge type。

```
ALTER TABLE person RENAME TO person2;
ALTER TABLE knows RENAME TO knows2;
```

## 添加属性

为 node 或 edge type 添加属性。

```
ALTER TABLE person ADD IF NOT EXISTS gender INT32;
ALTER TABLE knows ADD IF NOT EXISTS info STRING;
```

## 删除属性

从节点或边类型中删除属性。

```
ALTER TABLE person DROP IF EXISTS gender;
ALTER TABLE knows DROP IF EXISTS info;
```

## 重命名属性

重命名节点或边类型的属性。

```
ALTER TABLE person RENAME age TO age2;
ALTER TABLE knows RENAME weight TO weight2;
```