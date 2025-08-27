# DDL Clause

DDL (Data Definition Language) — это набор операций, предназначенных специально для управления схемой данных. Neug поддерживает операции добавления, удаления и изменения узлов, рёбер и свойств в схеме. Примеры использования приведены ниже.

## Создание типа узла

Создайте узел с типом Label "person", указав имена свойств, их типы и первичный ключ для person.

```
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

По умолчанию, если тип person уже существует в базе данных, будет выдана ошибка. Используйте IF NOT EXISTS, чтобы избежать ошибки — таблица будет создана только в том случае, если такого типа ещё нет в базе, в противном случае ничего не произойдёт.

```
CREATE NODE TABLE IF NOT EXISTS person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

## Создание типа Rel

Создайте ребро типа "knows" от person к person, указав имена и типы свойств для knows. В настоящее время ребра не поддерживают указание первичных ключей.

```
CREATE REL TABLE IF NOT EXISTS knows (
    FROM person TO person,
    weight DOUBLE
);
```

## Удаление типа Node

Удалите указанный тип Node. Используйте IF EXISTS, чтобы избежать ошибок, когда тип не существует.

```
DROP TABLE IF EXISTS person;
```

## Удаление типа Rel

Удалите указанный тип Relationship. Используйте IF EXISTS, чтобы избежать ошибок, когда тип не существует.

```
DROP TABLE IF EXISTS knows;
```

## Переименование типа Node или Rel

Переименуйте тип node или relationship с помощью `RENAME TO`.

```
ALTER TABLE person RENAME TO person2;
ALTER TABLE knows RENAME TO knows2;
```

## Добавление свойства

Добавьте свойства к типу node или relationship.

```
ALTER TABLE person ADD IF NOT EXISTS gender INT32;
ALTER TABLE knows ADD IF NOT EXISTS info STRING;
```

## Drop Property

Удаление свойств из типа узла или связи.

```
ALTER TABLE person DROP IF EXISTS gender;
ALTER TABLE knows DROP IF EXISTS info;
```

## Rename Property

Переименование свойств типа узла или связи.

```
ALTER TABLE person RENAME age TO age2;
ALTER TABLE knows RENAME weight TO weight2;
```