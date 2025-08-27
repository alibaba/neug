# Предложение With

WITH в основном используется для дальнейшей проекции или агрегации текущих данных. Далее мы рассмотрим соответствующее использование с этих двух сторон.

## Агрегация

Агрегация похожа на операцию GROUP BY в SQL, которая группирует текущие данные по свойствам и выполняет соответствующие операции агрегатных функций над данными каждой группы. Neug в настоящее время поддерживает основные агрегатные функции, включая:
- COUNT
- COUNT_STAR
- COLLECT
- SUM
- MIN
- MAX
- AVG

Мы подробно рассмотрим эти функции в разделе [Агрегатные функции](../expression/agg_func.md).

### Агрегация по одному свойству
```
Match (a) With label(a) as label, count(a.name) as cnt Return label, cnt;
```

<!-- todo: label не включен в текущий пакет -->

### Агрегация по нескольким свойствам
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt Return a_label, c_label, cnt;
```

### Применение нескольких агрегатных функций
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt, sum(b.weight) as weights Return a_label, c_label, cnt, weights;
```

### Фильтрация на основе результатов агрегации

```
Match (a:person) With label(a) as label, count(a.name) as cnt Where cnt > 2 Return label, cnt;
```

<!-- todo: label is null in output -->

## Проекция

Еще одно распространенное использование WITH — это дальнейшая проекция текущих результатов по столбцам, что эквивалентно Column Pruning в SQL, то есть вывод только тех столбцов, которые необходимы для последующих запросов.

## Проекция данных вершин

```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With b
Match (b)-[:created]->(c)
Return c.name;
```

<!-- todo: multiple match is not included in current package -->

## Проекция данных вершин/ребер

```
Match (a:person {name: 'marko'})-[k:knows]->(b:person)
With b, k
Match (b)-[:created]->(c)
Return k.weight, c.name;
```

## Свойства проекта
```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With a, b.age as b_age
Match (a)-[:created]->(c)
Where b_age > 20
Return c.name;
```

Проектируем свойства данных b, сгенерированные первым Match, фильтруем свойства через Filter, и в конце выводим все c.name, которые удовлетворяют условиям;