# Clause With

WITH est principalement utilisé pour effectuer une projection ou une agrégation supplémentaire des données actuelles. Nous allons maintenant introduire les utilisations associées depuis ces deux aspects.

## Agrégation

L'agrégation est similaire à l'opération GROUP BY en SQL, qui regroupe les données actuelles par propriétés et effectue les opérations correspondantes des fonctions d'agrégation sur les données de chaque groupe. Neug supporte actuellement les fonctions d'agrégation principales, incluant :
- COUNT
- COUNT_STAR
- COLLECT
- SUM
- MIN
- MAX
- AVG

Nous introduirons ces fonctions en détail dans la [Section Fonctions d'Agrégation](../expression/agg_func.md).

### Agrégation par Propriété Unique
```
Match (a) With label(a) as label, count(a.name) as cnt Return label, cnt;
```

<!-- todo: label n'est pas inclus dans le package actuel -->

### Agrégation par Propriétés Multiples
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt Return a_label, c_label, cnt;
```

### Appliquer plusieurs fonctions d'agrégation
```
Match (a)-[b:knows]->(c) With label(a) as a_label, label(c) as c_label, count(b) as cnt, sum(b.weight) as weights Return a_label, c_label, cnt, weights;
```

### Filtrer selon les résultats d'agrégation

```
Match (a:person) With label(a) as label, count(a.name) as cnt Where cnt > 2 Return label, cnt;
```

<!-- todo: label is null in output -->

## Projection

Une autre utilisation courante de WITH est de projeter davantage les résultats actuels par colonnes, ce qui équivaut à Column Pruning en SQL, en ne sortant que les colonnes nécessaires pour les requêtes suivantes.

## Projeter les données de vertex

```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With b
Match (b)-[:created]->(c)
Return c.name;
```

<!-- todo: multiple match is not included in current package -->

## Projeter les données de vertex/edge

```
Match (a:person {name: 'marko'})-[k:knows]->(b:person)
With b, k
Match (b)-[:created]->(c)
Return k.weight, c.name;
```

## Propriétés du projet
```
Match (a:person {name: 'marko'})-[:knows]->(b:person)
With a, b.age as b_age
Match (a)-[:created]->(c)
Where b_age > 20
Return c.name;
```

Projette les propriétés des données b générées par le premier Match, filtre les propriétés via Filter, et retourne finalement tous les c.name qui répondent aux conditions ;