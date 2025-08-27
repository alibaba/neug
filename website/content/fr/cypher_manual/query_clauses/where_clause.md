# Clause WHERE

La clause WHERE est utilisée pour filtrer davantage les résultats produits par les opérations de requête précédentes, en se basant sur des prédicats ou des sous-requêtes. Le filtrage repose principalement sur des expressions logiques, que nous introduirons en détail dans la [section expression](../expression.md). Elle ne retourne que les données qui satisfont les conditions spécifiées.

## Filtrer par propriétés

Dans le chapitre précédent, nous avons introduit comment restreindre les paires clé-valeur des propriétés de nœuds et de relations à l'aide d'expressions telles que `(a:person {name: 'marko'})`. Nous complétons ici en expliquant comment exprimer le même effet via la clause WHERE.

### Filtrer par propriétés de nœuds
```cypher
MATCH (a:person) 
WHERE a.name = 'marko' OR a.age > 27
RETURN a.name, a.age;
```

résultat :
```
+-------------+------------+
| _0_a.name   |   _0_a.age |
+=============+============+
| marko       |         29 |
+-------------+------------+
| josh        |         32 |
+-------------+------------+
| peter       |         35 |
+-------------+------------+
```

### Filtrer par propriétés de nœuds/relations
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name = 'marko' AND b.weight = 1.0
RETURN a.name, b.weight;
```

résultat :
```
+-------------+---------------+
| _0_a.name   |   _4_b.weight |
+=============+===============+
| marko       |             1 |
+-------------+---------------+
```

### Filtrer par propriétés corrélées
```cypher
MATCH (a:person)-[b:knows]->(c:person) 
WHERE a.name <> c.name AND a.age > c.age 
RETURN a.name, a.age, c.name, c.age;
```

Résultat :
```
+-------------+------------+-------------+------------+
| _0_a.name   |   _0_a.age | _2_c.name   |   _2_c.age |
+=============+============+=============+============+
| marko       |         29 | vadas       |         27 |
+-------------+------------+-------------+------------+
```

## Filtrer avec NULL

Les valeurs NULL sont inévitables dans les processus de stockage et de calcul des données de graphe. Pour conserver ou supprimer ces valeurs NULL, nous pouvons utiliser `IS NULL` ou `IS NOT NULL` dans la clause WHERE.

### Filtrer les données de propriétés avec NULL
```cypher
MATCH (a) 
WHERE a.age IS NULL 
RETURN a.name;
```

```
+-------------+
| _0_a.name   |
+=============+
| lop         |
+-------------+
| ripple      |
+-------------+
```

### Filtrer les données optionnelles avec NULL
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NULL 
RETURN a.name;
```

<!-- todo: optional match is not included in current pip package -->

### Exclure les données optionnelles avec IS NOT NULL
```cypher
MATCH (a) 
OPTIONAL MATCH (a)-[:knows]->(b) 
WHERE b IS NOT NULL 
RETURN a.name;
```

## WHERE avec sous-requête

La clause WHERE peut également être utilisée avec des sous-requêtes pour effectuer des opérations de filtrage plus complexes.

### Pattern Exists
```cypher
MATCH (a) 
WHERE (a)-[:knows]->(b) 
RETURN a.name;
```
Cette requête retourne toutes les valeurs `a.name` qui ont une relation `knows`.

### Pattern Not Exists
```cypher
MATCH (a) 
WHERE NOT (a)-[:knows]->(b) 
RETURN a.name;
```
Cette requête retourne toutes les valeurs `a.name` où il n'y a pas de relations `knows`, équivalent à la sémantique ANTI_JOIN en SQL.

<!-- todo: where subquery is unsupported yet -->