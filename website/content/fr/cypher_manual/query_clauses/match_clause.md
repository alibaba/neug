# Clause MATCH

La clause `MATCH` est utilisée pour rechercher des motifs dans la base de données graphe. Elle vous permet de trouver des nœuds, des relations et des chemins qui correspondent à des critères spécifiques.

## Rechercher des Nœuds

### Rechercher des Nœuds avec une Seule Étiquette

Trouver tous les nœuds avec une étiquette spécifique. Cette requête retourne tous les nœuds étiquetés comme `person`.

```cypher
MATCH (p:person) RETURN p;
```

Résultat :
```
+-------------------------------------------------------+
| p                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29} |
+-------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27} |
+-------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}  |
+-------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35} |
+-------------------------------------------------------+
```

### Rechercher des nœuds avec plusieurs labels

Trouvez les nœuds qui possèdent l'un des labels spécifiés. Cette requête retourne tous les nœuds étiquetés soit comme `person`, soit comme `software`.

**Note** : Contrairement à Neo4j, Neug ne prend pas en charge les nœuds multi-labels. Dans Neo4j, `(p:person:software)` représente des nœuds qui ont à la fois les labels `person` et `software` simultanément. Dans Neug, cette syntaxe représente une union de nœuds avec soit le label `person`, soit le label `software`.

```cypher
MATCH (p:person:software) RETURN p;
```

Résultat :
```
+-----------------------------------------------------------------------------+
| p                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: software, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
```

### Match Nodes avec N'importe quel Label

Matchez des nodes sans spécifier de label. Neug supporte les requêtes sans labels explicites et infère automatiquement les labels inconnus pendant la compilation en se basant sur les contraintes de schéma définies.

```cypher
MATCH (p) RETURN p;
```

output:
```
+-----------------------------------------------------------------------------+
| p                                                                           |
+=============================================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29}                       |
+-----------------------------------------------------------------------------+
| {_ID: 1, _LABEL: person, name: vadas, age: 27}                       |
+-----------------------------------------------------------------------------+
| {_ID: 2, _LABEL: person, name: josh, age: 32}                        |
+-----------------------------------------------------------------------------+
| {_ID: 3, _LABEL: person, name: peter, age: 35}                       |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927936, _LABEL: software, name: lop, lang: java}    |
+-----------------------------------------------------------------------------+
| {_ID: 72057594037927937, _LABEL: software, name: ripple, lang: java} |
+-----------------------------------------------------------------------------+
```

## Filtrer les Nœuds avec des Conditions

En plus des contraintes de label, vous pouvez spécifier des conditions de filtrage basées sur les propriétés.

```cypher
MATCH (p:person {name: 'marko'}) RETURN p;
```

output:
```
+-------------------------------------------------------+
| p                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, name: marko, age: 29} |
+-------------------------------------------------------+
```

## Filtrer les Relations

### Requête de relations avec un seul label

```cypher
MATCH (p:person)-[k:knows]->(f:person) RETURN k;
```

Résultat :
```
+------------------------------------------------------------------------------------------------------+
| k                                                                                                    |
+======================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5} |
+------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0} |
+------------------------------------------------------------------------------------------------------+
```

### Requête de relations avec plusieurs labels

```cypher
MATCH (p:person)-[k:knows|created]->(f) RETURN k;
```

Résultat :
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

### Rechercher les relations avec n'importe quelle étiquette

```cypher
MATCH (p:person)-[k]->(f) RETURN k;
```

Résultat :
```
+--------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                    |
+======================================================================================================================================+
| {_ID: 1, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 1, weight: 0.5}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}                                 |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103806595072, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 0, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692224, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927936, weight: 0.4} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103808692225, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937, weight: 1.0} |
+--------------------------------------------------------------------------------------------------------------------------------------+
| {_ID: 1103809740800, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 3, _DST_ID: 72057594037927936, weight: 0.2} |
+--------------------------------------------------------------------------------------------------------------------------------------+
```

## Filtrer les relations avec des conditions

Filtre les relations en fonction de leurs propriétés.

```cypher
MATCH (p:person)-[k:knows {weight: 1.0}]->(f:person) RETURN k;
```

Résultat :
```
+------------------------------------------------------------------------------------------------------+
| k                                                                                                    |
+======================================================================================================+
| {_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0} |
+------------------------------------------------------------------------------------------------------+
```

## Explorer les chemins répétés

Neug prend en charge l'exploration de chemins répétés de longueur variable, une fonctionnalité courante dans les requêtes graphes.

### Matcher des chemins répétés avec une longueur variable

Rechercher des chemins avec un nombre variable de hops. Cette requête retourne tous les chemins composés de 1 ou 2 relations.

```cypher
MATCH (p:person)-[k*1..2]->(f) RETURN k;
```

<!-- todo: output is incorrect -->

### Matcher des chemins répétés avec des conditions sur la source

Spécifier des conditions de filtrage basées sur les propriétés du nœud source. Cette requête trouve les chemins de 1 ou 2 hops partant du nœud ayant le nom 'marko'.

```cypher
MATCH (p:person {name: 'marko'})-[k*1..2]->(f) RETURN k;
```

<!-- todo: output is incorrect -->

### Faire correspondre les chemins répétés avec des conditions cibles

Spécifiez des conditions de filtrage basées sur les propriétés du nœud cible. Cette requête trouve les chemins se terminant au nœud ayant pour nom 'josh'.

```cypher
MATCH (p:person {name: 'marko'})-[k*1..2]->(f {name: 'josh'}) RETURN k;
```

output:
```
+---------------------------------------------------------------------------------------------------------------------------------------------------+
| k                                                                                                                                                 |
+===================================================================================================================================================+
| {_ID: 2, _LABEL: person}, {_ID: 2097152, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 2, _DST_ID: 0}, {_ID: 0, _LABEL: person} |
+---------------------------------------------------------------------------------------------------------------------------------------------------+
```

### Correspondance des chemins répétés avec conditions sur les arêtes

Selon la [spécification de Kuzu](https://docs.kuzudb.com/cypher/query-clauses/match/#filter-recursive-relationships), Neug prend également en charge le filtrage par propriétés sur chaque arête durant l'expansion du chemin.

Cette requête impose à chaque arête du chemin de satisfaire la contrainte `r.weight < 1.0`.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows*1..2 (r, _ | WHERE r.weight <= 1.0)]->(f:person)
Return k;
```

<!-- todo: output is incorrect -->

### Correspondance des chemins sans répétition d'arêtes (Trail Path)

En utilisant l'option `TRAIL`, vous pouvez restreindre davantage les chemins répétés pour garantir qu'aucune arête n'est visitée plusieurs fois, assurant ainsi que l'expansion du chemin se termine sans boucle infinie.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows* TRAIL 1..2]->(f:person)
Return k;
```

### Match Simple Path

En utilisant l'option `SIMPLE`, vous pouvez restreindre davantage les chemins répétés pour vous assurer qu'aucun nœud n'est répété, garantissant ainsi la sortie de chemins simples.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows* ACYCLIC 1..2]->(f:person)
Return k;
```

### Match Unweighted Shortest Path

Spécifiez l'option `SHORTEST` pour obtenir le chemin le plus court non pondéré entre deux nœuds donnés.

```cypher
MATCH (p:person {name: 'marko'})-[k:knows* SHORTEST 1..2]->(f:person {name: 'josh'})
RETURN k;
```

## Match Patterns

La clause `MATCH` prend en charge le pattern matching complexe qui combine les nœuds, les relations et les conditions de différentes manières pour exprimer des requêtes graphiques sophistiquées.

Voici quelques patterns classiques de requêtes graphiques largement utilisés dans divers benchmarks de requêtes graphiques :
- Triangle Pattern
```
Match (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(c:person)
Where a.name <> b.name AND b.name <> c.name
Return count(*);
```

- Square Pattern
```
Match (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(d:person),
      (c:person)<-[:knows]-(d:person)
Where a.name <> b.name AND b.name <> c.name AND c.name <> d.name
Return count(*);
```

- Long Path
```
Match (a:person)-[:knows]->(b:person),
      (b:person)-[:knows]->(c:person),
      (c:person)-[:created]->(d:software),
      (d:software)<-[:created]-(e:person),
      (e:person)-[:knows]->(f:person)
Where a.name <> b.name AND b.name <> c.name 
    AND c.name <> d.name AND d.name <> e.name
Return count(*);
```

- Clique Path
```
Match (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(d:person),
      (c:person)<-[:knows]-(d:person),
      (a:person)-[:knows]->(c:person),
      (d:person)-[:created]->(b:software)
Where a.name <> b.name AND b.name <> c.name AND c.name <> d.name
Return count(*);
```

## Optional Match

La clause `OPTIONAL MATCH` vous permet de matcher des patterns qui peuvent ou non exister dans le graphe, en retournant `null` pour les parties du pattern qui ne matchent pas.

Voici comment utiliser Optional Match :

```cypher
MATCH (a:person)-[:knows]->(b:person)
OPTIONAL MATCH (b:person)-[:created]->(c:software)
RETURN a.name, b.name, c.name
```

<!-- todo: la fonctionnalité n'est pas incluse dans le package pip actuel -->

Dans les résultats de sortie ci-dessus, pour chaque paire (a, b) :
- Si b a des nœuds connectés c, tous les triplets (a, b, c) sont retournés. Par exemple, pour ('marko', 'josh'), les triplets correspondants sont {('marko', 'josh', 'lop'), ('marko', 'josh', 'ripple')}.
- Si b n'a pas de nœuds connectés c, une ligne avec c=null est préservée pour la paire (a,b) courante. Par exemple, pour ('marko', 'vadas'), le triplet de sortie est {('marko', 'vadas', null)}.

C'est là la principale utilité de la clause OPTIONAL MATCH - de préserver les lignes du MATCH principal même lorsque le pattern optionnel ne matche pas.