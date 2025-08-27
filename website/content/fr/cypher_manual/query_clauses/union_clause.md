# Clause Union
L'opérateur Union dans Neug est utilisé pour combiner les résultats de plusieurs sous-requêtes en un seul ensemble de résultats. Toutes les sous-requêtes participantes doivent produire un schéma de sortie cohérent—c'est-à-dire le même nombre de colonnes avec des noms et des types de données correspondants.

Actuellement, Neug supporte la variante `UNION ALL`, qui concatène les résultats sans effectuer de déduplication. Deux formes syntaxiques sont disponibles :
- **Union standard** : Similaire à la syntaxe standard dans [Kùzu](https://docs.kuzudb.com/cypher/query-clauses/union/).
- **Call Union** : Une forme étendue inspirée de [Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union), permettant une composition de requêtes plus flexible.

## Union standard

Dans l'usage standard, `UNION ALL` est utilisé pour fusionner la sortie de plusieurs sous-requêtes. L'union doit apparaître comme opérateur terminal, combinant les sorties de toutes les branches précédentes.

```cypher
MATCH (n {name: 'marko'}) RETURN n.age
UNION ALL
MATCH (n {name: 'josh'}) RETURN n.age;
```

## Call Union

Inspiré par [Neo4j](https://neo4j.com/docs/cypher-manual/current/subqueries/call-subquery/#call-post-union), Neug étend la sémantique des unions via un bloc `CALL {}` avec des entrées paramétrées, permettant une composition de requêtes plus expressive et modulaire. Cette construction permet :
- D'exécuter une logique supplémentaire après l'union.
- De partager un contexte précalculé (ex. : variables liées) entre les branches de l'union.

Exemple :
```cypher
MATCH (person:person {id: 123})
WITH person
CALL (person) {
  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight > 1.0
  RETURN friend

  UNION ALL

  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight < 1.0
  RETURN friend
}
RETURN friend.id, friend.name
```

Cette requête peut être décomposée en trois étapes :
- **PreQuery** : Exécutée avant le bloc `CALL {}` (ex. : MATCH (person)), effectue un contexte précalculé qui sera partagé entre les sous-requêtes de l'union.
- **Union Subqueries** : Définies dans le bloc `CALL {}`. Chaque branche a accès au contexte partagé (ex. : person).
- **PostQuery** : Exécutée après le `CALL {}`, consomme l'ensemble de résultats unifié (ex. : RETURN friend.id, friend.name).

La syntaxe `CALL (person)` injecte des variables externes dans la portée de l'union, permettant à chaque sous-requête d'accéder et d'opérer sur un contexte partagé. Ce pattern est particulièrement utile lorsqu'on applique plusieurs stratégies de filtrage ou de traversée sur la même entité d'entrée.