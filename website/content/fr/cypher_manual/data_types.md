# Types de données Neug

Ce document fournit une vue d'ensemble complète de tous les types de données pris en charge par Neug.

## Tableau récapitulatif des types de données

Le tableau suivant présente tous les types de données pris en charge par Neug et leurs différences avec Neo4j.

| Catégorie | Type | Exemple Neug | Exemple Neo4j |
|----------|------|--------------|---------------|
| Primitive | INT32 | `Return CAST(42, 'INT32')` | `Return 42` |
| Primitive | UINT32 | `Return CAST(42, 'UINT32')` | non supporté |
| Primitive | INT64 | `Return 9223372036854775807` | `Return 9223372036854775807` |
| Primitive | UINT64 | `Return CAST(9223372036854775807, 'UINT64')` | non supporté |
| Primitive | FLOAT | `Return CAST(3.14, 'FLOAT')` | `Return 3.14f` |
| Primitive | DOUBLE | `Return 3.14159265359` | `Return 3.14159265359d` |
| Primitive | BOOL | `Return true` | `Return true` |
| Primitive | NULL | `Return null` | `Return null` |
| String | VARCHAR | `Return 'Hello World'` | `Return 'Hello World'` |
| Temporal | DATE | `Return date('2022-06-06')` | `Return date('2022-06-06')` |
| Temporal | DATETIME | `Return timestamp('2022-06-06 12:00:00')` | `Return datetime('2022-06-06T12:00:00')` |
| Temporal | INTERVAL | `Return interval('1 year 2 days')` | `Return duration('P1Y2D')` |
| Composite | LIST | `Return [1, 2, 3]` | `Return [1, 2, 3]` |
| Pattern | NODE | `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}` | `(:person {name: 'Alice', age: 30})` |
| Pattern | REL | `{_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}` | `[:knows {weight: 1.0}]` |
| Pattern | REPEATED PATH | `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}` | `(:Person {name: "Kiefer", id: 4, age: 1992})-[:FOLLOWS]->(:Person {name: "Jack", id: 3, age: 1979})-[:FOLLOWS]->(:Person {name: "Kevin", id: 5, age: 1997})` |

## Introduction détaillée

### Types primitifs

#### INT32
- **Description** : Type d'entier signé 32 bits
- **Plage** : [2,147,483,648, 2,147,483,647]
- **Exemple de requête** : `Return CAST(42, 'INT32') as int32_value;`

#### UINT32
- **Description** : Type d'entier non signé 32 bits
- **Plage** : [0, 4,294,967,295]
- **Exemple de requête** : `RETURN CAST(42, 'UINT32') AS uint32_value;`

#### INT64
- **Description** : Type d'entier signé 64 bits, type par défaut des valeurs entières
- **Plage** : [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]
- **Exemple de requête** : `RETURN 9223372036854775807 AS int64_value;`

#### UINT64
- **Description** : Type d'entier non signé 64 bits
- **Plage** : [0, 18,446,744,073,709,551,615]
- **Exemple de requête** : `RETURN CAST(18446744073709551615, 'UINT64') AS uint64_value;`

#### FLOAT
- **Description** : Nombre à virgule flottante simple précision
- **Précision** : ~7 chiffres décimaux
- **Exemple de requête** : `RETURN CAST(3.14, 'FLOAT') AS float_value;`

#### DOUBLE
- **Description** : Nombre à virgule flottante double précision, type par défaut des valeurs float
- **Précision** : ~15-17 chiffres décimaux
- **Exemple de requête** : `RETURN 3.14159265359 AS double_value;`

#### BOOL
- **Description** : Type booléen représentant les valeurs true ou false
- **Valeurs** : `true`, `false`
- **Exemple de requête** : `RETURN true AS bool_value;`

#### NULL
- **Description** : Représente les valeurs manquantes ou non définies
- **Exemple de requête** : `RETURN null AS null_value;`

### Types String

#### VARCHAR
- **Description** : Chaîne de caractères de longueur variable avec encodage UTF-8
- **Exemple de requête** : `RETURN 'Hello World' AS string_value;`
- **Longueur** : Variable, limitée par les contraintes du système, la valeur par défaut est `65536`

### Types Temporels

#### DATE
- **Description** : Type date pour stocker les dates du calendrier
- **Format** : YYYY-MM-DD
- **Exemple de requête** : `RETURN date('2022-06-06') AS date_value;`

#### DATETIME
- **Description** : Type combinant date et heure
- **Format** : YYYY-MM-DD HH:MM:SS
- **Query Example** : `RETURN timestamp('2022-06-06 12:00:00') AS datetime_value;`

#### INTERVAL
- **Description** : Type représentant une durée ou un intervalle de temps, suivant [la spécification de kuzu]().
- **Query Example** : `RETURN interval('1 year 2 days') AS interval_value;`

### Composite Types

#### LIST
- **Description** : Collection ordonnée de valeurs avec des types hétérogènes
- **Exemple de requête** : `RETURN [1, 2, 3] AS list_value;`

Le tableau suivant montre tous les types de composants que LIST peut supporter :

| Catégorie | Type | Exemple |
|-----------|------|---------|
| Numérique | INT32, INT64, UINT32, UINT64, DOUBLE, FLOAT | `RETURN [1, 2, 3.0];` |
| Chaîne de caractères | VARCHAR | `RETURN ['marko', 'josh'];` |
| Date | DATE, DATETIME | `RETURN [date('2011-01-25'), timestamp('2011-01-25 11:20:33')];` |
| Booléen | BOOL | `RETURN [true, false];` |
| Composite | LIST | `RETURN [[1, 2], [4, 5]];` |

**Note importante sur les types de composants de LIST** :

Neug prend en charge les listes via les types de données tuple, ce qui signifie que les types composites peuvent être hétérogènes. Voici quelques exemples :

Mélange de différents types primitifs dans une seule liste :
```cypher
RETURN ['marko', 2];
```

Combinaison de différents types de propriétés de nœuds dans une liste :
```cypher
MATCH (n:person) RETURN [n.name, n.age];
```

Support des structures de listes imbriquées :
```cypher
MATCH (n:person) RETURN [["name", n.name], ["age", n.age]];
```

**Détails techniques clés :**
- Les listes dans Neug peuvent contenir des éléments de différents types de données (listes hétérogènes)
- Cela est réalisé grâce au support interne des types de données tuple
- La conversion de type est gérée automatiquement lorsque c'est possible
- Les listes imbriquées sont entièrement prises en charge pour les structures de données complexes
- Le système maintient la sécurité des types tout en permettant une flexibilité dans la composition des listes

### Types de Patterns

#### NODE
- **Description** : Représente un vertex dans le graphe
- **Structure interne** : Contient `_ID` (identifiant interne), `_LABEL` (label du node) et les champs de propriétés
- **Exemple de requête** : `MATCH (n:person) RETURN n AS node_value;`
- **Format Neug** : `{_ID: 0, _LABEL: person, id: 1, name: marko, age: 29}`

#### REL (Relationship)
- **Description** : Représente une arête dans le graphe
- **Structure interne** : Contient `_SRC` (ID et LABEL du node source), `_DST` (ID et LABEL du node de destination), `_ID` (identifiant interne de la relationship), `_LABEL` (type de relationship) et les champs de propriétés
- **Exemple de requête** : `MATCH ()-[r:knows]->() RETURN r AS rel_value;`
- **Format Neug** : `{_ID: 2, _LABEL: knows, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2, weight: 1.0}`

#### CHEMIN RÉPÉTÉ
- **Description** : Représente un chemin composé d'arêtes répétées dans le graphe
- **Structure interne** : Contient chaque nœud et relation ainsi que le chemin, y compris les nœuds de départ et de destination
- **Exemple de requête** : `MATCH (a:person)-[p*1..2]->(c) RETURN p AS path_value;`
- **Format Neug** : `{_ID: 0, _LABEL: person}, {_ID: 4294967298, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: person, _SRC_ID: 0, _DST_ID: 2}, {_ID: 2, _LABEL: person}, {_ID: 4297064449, _LABEL: created, _SRC_LABEL: person, _DST_LABEL: software, _SRC_ID: 2, _DST_ID: 72057594037927937}, {_ID: 72057594037927937, _LABEL: software}`