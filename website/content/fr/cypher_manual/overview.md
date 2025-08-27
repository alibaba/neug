# Aperçu de la syntaxe

## Qu'est-ce que Cypher ?

Cypher est un langage de requête graphique déclaratif conçu spécifiquement pour les bases de données graphiques. Il fournit une manière intuitive et expressive de requêter, manipuler et gérer les données graphiques. Notre implémentation est basée sur la spécification [OpenCypher](https://opencypher.org/), qui est un standard ouvert pour les langages de requête graphiques.

## Différences clés par rapport à SQL

Alors que SQL est conçu pour les bases de données relationnelles avec des tables et des lignes, Cypher est optimisé pour les bases de données graphiques avec des nœuds, des relations et des propriétés :

- **Structure** : SQL utilise des tables et des jointures ; Cypher utilise des nœuds, des relations et des motifs
- **Pattern Matching** : SQL nécessite des jointures explicites ; Cypher utilise une syntaxe de pattern matching
- **Traversal** : SQL nécessite des jointures complexes pour les requêtes multi-sauts ; Cypher supporte naturellement le parcours de chemins
- **Lisibilité** : La syntaxe ASCII art de Cypher rend les motifs graphiques visuellement intuitifs

## Que pouvez-vous faire avec Cypher dans Neug ?

Dans Neug, nous appelons une requête Cypher un **Statement**. Un Statement est composé de plusieurs **Clauses**. Par exemple, dans la requête suivante :

```cypher
MATCH (p:person)
WHERE p.age = '29'
RETURN p.name as name;
```

Les composants `MATCH`, `WHERE`, et `RETURN` sont appelés des Clauses, qui sont les unités logiques fondamentales pour les opérations de base de données graphe.

Basé sur OpenCypher, nous avons défini une série de syntaxes de Statement pour gérer la base de données graphe de Neug, incluant :

### Gestion du Schéma (DDL)

Neug cible principalement les scénarios de données graphiques avec schéma strict, où chaque donnée doit se conformer à des spécifications de schéma prédéfinies. Cela ressemble au SQL traditionnel ; cependant, les données graphiques impliquent des structures de nœuds et de relations plus complexes qui doivent également respecter les exigences de schéma prédéfinies.

Par exemple, considérons le schéma graphique suivant :

<img src="figures/modern_schema.png" alt="Modern Schema Graph" style="display: block; margin: 2em auto; max-width: 500px;">

Le schéma graphique ci-dessus peut être créé à l’aide des instructions suivantes :

```cypher
// Exemple de définition de schéma
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);

CREATE NODE TABLE software (
    name STRING,
    lang STRING,
    PRIMARY KEY (name)
);

CREATE REL TABLE knows (
    FROM person TO person,
    weight DOUBLE
);

CREATE REL TABLE created (
    FROM person TO software,
    weight DOUBLE
);
```

**Requête conforme au schéma :**  
Dans la requête suivante, l’étiquette de sommet `person` et l’étiquette d’arête `(person-knows->person)` respectent toutes deux les contraintes de schéma définies ci-dessus. Le nœud `person` contient les propriétés `age` et `name`, et la propriété `age` est de type INT32, ce qui permet la comparaison avec la constante 18. Par conséquent, cette requête satisfait toutes les contraintes du schéma et est valide :

```cypher
MATCH (p:person)-[:knows]->(f:person)
WHERE p.age > 18
RETURN p.name, f.name;
```

**Requête non conforme au schéma (échouerait) :**  
L’étiquette d’arête `(person-follows->person)` spécifiée dans cette requête n’existe pas dans le schéma, ce qui la rend invalide et entraîne une erreur de type "Table `follows` does not exist".

```cypher
MATCH (p:person)-[:follows]->(m:person)
RETURN p.name;
```

Nous définissons un ensemble de syntaxes pour créer des schémas graphiques comme illustré ci-dessus, que nous appelons DDL (Data Definition Language). Toutes les opérations ultérieures de mise à jour et de requêtage des données doivent se conformer aux spécifications de schéma définies par le DDL actuel. Nous détaillerons cela dans la [section DDL](ddl_clause.md).

### Data Query (DQL)

Nous définissons également un ensemble de syntaxes de requête capable de satisfaire à la fois les besoins de requêtes de traitement transactionnel (TP) et de traitement analytique (AP).

Par exemple, vous pouvez interroger tous les motifs triangulaires dans la base de données graphe à l'aide de la requête suivante :

```cypher
MATCH (a:person)-[:created]->(b:software),
      (c:person)-[:created]->(b:software),
      (a:person)-[:knows]->(c:person)
WHERE a.name < c.name
RETURN a.name, b.name, c.name;
```

Nous appelons chaque `MATCH`, `WHERE` et `RETURN` une Clause, qui constitue l'unité de base des opérations sur les données graphe. Ici, l'opération `MATCH` correspond principalement à la recherche de toutes les données formant un motif triangulaire, `WHERE` filtre davantage les données du motif pour garantir l'absence de doublons, et l'opération `RETURN` effectue la projection des noms et retourne les résultats finaux. L'opération `MATCH` réalise principalement le matching de motifs graphe, tandis que les opérations `WHERE`/`RETURN` effectuent principalement des opérations relationnelles similaires à SQL. Ces clauses seront présentées en détail dans la [section DQL](query_clauses.md).

Pour garantir davantage la validité des opérations Clause sur les données, nous avons défini les limites des types de données pris en charge par Neug, ainsi que les opérations d'expression basées sur ces types de données. Ces éléments seront présentés en détail dans les sections [Types de données](data_types.md) et [Expressions](expression.md).

### Gestion des données (DML)

En plus du DQL et du DDL, Neug prend également en charge les fonctionnalités de mise à jour des données, que nous appelons DML (Data Manipulation Language). Les opérations DML peuvent être effectuées via des chargements en masse ou des mises à jour incrémentales.

**Exemple d'import en masse :**
```cypher
COPY person FROM `person.csv`;
COPY knows FROM `knows.csv`;
```

Les deux instructions ci-dessus chargent d'abord en masse les données de nœuds avec le label `person` depuis le fichier person.csv, puis chargent en masse les données d'arêtes avec le label `person-[knows]->person` depuis le fichier knows.csv.

**Exemple de mise à jour incrémentale :**

Nous fournissons également une syntaxe d'écriture incrémentale pour mettre à jour progressivement les données du graphe.

**Exemple de création de nœud :**
```cypher
CREATE (p:person {name: 'Bob', age: 30});
```

**Exemple de création de relation :**
```cypher
MATCH (a:person {name: 'Bob'}), (b:person {name: 'marko'})
CREATE (a)-[:knows {weight: 3.0}]->(b);
```

**Exemple de suppression de nœud :**
```cypher
MATCH (p:person {name: 'Bob'})
DELETE p;
```

Nous présenterons ces opérations DML en détail dans la [section DML](dml_clause.md).