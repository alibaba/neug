# Clause DML

Le DML (Data Manipulation Language) fournit des opérations pour l'insertion, la suppression et la modification de données dans les bases de données graphes. Neug supporte à la fois les opérations de données en masse (comme COPY FROM) et les opérations individuelles (comme CREATE, SET et DELETE). Ce document fournit des exemples et des explications pour chaque type d'opération.

## COPY FROM

La commande COPY FROM vous permet de charger des données en masse depuis des sources de données externes et de construire des vertex et des edges dans le stockage graphe. Les sources de données actuellement supportées incluent les formats CSV.

### Chargement des données de vertex

Charge les données de vertex person depuis un fichier CSV. Chaque ligne du CSV correspond à un vertex, avec les colonnes correspondant aux propriétés du vertex définies dans le schéma person.

**person.csv:**
```
name,age
marko,39
vadas,27
josh,32
peter,35
```

**Commande:**
```cypher
COPY person FROM "person.csv"
```

### Chargement des données d'arêtes

Load récupère les données d'arêtes depuis un fichier CSV. Les deux premières colonnes spécifient les clés primaires des sommets source et cible, tandis que les colonnes supplémentaires définissent les propriétés des arêtes.

**knows.csv :**
```
src_name,dst_name,weight
marko,josh,1.0
marko,vadas,0.5
josh,peter,0.8
```

**Commande :**
```cypher
COPY knows FROM "knows.csv"
```

## CREATE

La clause CREATE est utilisée pour insérer de nouveaux sommets et relations dans le graphe.

### Création de sommets

Crée de nouveaux sommets avec les propriétés spécifiées. Si un sommet avec la même clé primaire existe déjà, une erreur sera signalée.

```cypher
CREATE (a:person {name: 'taylor', age: 25}), (b:person {name: 'julie', age: 30})
```

### Création de sommets et de relations

Crée des sommets et des relations en une seule instruction. Cela est utile lorsque vous devez créer à la fois les sommets et la relation entre eux.

```cypher
CREATE (a:person {name: 'mars', age: 28})-[:knows {weight: 16.0}]->(b:person {name: 'jennie', age: 26})
```

### Création de relations entre des sommets existants

Commencez par faire correspondre les sommets existants, puis créez une relation entre eux.

```cypher
MATCH (a:person {name: 'taylor'}), (b:person {name: 'julie'})
CREATE (a)-[:knows {weight: 20.0}]->(b)
```

## SET

La clause SET est utilisée pour mettre à jour les propriétés des sommets et des relations existants.

### Mise à jour des propriétés d'un sommet

Mettez à jour les propriétés d'un sommet spécifique.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
SET a.age = 37, a.city = 'New York'
RETURN a.*
```

### Mise à jour des propriétés d'une relation

Mettez à jour les propriétés d'une relation spécifique.

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
SET k.weight = 10.0, k.since = '2023-01-01'
RETURN k.*
```

## DELETE

La clause DELETE est utilisée pour supprimer des sommets et des relations du graphe.

### Suppression de sommets

Supprimez un sommet du graphe. Par défaut, vous ne pouvez supprimer que les sommets qui n'ont aucune relation pour éviter de créer des arêtes orphelines.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DELETE a
```

### Suppression de sommets avec relations (DETACH DELETE)

Utilisez DETACH DELETE pour forcer la suppression d'un sommet et de toutes ses relations. Cela permet d'éviter les erreurs lors de la suppression de sommets ayant des relations existantes.

```cypher
MATCH (a:person)
WHERE a.name = 'marko'
DETACH DELETE a
```

### Suppression de relations

Supprimez des relations spécifiques entre des sommets tout en conservant les sommets intacts.

```cypher
MATCH (a:person)-[k:knows]->(b:person)
WHERE a.name = 'marko' AND b.name = 'josh'
DELETE k
```