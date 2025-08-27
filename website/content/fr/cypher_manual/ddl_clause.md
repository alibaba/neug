# Clause DDL

Le DDL (Data Definition Language) est un ensemble d'opérations spécifiquement conçues pour la gestion du schéma. Neug prend en charge les opérations d'ajout, de suppression et de modification des nœuds, arêtes et propriétés du schéma. Veuillez vous référer aux exemples d'utilisation suivants.

## Créer un Type de Nœud

Créer un nœud avec le type Label "person", en spécifiant les noms des propriétés, les types et la clé primaire pour la personne.

```
CREATE NODE TABLE person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

Par défaut, si un type person existe déjà dans la base de données, une erreur sera signalée. Utilisez IF NOT EXISTS pour éviter les erreurs - cela créera uniquement si le type n'existe pas dans la base de données, sinon cela ne fera rien.

```
CREATE NODE TABLE IF NOT EXISTS person (
    name STRING,
    age INT32,
    PRIMARY KEY (name)
);
```

## Créer un type Rel

Créez une arête de type "knows" entre deux personnes, en spécifiant les noms et types de propriétés pour "knows". Actuellement, les arêtes ne permettent pas de spécifier de clés primaires.

```
CREATE REL TABLE IF NOT EXISTS knows (
    FROM person TO person,
    weight DOUBLE
);
```

## Supprimer un type Node

Supprimez un type de Node spécifié. Utilisez IF EXISTS pour éviter les erreurs lorsque le type n'existe pas.

```
DROP TABLE IF EXISTS person;
```

## Supprimer un type Rel

Supprimez un type de Relationship spécifié. Utilisez IF EXISTS pour éviter les erreurs lorsque le type n'existe pas.

```
DROP TABLE IF EXISTS knows;
```

## Renommer un type Node ou Rel

Renommez un type de node ou de relationship avec `RENAME TO`.

```
ALTER TABLE person RENAME TO person2;
ALTER TABLE knows RENAME TO knows2;
```

## Ajouter une propriété

Ajoutez des propriétés à un type de node ou de relationship.

```
ALTER TABLE person ADD IF NOT EXISTS gender INT32;
ALTER TABLE knows ADD IF NOT EXISTS info STRING;
```

## Drop Property

Supprime des propriétés d'un type de nœud ou de relation.

```
ALTER TABLE person DROP IF EXISTS gender;
ALTER TABLE knows DROP IF EXISTS info;
```

## Rename Property

Renomme des propriétés d'un type de nœud ou de relation.

```
ALTER TABLE person RENAME age TO age2;
ALTER TABLE knows RENAME weight TO weight2;
```