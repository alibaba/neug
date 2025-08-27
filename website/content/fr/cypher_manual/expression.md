# Expression

## Aperçu

Les expressions sont des composants fondamentaux dans Cypher qui vous permettent de calculer des valeurs, d'effectuer des opérations et de manipuler des données au sein des requêtes. Contrairement à SQL traditionnel qui se concentre sur les opérations relationnelles, les expressions Cypher sont spécifiquement conçues pour les structures de données en graphe et offrent des capacités puissantes pour traverser les relations, faire du pattern matching et effectuer des calculs spécifiques aux graphes.

Les expressions Cypher remplissent plusieurs objectifs clés :

- **Calcul de valeurs** : Calculer des valeurs dérivées à partir des propriétés des nœuds, des attributs des relations ou de résultats calculés
- **Évaluation de conditions** : Créer des expressions booléennes pour filtrer les nœuds, les relations ou les chemins
- **Transformation de données** : Convertir et formater les types de données pour l'affichage ou un traitement ultérieur
- **Traversée de graphe** : Naviguer à travers les structures de graphe en utilisant des expressions de chemin et du pattern matching
- **Agrégation** : Effectuer des calculs sur des collections de nœuds ou de relations
- **Traitement de chaînes et de texte** : Manipuler les données textuelles pour la recherche, l'affichage ou l'analyse

Les expressions Cypher sont construites à partir de deux catégories principales de composants :

1. **Opérateurs** : Symboles et mots-clés qui effectuent des opérations basiques sur des opérandes
2. **Fonctions** : Opérations prédéfinies qui encapsulent une logique complexe et prennent des paramètres

Ces composants travaillent ensemble pour créer des expressions puissantes capables de gérer aussi bien des calculs simples que des opérations complexes sur des graphes.

## Opérateurs

Les opérateurs dans Neug sont des symboles ou des mots-clés qui effectuent des opérations sur des opérandes. Ils constituent les blocs de base fondamentaux pour construire des expressions et des requêtes.

### Types d'opérateurs supportés

| Type | Description |
|------|-------------|
| [Comparaison](./expression/comparison_op.md) | Opérateurs pour comparer des valeurs (ex : `=`, `<>`, `<`, `>`, `<=`, `>=`) |
| [Logique](./expression/logical_op.md) | Opérateurs booléens pour combiner des conditions (ex : `AND`, `OR`, `NOT`) |
| [Arithmétique](./expression/arithmetic_op.md) | Opérations mathématiques (ex : `+`, `-`, `*`, `/`, `%`) |
<!-- | Bit | Opérations bit à bit (ex : `&`, `|`, `^`, `<<`, `>>`) | -->
| [Null](./expression/null_op.md) | Opérations pour gérer les valeurs null (ex : `IS NULL`, `IS NOT NULL`) |
| [Liste](./expression/list_op.md) | Opérations pour travailler avec des structures de données de type liste (ex : `IN`) |
<!-- | Case When | Expressions conditionnelles utilisant la syntaxe `CASE WHEN` | -->

## Fonctions

Les fonctions dans Neug sont des opérations prédéfinies qui prennent des paramètres d'entrée et retournent des valeurs calculées. Elles fournissent des fonctionnalités spécialisées pour la manipulation et l'analyse des données.

### Catégories de fonctions supportées

| Catégorie | Description |
|----------|-------------|
| [Agrégation](./expression/agg_func.md) | Fonctions qui opèrent sur des collections de valeurs et retournent un résultat unique (ex. `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`) |
| [Cast](./expression/cast_func.md) | Fonctions pour convertir les types de données entre différents formats |
| [Temporel](./expression/temporal_func.md) | Fonctions pour travailler avec les données de date et d'heure |
| [Motif de graphe](./expression/graph_func.md) | Fonctions spécialement conçues pour les nœuds, les relations ou les chemins |
<!-- | Texte | Fonctions de manipulation de chaînes de caractères et de traitement de texte |
| Numérique | Fonctions de calcul mathématique et numérique | -->