# Clause Limit

La clause Limit est utilisée pour contrôler le nombre de résultats en sortie. En plus d'être utilisée seule, elle peut également être combinée avec Order BY pour réaliser des opérations de type TopK. Neug supporte actuellement deux types d'expressions dans Limit : 1. Une constante entière simple. 2. Des expressions arithmétiques ne contenant que des paramètres constants.

## Limit avec une valeur entière

```
Match (a:person)
Return a.age
Limit 2;
```
Puisqu'il n'y a pas d'ordre spécifié pour la sortie, le résultat peut correspondre à n'importe quels deux enregistrements.

output:
```
+------------+
|   _0_a.age |
+============+
|         29 |
+------------+
|         27 |
+------------+
```

## Limit avec une expression entière

```
Match (a:person)
Return a.age
Limit 1+1;
```

output:
```
+------------+
|   _0_a.age |
+============+
|         29 |
+------------+
|         27 |
+------------+
```

# Clause Skip

La clause `Limit` contrôle le nombre de résultats en sortie, ce qui équivaut à définir l'intervalle des numéros de ligne des résultats comme [0, borne_supérieure). La clause `Skip` permet de sauter les premières lignes des résultats en sortie, ce qui revient à définir l'intervalle des numéros de ligne des résultats comme [borne_inférieure, +∞), en supposant que les numéros de ligne commencent à 0.

## Skip avec une valeur entière

```
Match (a:person)
Return a.age
Skip 2;
```
Cette requête est utilisée pour ignorer les deux premières lignes des résultats.

Résultat :
```
+------------+
|   _0_a.age |
+============+
|         32 |
+------------+
|         35 |
+------------+
```

## Skip avec une expression entière

```
Match (a:person)
Return a.age
Skip 1+1;
```

Résultat :
```
+------------+
|   _0_a.age |
+============+
|         32 |
+------------+
|         35 |
+------------+
```