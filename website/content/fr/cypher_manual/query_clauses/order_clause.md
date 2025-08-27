# Clause Order

Order est utilisé pour trier les résultats actuels en fonction de propriétés afin d'assurer une sortie déterministe. Nous prenons actuellement en charge deux options de tri : `ASC` pour l'ordre croissant et `DESC` pour l'ordre décroissant. Si non spécifié explicitement, l'ordre croissant est utilisé par défaut. Il est important de noter que Order peut être utilisé en combinaison avec Limit, ce qui équivaut à une opération TopK. Nous allons introduire les modèles d'utilisation courants suivants.

## Tri par une seule propriété

```
Match (a)
Return a.name
ORDER BY a.name ASC;
```

output:
```
+-------------+
| _0_a.name   |
+=============+
| josh        |
+-------------+
| lop         |
+-------------+
| marko       |
+-------------+
| peter       |
+-------------+
| ripple      |
+-------------+
| vadas       |
+-------------+
```

## Tri par plusieurs propriétés

```
Match (a)-[b]->(c)
Return a.name, b.weight
ORDER BY a.name ASC, b.weight ASC;
```

Résultat :
```
+-------------+---------------+
| _0_a.name   |   _4_b.weight |
+=============+===============+
| josh        |           0.4 |
+-------------+---------------+
| josh        |           1   |
+-------------+---------------+
| marko       |           0.4 |
+-------------+---------------+
| marko       |           0.5 |
+-------------+---------------+
| marko       |           1   |
+-------------+---------------+
| peter       |           0.2 |
+-------------+---------------+
```

## Tri par expressions

En plus du tri direct par propriétés, la clause ORDER BY peut également utiliser des expressions plus complexes comme clé de tri, telles que les résultats d'opérations arithmétiques, les retours de fonctions scalaires, etc.

### Tri par Expression Pré-calculée

```
Match (a)-[b]->(c)
Return a.age, c.name
ORDER BY a.age + 10 ASC, c.name;
```

résultat :
```
+------------+-------------+
|   _0_a.age | _2_c.name   |
+============+=============+
|         29 | josh        |
+------------+-------------+
|         29 | lop         |
+------------+-------------+
|         29 | vadas       |
+------------+-------------+
|         32 | lop         |
+------------+-------------+
|         32 | ripple      |
+------------+-------------+
|         35 | lop         |
+------------+-------------+
```

### Tri par Résultats de Fonctions Scalaires

```
Match (a)-[b]->(c)
Return a.name, c.name
Order BY label(a);
```

<!-- todo: la fonction label n'est pas incluse dans le package pip actuel -->

Pour plus d'opérations avec les fonctions scalaires, consultez la [section Fonctions](../expression.md).

## Tri avec Limit

De plus, dans les scénarios de requêtes BI (Business Intelligence), TopK est l'une des opérations les plus courantes, tronquant et retournant uniquement les résultats les plus significatifs. Neug supporte également ce type de requêtes.

```
Match (a)-[b]->(c)
Return a.age, c.name
ORDER BY a.age + 10 ASC, c.name ASC
Limit 2;
``` 

Résultat :
```
+------------+-------------+
|   _0_a.age | _2_c.name   |
+============+=============+
|         29 | josh        |
+------------+-------------+
|         29 | lop         |
+------------+-------------+
```