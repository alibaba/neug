# Opérateurs de comparaison

Nous présentons les différents opérateurs de comparaison supportés par Neug dans le tableau suivant.

Opérateur | Description | Exemple | Résultat
---------|-------------|---------|--------
`=` | Égal à | `1 = 1` | `true`
`<>` | Différent de | `1 <> 2` | `true`
`<` | Inférieur à | `1 < 2` | `true`
`<=` | Inférieur ou égal à | `1 <= 1` | `true`
`>` | Supérieur à | `3 > 2` | `true`
`>=` | Supérieur ou égal à | `3 >= 3` | `true`

## Gestion des valeurs NULL

Neug gère les valeurs NULL dans les comparaisons selon les standards SQL. Lorsqu'un des opérandes dans une opération de comparaison est NULL, le résultat est toujours NULL, quel que soit l'opérateur de comparaison utilisé. Ce comportement est appelé "logique à trois valeurs" où les comparaisons peuvent donner `true`, `false`, ou `NULL`.

Le tableau suivant montre comment les valeurs NULL sont gérées dans les opérations de comparaison :

Operator | Example | Result
---------|---------|--------
`=` | `NULL = NULL`, `NULL = 1` | `NULL`
`<>` | `NULL <> NULL`, `NULL <> 1` | `NULL`
`<` | `NULL < NULL`, `NULL < 1` | `NULL`
`<=` | `NULL <= NULL`, `NULL <= 1` | `NULL`
`>` | `NULL > NULL`, `NULL > 1` | `NULL`
`>=` | `NULL >= NULL`, `NULL >= 1` | `NULL`