# Opérateurs logiques

Neug prend actuellement en charge trois opérateurs logiques : AND, OR, NOT ; et gère les valeurs NULL de manière particulière selon la « logique à trois valeurs » de SQL. Les tables de vérité spécifiques sont présentées ci-dessous.

## Table de vérité de AND

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| TRUE     | TRUE     | TRUE   |
| TRUE     | FALSE    | FALSE  |
| TRUE     | NULL     | NULL   |
| FALSE    | TRUE     | FALSE  |
| FALSE    | FALSE    | FALSE  |
| FALSE    | NULL     | FALSE  |
| NULL     | TRUE     | NULL   |
| NULL     | FALSE    | FALSE  |
| NULL     | NULL     | NULL   |

## Table de vérité de OR

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| TRUE     | TRUE     | TRUE   |
| TRUE     | FALSE    | TRUE   |
| TRUE     | NULL     | TRUE   |
| FALSE    | TRUE     | TRUE   |
| FALSE    | FALSE    | FALSE  |
| FALSE    | NULL     | NULL   |
| NULL     | TRUE     | TRUE   |
| NULL     | FALSE    | NULL   |
| NULL     | NULL     | NULL   |

## Table de vérité du NOT

| Opérande | Résultat |
|----------|----------|
| TRUE     | FALSE    |
| FALSE    | TRUE     |
| NULL     | NULL     |