# Opérateur arithmétique

Neug prend en charge les opérations arithmétiques courantes, notamment l'addition (+), la soustraction (-), la multiplication (*), la division (/) et le modulo (%).

## Type de résultat

Les types de données pouvant participer aux opérations arithmétiques sont : INT32, UINT32, INT64, UINT64, FLOAT, DOUBLE. Le type du résultat d'une opération arithmétique est déterminé par les types des opérandes, selon les règles suivantes :
- Si les largeurs des opérandes gauche et droite sont différentes, le résultat est le type ayant la plus grande largeur
- Si les largeurs des opérandes gauche et droite sont identiques mais de signes différents, le résultat est le type signé immédiatement supérieur
- Le type de largeur maximale pour les nombres à virgule flottante est DOUBLE, et pour les entiers c'est INT64

Le tableau suivant détaille les types de résultats pour différentes combinaisons d'opérandes :

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| INT32    | INT32    | INT32  |
| INT32    | UINT32   | INT64  |
| INT32    | INT64    | INT64  |
| INT32    | UINT64   | UINT64 |
| INT32    | FLOAT    | FLOAT  |
| INT32    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| UINT32   | INT32    | INT64  |
| UINT32   | UINT32   | UINT32 |
| UINT32   | INT64    | INT64  |
| UINT32   | UINT64   | UINT64 |
| UINT32   | FLOAT    | FLOAT  |
| UINT32   | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| INT64    | INT32    | INT64  |
| INT64    | UINT32   | INT64  |
| INT64    | INT64    | INT64  |
| INT64    | UINT64   | UINT64 |
| INT64    | FLOAT    | FLOAT  |
| INT64    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| UINT64   | INT32    | UINT64 |
| UINT64   | UINT32   | UINT64 |
| UINT64   | INT64    | UINT64 |
| UINT64   | UINT64   | UINT64 |
| UINT64   | FLOAT    | FLOAT  |
| UINT64   | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| FLOAT    | INT32    | FLOAT  |
| FLOAT    | UINT32   | FLOAT  |
| FLOAT    | INT64    | FLOAT  |
| FLOAT    | UINT64   | FLOAT  |
| FLOAT    | FLOAT    | FLOAT  |
| FLOAT    | DOUBLE   | DOUBLE |

| Operand0 | Operand1 | Result |
|----------|----------|--------|
| DOUBLE   | INT32    | DOUBLE |
| DOUBLE   | UINT32   | DOUBLE |
| DOUBLE   | INT64    | DOUBLE |
| DOUBLE   | UINT64   | DOUBLE |
| DOUBLE   | FLOAT    | DOUBLE |
| DOUBLE   | DOUBLE   | DOUBLE |

## Gestion des erreurs

En plus des types de résultats, les opérations arithmétiques peuvent rencontrer des erreurs de dépassement (overflow), de sous-dépassement (underflow) ou de division par zéro, selon les valeurs des opérandes. Pour ces erreurs, Neug propose différents comportements en fonction du type de données et du mode de déploiement :

1. **Types à virgule flottante** : Aucune gestion particulière n’est effectuée ; le système s’appuie sur le comportement standard [IEEE 754]() qui retourne les valeurs `Infinity`, `-Infinity` ou `NaN`.

2. **Types entiers** : Le comportement diffère entre le mode debug et le mode release :
   - **Mode debug** : Les contraintes de performance étant plus faibles, une validation des entrées est effectuée à l’exécution, avec levée explicite d’exceptions.
   - **Mode release** : Les impératifs de performance élevée impliquent qu’un overflow ou underflow retourne une valeur indéfinie, à l’exception de la division par zéro qui peut provoquer un core dump.

Le tableau suivant détaille les types d’erreurs que chaque opérateur peut rencontrer :

| Opérateur | Overflow | Underflow | Division par zéro | Exemple |
|-----------|----------|-----------|-------------------|---------|
| +         | OUI      | OUI       | NON               | Return CAST(2147483647, 'int32') + CAST(1, 'int32') |
| -         | OUI      | OUI       | NON               | Return CAST(-2147483648, 'int32') - CAST(1, 'int32') |
| *         | OUI      | OUI       | NON               | Return CAST(2147483647, 'int32') * CAST(2, 'int32') |
| /         | NON      | NON       | OUI               | Return 5 / 0 |
| %         | NON      | NON       | OUI               | Return 5 % 0 |

## Arithmétique sur les dates

En plus des types numériques, les opérations arithmétiques peuvent également être effectuées sur les types datetime et interval. Neug prend en charge les opérations arithmétiques sur les dates qui vous permettent d'ajouter ou de soustraire des intervalles à partir de dates et timestamps, ainsi que de calculer les différences entre des valeurs temporelles.

### Opérations supportées

Le tableau suivant détaille les opérations arithmétiques sur les dates prises en charge :

| Opération | Description | Exemple | Résultat |
|-----------|-------------|---------|--------|
| DATE + INTERVAL | Ajouter un intervalle à une date | `DATE('2011-02-15') + INTERVAL('5 DAYS')` | `DATE('2011-02-20')` |
| DATE - INTERVAL | Soustraire un intervalle d'une date | `DATE('2011-02-15') - INTERVAL('5 DAYS')` | `DATE('2011-02-10')` |
| TIMESTAMP + INTERVAL | Ajouter un intervalle à un timestamp | `TIMESTAMP('2011-10-21 14:25:13') + INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-22 20:25:33')` |
| TIMESTAMP - INTERVAL | Soustraire un intervalle d'un timestamp | `TIMESTAMP('2011-10-21 14:25:13') - INTERVAL('30 HOURS 20 SECONDS')` | `TIMESTAMP('2011-10-20 08:24:53')` |
| INTERVAL + INTERVAL | Additionner deux intervalles | `INTERVAL('5 DAYS') + INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('6 DAYS 6 HOURS 20 SECONDS')` |
| INTERVAL - INTERVAL | Soustraire des intervalles | `INTERVAL('5 DAYS') - INTERVAL('30 HOURS 20 SECONDS')` | `INTERVAL('3 DAYS 17 HOURS 39 MINUTES 40 SECONDS')` |
| TIMESTAMP - TIMESTAMP | Calculer la différence de temps | `TIMESTAMP('2011-10-21 14:25:13') - TIMESTAMP('1989-10-21 14:25:13')` | `INTERVAL('8035 DAYS')` |