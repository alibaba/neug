# Modèle de données

## Types de données

`Neug` offre un support robuste pour un large éventail de types de données utilisés dans les propriétés des vertex et des edge. Ces types de données sont organisés en cinq catégories distinctes :

- **Types primitifs :** Ces types incluent les données fondamentales telles que les entiers, les nombres à virgule flottante et les booléens, qui sont essentiels pour représenter les valeurs numériques et logiques de base.

- **Types String :** Cette catégorie comprend les types qui gèrent les données textuelles, permettant un stockage et une manipulation efficaces des strings au sein des vertex et des edge.

- **Types temporels :** Ces types sont conçus pour gérer les informations de date et d'heure, en supportant diverses représentations temporelles pour permettre l'analyse et les requêtes temporelles.

- **Types Array :** Les types Array permettent le stockage de collections d'éléments, offrant la possibilité de représenter des listes ou des ensembles de valeurs associées à un élément du graphe.

- **Types complexes :** (Actuellement non supportés) Ces types pourraient inclure des objets définis par l'utilisateur ou des structures composées qui combinent plusieurs types primitifs, permettant un modeling de données et des relations plus sophistiqués au sein du graphe.

### Types Primitifs

Certainement ! Voici un tableau Markdown avec l'en-tête et la première ligne spécifiés :

| Type   | Taille  | Description                    |
|--------|---------|--------------------------------|
| BOOL   | 1 bit   | Boolean sur un bit             |
| INT8   | 1 octet | Entier sur un octet            |
| UINT8  | 1 octet | Entier non signé sur un octet  |
| INT16  | 2 octets| Entier sur deux octets         |
| UINT16 | 2 octets| Entier non signé sur deux octets|
| INT32  | 4 octets| Entier sur quatre octets       |
| UINT32 | 4 octets| Entier non signé sur quatre octets|
| INT64  | 8 octets| Entier sur huit octets         |
| UINT64 | 8 octets| Entier non signé sur huit octets|
| FLOAT  | 4 octets| Float simple précision         |
| DOUBLE | 8 octets| Float double précision         |

### Types String

| Type   | Taille  | Description                    |
|--------|---------|--------------------------------|
| STRING | -       | Chaîne de caractères de longueur variable |

### Types temporels

#### DATE

`DATE` désigne un jour du calendrier caractérisé par les composants année, mois et jour, au format respectant la norme ISO-8601 (YYYY-MM-DD). Il occupe 4 octets en stockage.

```cypher
RETURN date('2022-06-06') as x;
```

#### DATE32

`DATE32` représente un jour du calendrier en comptant le nombre de jours depuis le 1er janvier 1970. Les jours précédant cette date sont indiqués par des valeurs négatives. Il occupe 4 octets en stockage. (TODO: (xiaoli): vérifier que cela fonctionne)

```cypher
RETURN date32('2022-06-06') as x;
```

#### TIMESTAMP

`TIMESTAMP` intègre à la fois les composants de date et d'heure—heure, minute, seconde et milliseconde—formatés selon la norme ISO-8601 (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]]). Ce format spécifie la date (YYYY-MM-DD), l'heure (hh:mm:ss[.zzzzzz]) et le décalage horaire optionnel [+-TT[:tt]]. La partie date est obligatoire, tandis que le composant temps est optionnel, permettant l'inclusion de millisecondes [.zzzzzz] et d'un décalage horaire si désiré. Il occupe 8 octets en stockage. Par exemple : "1970-01-01 00:00:00.004666-10"

```cypher
RETURN timestamp("1970-01-01 00:00:00.004666-10") as x;
```

#### INTERVAL

`INTERVAL` représente une période de temps, pouvant être caractérisée par les composants année, mois, jour, heure, minutes, microsecondes. Nous suivons la définition d'Interval dans [kuzu](https://docs.kuzudb.com/cypher/data-types/#interval).

```cypher
RETURN interval("1 year 2 days") as x;
```

### Types de tableau

Les types de tableau ne sont actuellement pas supportés, mais leur prise en charge est prévue dans un avenir proche.
Une fois supportés, ils nécessiteront que chaque élément du tableau respecte l'un des types primitifs mentionnés précédemment.

### Types complexes

Les types complexes ne sont actuellement pas supportés, notamment `STRUCT`, `MAP`, `UNION`, `BLOB`.

## Conversion de types

Nous prenons en charge le [casting entre les types numériques](https://github.com/GraphScope/neug/issues/416).

- INT32
- UINT32
- INT64
- UINT64
- FLOAT
- DOUBLE

**Toutes les paires de ces types peuvent être converties entre elles**. Lors du casting, le compilateur effectue des vérifications de dépassement pour garantir que les conversions sont sûres. Le tableau ci-dessous résume les scénarios de dépassement potentiels gérés lors de ces conversions :

| From \ To | INT32  | UINT32 | INT64  | UINT64 | FLOAT  | DOUBLE |
|--------|--------|--------|--------|--------|--------|--------|
| **INT32**  | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT32** | ⚠️ May Overflow (if value > INT32_MAX)         | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe | ✅ Safe |
| **INT64**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ✅ Safe | ⚠️ May Overflow (if value < 0)        | ✅ Safe | ✅ Safe |
| **UINT64** | ⚠️ May Overflow (if value > INT32_MAX)         | ⚠️ May Overflow (if value > UINT32_MAX)       | ⚠️ May Overflow (if value > INT64_MAX) | ✅ Safe | ✅ Safe | ✅ Safe |
| **FLOAT**  | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ✅ Safe | ✅ Safe |
| **DOUBLE** | ⚠️ May Overflow (if value < INT32_MIN or > INT32_MAX) | ⚠️ May Overflow (if value < 0 or > UINT32_MAX) | ⚠️ May Overflow (if value < INT64_MIN or > INT64_MAX) | ⚠️ May Overflow (if value < 0 or > UINT64_MAX) | ⚠️ May Overflow (if value < -FLT_MAX or > FLT_MAX) | ✅ Safe |