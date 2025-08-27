# Importer des données

NeuG permet d'insérer des données dans une base de données à partir d'un fichier spécifié. Le prérequis pour insérer des données dans une base de données est que vous devez d'abord créer un schéma de graphe, c'est-à-dire la structure de vos tables de nœuds et de relations.

Généralement, vous pouvez utiliser la commande `COPY FROM` pour importer des données depuis un fichier vers la base de données.

## Copier depuis un CSV

### Import dans la Table Node

Créez une table node `person` comme suit :

```cypher
CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY KEY(id));
```

Le fichier CSV `person.csv` contient les colonnes suivantes :

```csv
id|name|age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
...
```

L'instruction suivante va importer `person.csv` dans la table person :

```cypher
COPY User FROM "user.csv" (header=true);
```

**Note :**
- Le nombre de colonnes dans le fichier CSV doit être égal au nombre de propriétés définies pour le node.
- L'ordre des colonnes dans le fichier CSV doit correspondre à l'ordre des propriétés définies pour le node. Par exemple, le node ci-dessus a des propriétés dans l'ordre : id, age, name, qui correspondent aux colonnes du fichier CSV.

### Import dans une table de relations
Créez une table de relations `knows` en utilisant la requête suivante :
```cypher
CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);
```
Le fichier CSV `person_knows_person.csv` contient les colonnes suivantes :
```csv
person.id|person.id|weight
1|2|0.5
1|4|1.0
...
```
L'instruction suivante importe le fichier `person_knows_person.csv` dans la table `knows`.
```cypher
COPY knows from "person_knows_person.csv" (from="person", to="person", header=true);
```
**Note**
- Lors de l'import dans une table de relations, NeuG considère que les deux premières colonnes du fichier sont les clés primaires des nœuds `FROM` et `TO` ; les autres colonnes correspondent aux propriétés de la relation.
- Les paramètres `FROM` et `TO` doivent être inclus dans les configurations CSV pour spécifier les labels des nœuds `FROM` et `TO`.

### Configurations CSV
Les paramètres de configuration suivants sont pris en charge :
|Paramètre|Description|Valeur par défaut|
|---|---|---|
|`HEADER`|Indique si la première ligne du fichier CSV est l'en-tête. Peut être `true` ou `false`.|`false`|
|`DELIM` ou `DELIMITER`|Caractère qui sépare les différentes colonnes dans une ligne.|`\|`|
|`QUOTE`|Caractère utilisé pour commencer une chaîne de caractères entre guillemets.|`"`|
|`ESCAPE`|Caractère à l'intérieur des guillemets pour échapper le caractère `QUOTE` et d'autres caractères, par exemple un saut de ligne.|`\`|
|`SKIP`|Nombre de lignes à ignorer depuis le début du fichier d'entrée.|`0`|
|`PARALLEL`|Lit les fichiers CSV en parallèle ou non.|`true`|
|`IGNORE_ERRORS`|Ignore les lignes mal formées dans les fichiers CSV si défini sur `true`.|`false`|
|`NULL_STRINGS`|Les chaînes de caractères qui doivent être considérées comme des valeurs nulles dans le fichier CSV.|`""` (chaîne vide)|
|`FROM`|Nom de l'étiquette du vertex source, utilisé uniquement lors de l'importation d'arêtes.|`-`|
|`TO`|Nom de l'étiquette du vertex de destination, utilisé uniquement lors de l'importation d'arêtes.|`-`|

**Note**
- Les paramètres doivent être spécifiés sous forme de paires clé-valeur séparées par des virgules, comme ceci : `param1=value1, param2=value2, ...`
- Les paramètres ne sont pas sensibles à la casse, ce qui signifie que `Header`, `HEADER` et `header` sont tous valides et seront correctement reconnus dans la liste des paramètres.
- Pour les paramètres de type booléen (par exemple, `header`) :
  1. Utilisez `True`, `true` ou `1` pour indiquer qu'il est activé.
  2. Utilisez `False`, `false` ou `0` pour indiquer qu'il est désactivé.
  3. La partie valeur peut être omise si elle est égale à `true`, par exemple `header=true` peut être écrit simplement `header`.
- `FROM` et `TO` sont une paire spéciale de paramètres. Ils ne sont effectifs que lors de l'importation d'arêtes depuis un fichier CSV. Lorsque plusieurs combinaisons d'étiquettes existent pour les vertex source et cible d'une arête (par exemple, `Comment-replyOf->Post`, `Comment-replyOf->Comment`), `FROM` et `TO` doivent être spécifiés. L'utilisation de ces paramètres dans d'autres contextes entraînera une exception.