# Exporter des données
La commande `COPY TO` permet d'exporter directement les résultats d'une requête vers un format de fichier spécifié. Cela est utile pour persister le résultat d'une requête afin de l'utiliser dans d'autres systèmes ou pour conserver les résultats de requêtes à des fins d'archivage.

## Copie vers CSV
La clause COPY TO permet d'exporter les résultats d'une requête vers un fichier CSV et s'utilise comme suit :
```cypher
COPY (MATCH (p:person) RETURN p.*) TO 'person.csv' (header=true);
```

Le fichier CSV contient les champs suivants :
```csv
p.id|p.name|p.age
1|marko|29
2|vadas|27
4|josh|32
6|peter|35
```
Les types complexes, tels que les vertices et les edges, seront exportés sous forme de chaînes de caractères au format JSON.

Les paramètres disponibles sont :
|Paramètre|Description|Valeur par défaut|
|---|---|---|
|`HEADER`|Indique s'il faut inclure une ligne d'en-tête.|`false`|
|`DELIM` ou `DELIMITER`|Caractère utilisé pour séparer les champs dans le CSV.|`\|`|

Un autre exemple est présenté ci-dessous.
```cypher
COPY (MATCH (:person)-[e:knows]->(:person) RETURN e) TO 'person_knows_person.csv' (header=true);
```
Cela produit les résultats suivants dans `person_knows_person.csv` :
```csv
e
{'_SRC': '0:0', '_DST': '0:1', '_LABEL': 'knows', 'weight': 0.500000}
{'_SRC': '0:0', '_DST': '0:2', '_LABEL': 'knows', 'weight': 1.000000}
```