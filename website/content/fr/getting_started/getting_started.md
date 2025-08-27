# Premiers pas avec NeuG

Ce guide vous accompagnera dans la création de votre première base de données graphe, la réalisation d'opérations basiques, et l'exploration des modes embarqué et service.

## Prérequis

Avant de commencer, assurez-vous que NeuG est installé. Si ce n'est pas encore le cas, veuillez suivre le [guide d'installation](../installation/installation.md).

## Comprendre l'architecture de NeuG

NeuG repose sur deux concepts architecturaux distincts qui fonctionnent indépendamment :

### Modes de stockage de la base de données
Comment vos données sont stockées :
- **Base de données en mémoire** : Les données sont stockées uniquement en RAM (le plus rapide, temporaire)
- **Base de données persistante** : Les données sont stockées sur le disque (durable, survivent aux redémarrages)

### Modes de connexion
Comment vous accédez à la base de données :
- **Mode Embedded** : Accès direct dans le processus (analyse, utilisateur unique)
- **Mode Service** : Accès via le réseau (multi-utilisateur, accès concurrents)

Ces modes sont indépendants – vous pouvez les combiner selon vos besoins :

**Combinaisons courantes :**
- **Persistante + Embedded** : Analyse de données volumineuses, traitement ETL, recherche sur graphes
- **Persistante + Service** : Applications web en production, systèmes multi-utilisateurs, architectures microservices
- **En mémoire + Embedded** : Tests unitaires, prototypage, calculs temporaires, développement d'algorithmes
- **En mémoire + Service** : Couche de cache haute performance, analyse basée sur les sessions, charges de travail temporaires multi-utilisateurs

## Modes de stockage de la base de données

### Base de données persistante
- **Cas d'usage** : Applications en production, analyse de données, stockage à long terme
- **Durabilité** : Les données survivent aux redémarrages de l'application

```python

# Exemples en mode persistant

# Assurez-vous que le répertoire de la base de données existe et est accessible en écriture par l'utilisateur.
db_persistent = neug.Database(db_path="/path/to/database")
```

### Base de données en mémoire
- **Cas d'usage** : Calculs temporaires, tests, prototypage
- **Durabilité** : Les données sont perdues lorsque le processus se termine

```python

# Exemples en mode mémoire
db_memory = neug.Database(db_path="")
```

```{note}
Actuellement, le mode in-memory de NeuG crée un répertoire de base de données temporaire qui est automatiquement nettoyé lorsque le processus se termine.
```

## Modes de connexion

NeuG propose deux modes pour accéder à votre base de données :

### Mode Embedded
Accès direct dans le processus - le plus simple pour les scénarios mono-utilisateur :

```python
import neug

# Créer la base de données et se connecter directement
db = neug.Database(db_path="./neug/db")  # ou db_path="" pour le mode in-memory
conn = db.connect()

print("Connecté à NeuG en mode embedded")

conn.close()
db.close()
```

### Mode Service
Accès via le réseau - idéal pour les applications multi-utilisateurs :

**Démarrer le service :**
```python
import neug

# Démarrer NeuG en tant que service
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print("NeuG service started on localhost:10000")
```

**Se connecter depuis un client :**
```python
from neug import Session

# Connexion au service
session = Session("http://localhost:10000/")

session.close()
```

## Opérations de base

Les opérations suivantes fonctionnent de la même manière quel que soit le mode de base de données (en mémoire ou persistante) et le mode de connexion (embarqué ou service) que vous choisissez. Supposons que nous utilisons une base de données persistante en mode embarqué pour cet exemple.

```python
import neug

# Création de la base de données et établissement de la connexion
db = neug.Database(db_path="./neug/db")
conn = db.connect()
```

### Création de Nodes et Edges

Avant d'insérer des données, vous devez définir votre schéma de graphe avec les types de nodes et edges :

```python

# Créer les tables de nœuds
conn.execute("""
    CREATE NODE TABLE Person(
        id INT64 PRIMARY KEY,
        name STRING,
        age INT64,
        email STRING
    )
""")

conn.execute("""
    CREATE NODE TABLE Company(
        id INT64 PRIMARY KEY,
        name STRING,
        industry STRING,
        founded_year INT64
    )
""")

# Créer les tables d'arêtes
conn.execute("""
    CREATE REL TABLE WORKS_FOR(
        FROM Person TO Company,
        position STRING,
        start_date DATE,
        salary DOUBLE
    )
""")

conn.execute("""
    CREATE REL TABLE KNOWS(
        FROM Person TO Person,
        since_year INT64,
        relationship_type STRING
    )
""")

print("Schéma du graphe créé avec succès !")
```

### Insertion de données

Maintenant, ajoutons quelques données à notre graphe :

```python

# Insérer des nœuds
conn.execute("""
    CREATE (p:Person {id: 1, name: 'Alice Johnson', age: 30, email: 'alice@example.com'})
""")

conn.execute("""
    CREATE (p:Person {id: 2, name: 'Bob Smith', age: 35, email: 'bob@example.com'})
""")

conn.execute("""
    CREATE (c:Company {id: 1, name: 'TechCorp', industry: 'Technology', founded_year: 2010})
""")

# Insérer des relations
conn.execute("""
    MATCH (p:Person), (c:Company) WHERE p.id = 1 AND c.id = 1
    CREATE (p)-[:WORKS_FOR {position: 'Software Engineer', start_date: date('2020-01-15'), salary: 75000.0}]->(c)
""")

conn.execute("""
    MATCH (p1:Person {id: 2}), (p2:Person {id: 1})
    CREATE (p1)-[:KNOWS {since_year: 2018, relationship_type: 'colleague'}]->(p2)
""")

print("Données insérées avec succès !")
```

### Interroger les données

Explorons votre graphe avec quelques requêtes :

```python

```python
# Requête node simple
result = conn.execute("MATCH (p:Person) RETURN p.name, p.age")
for record in result:
    print(record)
    # ['Alice Johnson', 30]
    # ['Bob Smith', 35]

# Requête de relation
result = conn.execute("""
    MATCH (p:Person)-[w:WORKS_FOR]->(c:Company)
    RETURN p.name, w.position, c.name
""")
for record in result:
    print(f"{record[0]} works as {record[1]} at {record[2]}")
    # Alice Johnson works as Software Engineer at TechCorp

# Requête de pattern complexe
result = conn.execute("""
    MATCH (p1:Person)-[:KNOWS]->(p2:Person)-[:WORKS_FOR]->(c:Company)
    RETURN p1.name as person1, p2.name as person2, c.name as company
""")
for record in result:
    print(f"{record[0]} knows {record[1]} who works at {record[2]}")
    # Bob Smith knows Alice Johnson who works at TechCorp
```

### Fermer la connexion et la base de données

```python
conn.close()
db.close()
```

### Utilisation des datasets intégrés

NeuG fournit plusieurs datasets intégrés que vous pouvez utiliser pour démarrer rapidement avec l'analyse de graphes, l'apprentissage ou les tests. Ces datasets sont prêts à l'emploi et ne nécessitent aucune configuration.

#### Datasets disponibles

Vous pouvez lister tous les datasets intégrés disponibles :

```python
from neug.datasets import get_available_datasets

# Liste tous les datasets disponibles
datasets = get_available_datasets()
for dataset in datasets:
    print(f"{dataset.name}: {dataset.description}")
```

Les datasets intégrés actuellement disponibles sont :
- modern_graph

#### Chargement des datasets intégrés

Il existe plusieurs façons de travailler avec les datasets intégrés :

**Méthode 1 : Créer une nouvelle base de données à partir d'un dataset**

```python
from neug import Database

# Créer une base de données directement à partir d'un dataset intégré
db = Database.from_builtin_dataset(dataset_name="modern_graph")
conn = db.connect()
```

```markdown
# Explorer le dataset chargé
result = conn.execute("MATCH (n) RETURN count(n) as node_count")
print(f"Total nodes: {result.__next__()[0]}") # doit être 6
```

**Méthode 2 : Charger un dataset dans une base de données existante**

```python
import neug

# Créer une base de données vide
db = neug.Database("./my_analysis.db")

# Charger un dataset builtin dedans
db.load_builtin_dataset(dataset_name="modern_graph")
```

Notez que lors de l'import d'un dataset builtin dans une base de données existante, assurez-vous qu'il n'y a pas de conflit de schéma, c'est-à-dire qu'il n'y a pas de vertices avec le label `person` et `software`, ni d'edges avec le label `knows` et `created`.

**Méthode 3 : Utilisation de la fonction de convenience**

```python
from neug.datasets.loader import load_dataset

# Ou charger dans un chemin spécifique
db = load_dataset("modern_graph", "/tmp/modern_graph.db")

conn = db.connect()

# ... travailler avec votre dataset
conn.close()
db.close()
```

## Prochaines étapes

Félicitations ! Vous avez appris les bases de NeuG. Voici ce que vous pouvez explorer ensuite :

1. **[Import/Export de données](import_graph.md)** : Apprenez à importer de grands jeux de données
2. **[Requêtes Cypher avancées](cypher_query.md)** : Maîtrisez les motifs de graphe complexes
3. **[Modélisation des données](data_model.md)** : Concevez des schémas de graphe efficaces
4. **[Optimisation des performances](../performance/index.md)** : Optimisez votre base de données pour des performances maximales