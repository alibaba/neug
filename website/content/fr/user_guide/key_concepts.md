### Concepts clés
Database, Connection et Session sont des concepts fondamentaux dans NeuG.

## Database

Une **Database** dans NeuG représente un graphe composé de vertices et d'edges. Par défaut, les données sont persistées sur le disque dans un répertoire spécifié. NeuG propose également un mode **in-memory**, où les données résident uniquement en mémoire et ne sont pas sauvegardées sur le disque. Pour créer une database en mémoire, définissez `db_path` comme une chaîne vide :

```python
from neug import Database

# Create an in-memory database
db = Database(db_path="")
```

### Modes d'accès

Les bases de données NeuG peuvent être accédées en deux modes : `read_write` et `read_only`.

- **read_write** : L'ouverture d'un répertoire en mode **read_write** accorde un accès exclusif à ce processus en verrouillant le répertoire, empêchant tout autre processus d'y accéder simultanément.
- **read_only** : Lorsqu'il est ouvert en mode **read_only**, plusieurs processus peuvent accéder au répertoire simultanément en mode **read_only**.

Il est important de noter que les bases de données en mémoire ne peuvent pas être ouvertes en mode `read_only`.

## Connexion

Une **Connection** sert d'interface principale pour interagir avec la base de données embarquée, facilitant l'exécution de requêtes et la gestion des données.

### Établir des connexions depuis la base de données

Un objet **Connection** agit comme un canal pour accéder à la base de données, avec un comportement influencé par le mode d'accès de la base de données.

- En mode **read_write**, une Connection peut exécuter à la fois des requêtes de lecture (MATCH) et d'écriture (CREATE, COPY FROM). Une seule connexion peut être créée pour une base de données read_write afin de maintenir la cohérence des données.
  
- En mode **read_only**, une Connection est limitée à l'exécution de requêtes de lecture.

## Mode service et session

NeuG peut également fonctionner en **mode service**, où la base de données opère comme un serveur, acceptant des connexions à distance. Ce mode est optimal pour les applications nécessitant un accès fréquent, concurrent ou distribué.

Pour démarrer NeuG en mode service, spécifiez l'hôte et le port :

```python
import neug

# Lancer NeuG en tant que service
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print(f"NeuG service started on {service}")
```

Une fois le serveur actif, les clients peuvent se connecter en utilisant un objet `Session` :

```python
from neug import Session

# Établir une connexion au service NeuG
session = Session("http://localhost:10000/")

# Exécuter une requête
session.execute('MATCH(n) RETURN count(n)')

session.close()
```

Plusieurs sessions peuvent être instanciées dans différents processus pour soumettre des requêtes concurrentes. Le serveur de base de données garantit que les propriétés ACID de la base de données sont respectées durant toute la durée des opérations.