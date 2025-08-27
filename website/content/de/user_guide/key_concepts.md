### Schlüsselkonzepte
Database, Connection und Session sind grundlegende Konzepte in NeuG.

## Database

Eine **Database** in NeuG repräsentiert einen Graphen, der aus Vertices und Edges besteht. Standardmäßig werden die Daten auf der Festplatte in einem angegebenen Verzeichnis persistiert. NeuG bietet auch einen **in-memory** Modus, bei dem die Daten nur im Speicher verbleiben und nicht auf die Festplatte geschrieben werden. Um eine In-Memory-Datenbank zu erstellen, setze `db_path` auf einen leeren String:

```python
from neug import Database

# Erstelle eine In-Memory-Datenbank
db = Database(db_path="")
```

### Zugriffsmodi

NeuG-Datenbanken können in zwei Modi aufgerufen werden: `read_write` und `read_only`.

- **read_write**: Das Öffnen eines Verzeichnisses im **read_write**-Modus gewährt exklusiven Zugriff für diesen Prozess, indem das Verzeichnis gesperrt wird. Dadurch wird verhindert, dass andere Prozesse gleichzeitig darauf zugreifen.
- **read_only**: Wenn ein Verzeichnis im **read_only**-Modus geöffnet wird, können mehrere Prozesse gleichzeitig im **read_only**-Modus auf das Verzeichnis zugreifen.

Es ist wichtig zu beachten, dass In-Memory-Datenbanken nicht im `read_only`-Modus geöffnet werden können.

## Connection

Eine **Connection** dient als primäre Schnittstelle für die Interaktion mit der eingebetteten Datenbank und ermöglicht die Ausführung von Abfragen sowie die Verwaltung von Daten.

### Aufbau von Verbindungen aus der Datenbank

Ein **Connection**-Objekt dient als Verbindungskanal für den Zugriff auf die Datenbank, wobei sein Verhalten vom Zugriffsmodus der Datenbank beeinflusst wird.

- Im **read_write**-Modus kann eine Connection sowohl Lese- (MATCH) als auch Schreibabfragen (CREATE, COPY FROM) ausführen. Für eine read_write-Datenbank darf nur ein Connection-Objekt erstellt werden, um die Datenkonsistenz zu gewährleisten.
  
- Im **read_only**-Modus ist eine Connection auf die Ausführung von Leseabfragen beschränkt.

## Service-Modus und Session

NeuG kann auch im **Service-Modus** betrieben werden, bei dem die Datenbank als Server fungiert und Remote-Verbindungen akzeptiert. Dieser Modus ist optimal für Anwendungen, die häufigen, gleichzeitigen oder verteilten Zugriff erfordern.

Um NeuG im Service-Modus zu starten, gib den Host und Port an:

```python
import neug

# NeuG als Service starten
```python
db = neug.Database("./neug/db")
service = db.serve(host="localhost", port=10000)
print(f"NeuG service started on {service}")
```

Sobald der Server aktiv ist, können Clients über ein `Session`-Objekt eine Verbindung herstellen:

```python
from neug import Session

# Establish a connection to the NeuG service
session = Session("http://localhost:10000/")

# Perform a query
session.execute('MATCH(n) RETURN count(n)')

session.close()
```

Es können mehrere Sessions in verschiedenen Prozessen instanziiert werden, um gleichzeitige Queries abzusetzen. Der Datenbankserver stellt sicher, dass die ACID-Eigenschaften der Datenbank währenddessen gewährleistet bleiben.