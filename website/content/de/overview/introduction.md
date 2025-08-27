# Einführung in NeuG

Willkommen bei NeuG (ausgesprochen "new-gee"), einer HTAP (Hybrid Transactional/Analytical Processing) Graphdatenbank, die für hochperformante eingebettete Graphanalysen und Echtzeit-Transaktionsverarbeitung entwickelt wurde.

NeuG wurde entwickelt, um den wachsenden Bedarf an Systemen zu decken, die sowohl operationale Graphworkloads als auch komplexe analytische Abfragen auf denselben Daten verarbeiten können, und bietet eine einheitliche Plattform für moderne Graphanwendungen.

## Was ist NeuG?

NeuG ist eine Graphdatenbank, die durch ihre **einzigartige Dual-Mode-Architektur** das Beste aus beiden Welten kombiniert:
- **Transaktionale Verarbeitung (TP)**: Echtzeit-Graphoperationen mit ACID-Garantien
- **Analytische Verarbeitung (AP)**: Hochperformante Graphanalytik und Pattern Matching
- **Duale Verbindungsmodi**: Flexible Bereitstellung als eingebettetes oder service-basiertes System
- **Optimierte Performance**: Jeder Modus ist speziell für seine Zielworkload abgestimmt

Aufbauend auf einer modernen C++-Core-Engine bietet NeuG:
- Property-Graph-Datenmodell mit Cypher-Abfragesprache
- Spaltenbasierte festplattenbasierte Speicherung mit komprimierten Adjazenzlisten
- Vektorisierte und parallelisierte Abfrageverarbeitung
- MVCC-basiertes Transaktionsmanagement

## Warum NeuG wählen?

### Einzigartige Dual-Mode-Architektur

Das herausragende Merkmal von NeuG ist ihr **Dual-Mode-Design**, das die Performance für verschiedene Graph-Workload-Muster optimiert:

#### Embedded Mode - Optimiert für Analytics (AP)
**Beste Verwendung**: Komplexe Pattern Matching, Full-Graph-Berechnungen und analytische Workloads
- **Batch Data Loading**: Effiziente Verarbeitung von Large-Scale-Data-Imports
- **Maximum Query Performance**: Nutzung aller verfügbaren CPU-Kerne für komplexe Queries
- **Full Resource Utilization**: Jede Query kann die vollständigen Systemressourcen nutzen

**Typische Use Cases**:
- Graph Analytics und Data Science Workloads
- Komplexe Pattern Detection und Graph-Algorithmen
- Batch Processing und ETL-Operationen
- Forschung und explorative Datenanalyse

#### Service Mode - Optimiert für Transaktionen (TP)
**Bestens geeignet für**: Echtzeitanwendungen mit häufigen kleinen Updates und gleichzeitigem Zugriff
- **Gleichzeitige Lese- und Schreiboperationen**: Optimiert für hochfrequente, kleinskalige Datenänderungen
- **Point Queries**: Effizienter Zugriff auf kleine Teilmengen von Graphdaten
- **Multi-Session Support**: Verwaltung mehrerer gleichzeitiger Verbindungen
- **Low-Latency Operations**: Schnelle Antwortzeiten für transaktionale Workloads

**Typische Anwendungsfälle**:
- Echtzeit-Empfehlungssysteme
- Betrugsdetektion und Risikomanagement
- Online Transaction Processing

### Flexible Mode Combination

Die wahre Stärke von NeuG liegt in der **flexiblen Kombination** beider Modi:

- **Data Pipeline Integration**: Verwende den Embedded Mode für das initiale Laden von Bulk-Daten, wechsle dann zum Service Mode für operative Abfragen
- **Hybrid Workflows**: Exportiere einen Checkpoint von einem laufenden TP-Service in eine separate Kopie für intensive AP-Analysen
- **Development to Production**: Entwickle und teste im Embedded Mode, deploye als Service für die Produktion
- **Workload Separation**: Führe AP-Workloads auf dedizierten Embedded-Instanzen aus, während du TP-Services aufrechterhältst

> **Hinweis**: Obwohl der Embedded Mode kleine Änderungen verarbeiten kann, blockieren häufige Schreibvorgänge alle Leseabfragen (und umgekehrt) aufgrund des exklusiven Locking-Designs. Ebenso kann der Service Mode große Pattern-Abfragen ausführen, aber es wird nicht empfohlen, alle verfügbaren Threads für die Beschleunigung einer einzelnen Abfrage zu verwenden, da dies andere gleichzeitige Abfragen beeinträchtigen würde.

### Performance und Skalierbarkeit
NeuG ist für hochperformante Graph-Workloads konzipiert mit:
- Fortschrittlicher Query-Optimierung und Ausführungsplanung
- Vektorisierten Operationen für analytische Workloads
- Effizientem Speicherformat, optimiert für Graph-Traversals
- Multi-Core-Parallelität für komplexe Abfragen

### Flexibilität und Benutzerfreundlichkeit
- **Cross-Platform**: Läuft auf Linux und macOS auf x86- und ARM-Architekturen
- **Einfache Integration**: Die Embedded-Architektur eliminiert den Overhead des Server-Managements
- **Moduswechsel**: Nahtloser Wechsel zwischen Embedded- und Service-Modus

### Enterprise-Ready Features
- **ACID Transactions**: Serialisierbare ACID-Garantien für mission-critische Anwendungen
- **Concurrent Access**: Optimiert für read-heavy Analytics und write-intensive Operationen
- **Error Handling**: Umfassende Fehlerverwaltung und Recovery-Mechanismen
- **Monitoring**: Integriertes Performance-Monitoring und Query-Profiling

NeuG wird vom [GraphScope](https://graphscope.io) Team entwickelt, einer führenden Forschungsgruppe von Alibaba, die für ihre Expertise in Graph Computing und Large-Scale Data Processing bekannt ist. Das Framework integriert fortschrittliche Methoden, die aus Alibabas umfangreicher Erfahrung bei der Bereitstellung von Graph Analytics stammen, um unternehmensweite Herausforderungen in Bereichen wie E-Commerce, Supply Chain Optimization und Fraud Detection zu lösen.