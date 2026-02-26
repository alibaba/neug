# Overview

The Extension framework in database systems is a mechanism that allows dynamically adding new functionality without modifying the core engine code. NeuG also provides an Extension framework that enables external users to flexibly load new features, with the following key advantages:

- **Core engine remains lean**: Provides essential functionality for query parsing, optimization, and execution
- **New features developed as plugins**: Offers rich external extension capabilities, such as external data import, graph algorithm analysis, etc.
- **Reduced maintenance complexity**: Avoids core code bloat, improving readability and stability

## Available Extensions

The following extensions are currently supported or planned to be supported in NeuG:

| Category | Extension | Description | Since Version |
|----------|-----------|-------------|---------------|
| Data Source | [JSON](load_json.md) | Import & export  data from JSON file format | v0.1 |
| Data Source| PARQUET | Import & Export data from PARQUET format files | planned v0.2 |
| VFS | HTTP/HTTPS/S3/OSS | Provide data source based on HTTP/HTTPS/S3/OSS protocol | planned v0.2 |
| Graph Algorithm | K-Core | Find all subgraphs with core number ≥ k. Returns `(node, core_number)` | planned v0.2 |
| Graph Algorithm | PageRank | Calculate node importance scores. Returns `(node, rank)` | planned v0.2 |
| Graph Algorithm | Shortest Path (Dijkstra) | Single-source shortest path algorithm | planned v0.2 |
| Graph Algorithm | Connected Components | Weakly connected components detection | planned v0.2 |
| Graph Algorithm | Leiden | Community detection algorithm for finding communities in networks | planned v0.2 |
| Graph Algorithm | Label Propagation | Community detection algorithm that propagates labels through the network | planned v0.2 |
| Graph Algorithm | Subgraph Matching (Estimator) | Unbiased estimation of subgraph matching | planned v0.2 |

## Using Extensions

The following sections detail how to install and use the extensions listed above.

### Install Extension

The `INSTALL` command downloads official extensions from the NeuG Official Repository to your local machine. NeuG automatically downloads the appropriate platform-specific dynamic library based on your current operating system. 

Regarding the local download path, please note the following:
- By default, extensions are downloaded to `<python_wheel_install_home>/extension/<extension_name>`.
- You can set the `EXTENSION_HOME` environment variable to specify a custom download directory. When set, extensions will be downloaded to `$EXTENSION_HOME/extension/<extension_name>`.

NeuG automatically performs checksum verification on downloaded content to detect issues caused by network interruptions that might make extensions unusable. If the checksum verification fails, the downloaded file will be automatically removed and an error will be returned.

```cypher
INSTALL <extension_name>;
```

Example: Download JSON Extension

```cypher
INSTALL JSON;
```

### Load Extension

The `LOAD` command loads the dynamic library from `$EXTENSION_HOME/extension/<extension_name>` (or `<python_wheel_install_home>/extension/<extension_name>` if `EXTENSION_HOME` is not set) into the current database for use in subsequent queries.

```cypher
LOAD <extension_name>;
```

Example: Load JSON Extension

```cypher
LOAD JSON;
```

### List Extensions

Use the `CALL` command to view currently loaded extensions. This command outputs the extension name and description.

```cypher
CALL SHOW_LOADED_EXTENSIONS() RETURN *;
```

Example output:

| Extension Name | Description |
|----------------|-------------|
| JSON | Provides functions to read and write JSON files. |

### Uninstall Extensions

The `UNINSTALL` command removes the downloaded dynamic library from the local installation directory. This permanently deletes the extension files from your system.

```cypher
UNINSTALL <extension_name>;
```

Example: Uninstall JSON Extension

```cypher
UNINSTALL JSON;
```

## Extension Lifecycle

The typical lifecycle of an extension follows these steps:

1. **Install**: Download the extension from the official repository to your local system
2. **Load**: Load the extension into your current database to make it available for use
3. **Use**: Execute queries that utilize the extension's functionality
4. **Unload**: Extensions are automatically unloaded when the database closes
5. **Uninstall**: Remove the extension files from your local system when no longer needed
