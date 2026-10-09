# Overview

The Extension framework in database systems is a mechanism that allows dynamically adding new functionality without modifying the core engine code. NeuG also provides an Extension framework that enables external users to flexibly load new features, with the following key advantages:

- **Core engine remains lean**: Provides essential functionality for query parsing, optimization, and execution
- **New features developed as plugins**: Offers rich external extension capabilities, such as external data import, graph algorithm analysis, etc.
- **Reduced maintenance complexity**: Avoids core code bloat, improving readability and stability

## Available Extensions

The following extensions are currently supported or planned to be supported in NeuG:

| Category        | Extension                        | Description                                                               |
| --------------- | -------------------------------- | ------------------------------------------------------------------------- |
| Data Source     | [PARQUET](load_parquet.md)          | Import and export data in Parquet format                                  |
| File System     | [HTTP/HTTPS/S3/OSS](load_httpfs.md) | Provide data sources over HTTP, HTTPS, S3, and OSS                         |
| Graph Algorithm | [GDS](load_gds.md)                  | Graph Data Science algorithms (PageRank, BFS, SSSP, WCC, LCC, K-Core, Label Propagation, Louvain, Leiden) |
| Graph Query     | [Pattern Match](pattern_match.md)    | Subgraph pattern matching with exact DAF matching and sampled FaSTest matching |
| Vector Search   | [Vector Search](vector_search.md)    | Vector distance functions and HNSW-based approximate nearest neighbor search |
| Search          | [Full-Text Search](fts_search.md)    | BM25-ranked full-text search over string properties with SQLite FTS5 indexes |

To author a custom extension outside the NeuG tree (NeuG as submodule), see [Developing Extensions](../development/develop_extension.md).

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

Example: Download PARQUET Extension

```cypher
INSTALL PARQUET;
```

### Load Extension

The `LOAD` command loads the dynamic library from `$EXTENSION_HOME/extension/<extension_name>` (or `<python_wheel_install_home>/extension/<extension_name>` if `EXTENSION_HOME` is not set) into the current database for use in subsequent queries.

```cypher
LOAD <extension_name>;
```

Example: Load PARQUET Extension

```cypher
LOAD PARQUET;
```

### List Extensions

Use the `CALL` command to view currently loaded extensions. This command outputs the extension name and description.

```cypher
CALL SHOW_LOADED_EXTENSIONS() RETURN *;
```

Example output:

| Extension Name | Description                                            |
| -------------- | ------------------------------------------------------ |
| PARQUET        | Provides functions to read and write PARQUET files.    |

### Uninstall Extensions

The `UNINSTALL` command removes the downloaded dynamic library from the local installation directory. This permanently deletes the extension files from your system.

```cypher
UNINSTALL <extension_name>;
```

Example: Uninstall PARQUET Extension

```cypher
UNINSTALL PARQUET;
```

## Extension Lifecycle

The typical lifecycle of an extension follows these steps:

1. **Install**: Download the extension from the official repository to your local system
2. **Load**: Load the extension into your current database to make it available for use
3. **Use**: Execute queries that utilize the extension's functionality
4. **Uninstall**: Remove the extension files from your local system when no longer needed
