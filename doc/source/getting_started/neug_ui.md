# Neug Web UI

The Neug Web UI provides a browser-based interface for interacting with Neug databases. It offers an intuitive way to execute Cypher queries, visualize results, and explore your graph data through a modern web interface.

## Overview

The `neug-cli ui` command starts a Flask web server that serves a React-based frontend application. This allows you to:

- Execute Cypher queries through a web interface
- Browse database schema
- Visualize query results in tables and graphs
- Access the database through HTTP REST APIs

## Installation

Make sure you have `NeuG` package installed.

```bash
pip install neug
```

## Basic Usage

### Starting the Web UI

The simplest way to start the web UI:

```bash
neug-cli ui
```

This will start the web server on `http://127.0.0.1:5000` with an in-memory database.

### Opening a specific database

To connect to an existing database directory:

```bash
neug-cli ui --db /path/to/your/database
```

### Opening a remote database

To connect to a remote database endpoint

```bash
neug-cli ui --db http://127.0.0.1:10001
neug-cli ui --db 127.0.0.1:10001
```

### Custom host and port

To run on a different host or port:

```bash
neug-cli ui --host 0.0.0.0 --port 8080
```

### Debug mode

For development purposes, you can enable debug mode:

```bash
neug-cli ui --debug
```

## Command Options

| Option | Default | Description |
|--------|---------|-------------|
| `--db-dir` | None | Database directory to connect to (optional) |
| `--host` | 127.0.0.1 | Host address to bind the web server |
| `--port` | 5000 | Port number to bind the web server |
| `--debug` | False | Run the web server in debug mode |

## Examples

### Example 1: Start with local database
```bash
# Start UI with a specific database
neug-cli ui --db-dir ./my_graph_db
```

### Example 2: Public access
```bash
# Allow access from any IP address
neug-cli ui --host 0.0.0.0 --port 8080 --db-dir ./my_graph_db
```

### Example 3: Development mode
```bash
# Start in debug mode for development
neug-cli ui --debug --db-dir ./test_db
```

## Web Interface Features

Once the web UI is running, open your browser and navigate to the specified URL (default: `http://127.0.0.1:5000`).

### Query Interface

The web interface provides:

- **Query Editor**: Write and execute Cypher queries
- **Result Display**: View results in tabular format
- **Schema Browser**: Explore your database schema
- **Query History**: Access previously executed queries

### API Endpoints

The web UI also exposes REST API endpoints:

#### Schema Endpoint
```http
GET /schema
```
Returns the database schema in JSON format.

#### Query Execution Endpoint
```http
POST /cypher
Content-Type: text/plain

{
    "query": "MATCH (n) RETURN n LIMIT 10",
    "format": "json"
}
```
Executes a Cypher query and returns results in JSON format.

## Troubleshooting

### Port Already in Use
If port 5000 is already in use, specify a different port:
```bash
neug-cli ui --port 8080
```

### Database Connection Issues
Make sure the database directory exists and is accessible:
```bash
# Check if directory exists
ls -la /path/to/your/database

# Start with correct permissions
neug-cli ui --db-dir /path/to/your/database
```

### Missing Dependencies
If you see import errors, install the required dependencies:
```bash
pip install flask flask-cors
```

## Security Considerations

- The web UI runs on localhost by default for security
- When using `--host 0.0.0.0`, ensure proper firewall rules are in place
- In production environments, consider using a reverse proxy (nginx, Apache)
- Debug mode should never be used in production

## Development

For developers working on the web UI:

```bash
# Start in debug mode for hot reloading
neug-cli ui --debug --db-dir ./test_data

# The UI will automatically reload when files change
```

The web interface loads resources from CDN by default. For offline development, you may need to download the static assets locally.
