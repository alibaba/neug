# NeuG Web UI

The NeuG Web UI provides a browser-based interface for interacting with NeuG databases. It offers an intuitive way to execute Cypher queries, visualize results, and explore your graph data through a modern web interface.

## Basic Usage

### Starting the Web UI

The simplest way to start the web UI after start neug-cli:

```bash
neug > :ui
```

This will start the web server of the opening database on `http://127.0.0.1:5000`.

### Custom host and port

To run on a different host or port:

```bash
neug > :ui 127.0.0.1:8080
```

## Examples

### Example 1: Start with local database
```bash
# Start UI with a specific database
neug-cli open ./my_graph_db
neug > :ui
```

### Example 2: Public access
```bash
# Allow access from any IP address
neug-cli open ./my_graph_db
neug > :ui 0.0.0.0:8080
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
neug > :ui localhost:8080
```

### Database Connection Issues
Make sure the database directory exists and is accessible:
```bash
# Check if directory exists
ls -la /path/to/your/database

# Start with correct permissions
neug-cli open /path/to/your/database
neug > :ui
```

### Missing Dependencies
If you see import errors, install the required dependencies:
```bash
pip install flask flask-cors
```

## Security Considerations

- The web UI runs on localhost by default for security
- When using `0.0.0.0`, ensure proper firewall rules are in place
- In production environments, consider using a reverse proxy (nginx, Apache)
- Debug mode should never be used in production


The web interface loads resources from CDN by default. For offline development, you may need to download the static assets locally.
