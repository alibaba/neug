# The python binding API for NeuG

## Building the Wheel

### Develop

To build a wheel for the local environment, run:

```bash
source ~/.neug_env
cd tools/python_bind
export DEBUG=1
python3 -m pip  install -r requirements.txt
python3 -m pip  install -r requirements_dev.txt
python3 setup.py build_ext
python3 setup.py bdist_wheel
python3 -m pip  install dist/*
```

### Distribution

To build wheels for all supported Python versions on this platform, use the following commands:

```bash
python3 -m pip  install cibuildwheel
cd ${ROOT_DIR}
cibuildwheel ./tools/python_bind --output-dir wheelhouse
```

## Development Mode Setup

In development mode, wheels are not built or installed. Instead, the required dynamic library is built and copied to `tools/python_bind/build`. Any changes made to the Python codebase will take effect immediately, allowing for seamless reloading of files.

```bash
make develop
# run tests

python3 -m pytest tests/test_a.py
```

or 
```bash
python3 setup.py build_ext --inplace --build-temp=build
```

## Neug CLI

The `Neug` CLI tool provides an interactive shell for querying and managing NeuG database. It supports both local and remote database connections, Cypher query execution, and result formatting.

### Installation
`neug-cli` is included in neug package.

```bash
pip install neug
```

After installation, you can verify that `neug-cli` is installed correctly by running:

```bash
neug-cli --version
```

This should display:

```
neug-cli, version 0.1.0
```

### Usage

#### Overview

The `neug-cli` tool allows you to interact with the Neug database in both local and remote modes. To view the basic usage, run:

```bash
neug-cli --help
```

This displays:

```
Usage: neug-cli [OPTIONS] COMMAND [ARGS]...

  Neug CLI Tool.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  connect  Connect to a remote database.
  open     Open a local database.
```

#### Start the Shell by Opening a Local Database

To open a local Neug database, specify the database path when starting the CLI. By default, the database is opened in read-write mode, and changes are persisted to disk when the shell exits. If the specified directory does not exist, it will be created automatically.

To open the database in read-only mode, use the `--readonly` or `-r` option.

```bash
neug-cli open <path-to-db> -m [read-only|read-write]
```

- `--mode`, `-m`: Specify mode of database.

#### Start the Shell by Connecting to a Remote Database

To connect to a remote Neug server, specify the server URI when starting the CLI. You can optionally provide a username, password, and query timeout. Note that remote connection support is under development.

```bash
neug-cli connect <host:port> [--timeout <seconds>]
```

- `--timeout`: Connection timeout in seconds (default: 300).

#### Interactive Shell Commands

Once you start the shell, you can execute Cypher queries and use various interactive commands:

- Enter Cypher queries directly to execute them.
- Use `:help` to display this help message.
- Use `:quit` or press Ctrl+C to leave the shell.
- Use `:max_rows <number>` to set the maximum number of rows to display for query results.
- Use `:ui <endpoint>` to start a web ui service.
- Multi-line commands are supported. Use ';' at the end to execute.
- Command history is supported; use the up/down arrow keys to navigate previous commands.

### Example

```bash
neug-cli open /tmp/modern_graph
```

This will open embedded Neug database at `/tmp/modern_graph` in `rw` mode by default, and start the shell. Then you can execute Cypher queries directly:

```
Welcome to the Neug shell. Type :help for usage hints.

neug > MATCH (n:person) RETURN n;
+-------------------------------------------------------+
| n                                                     |
+=======================================================+
| {_ID: 0, _LABEL: person, id: 1, name: marko, age: 29} |
+-------------------------------------------------------+
| {_ID: 1, _LABEL: person, id: 2, name: vadas, age: 27} |
+-------------------------------------------------------+
| {_ID: 2, _LABEL: person, id: 4, name: josh, age: 32}  |
+-------------------------------------------------------+
| {_ID: 3, _LABEL: person, id: 6, name: peter, age: 35} |
+-------------------------------------------------------+
```