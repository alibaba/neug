# Command Line Interface (CLI)
NeuG provides a CLI shell through which you can query the database using Cypher queries.

## Start the Command Line
NeuG support embedded and remote database services. The Command Line Interface (CLI) provides identical functionality across these two modes.

### Embedded Database
You can create an embedded database in the CLI by specifying a path after the `neug-cli open` command in the terminal. This opens the database embedded, in read-write mode, and the data is persisted to disk after the CLI shell is closed. Note that if the database directory does not exist, it will be created for you.
```bash
neug-cli open /workspaces/neug_example
```
```
INFO:neug:Build directory: /workspaces/neug/tools/python_bind/neug/../build/lib.linux-x86_64-3.10
INFO:neug:Adding build directory to sys.path: /workspaces/neug/tools/python_bind/neug/../build/lib.linux-x86_64-3.10
Opened database at /workspaces/neug_example in rw mode
INFO:neug.database:Open database /workspaces/neug_example in rw mode
INFO:neug:Connection established.
Welcome to the NeuG shell. Type :help for usage hints.

neug > 
```

### Remote Database Service
You can also use the `neug-cli connect` command to connect to an already running database service. Assume that a NeuG service is running at http://127.0.0.1:10001, you can use the following command to connect to the service.
```bash
neug-cli connect http://127.0.0.1:10001
```
```
INFO:neug:Build directory: /workspaces/neug/tools/python_bind/neug/../build/lib.linux-x86_64-3.10
INFO:neug:Adding build directory to sys.path: /workspaces/neug/tools/python_bind/neug/../build/lib.linux-x86_64-3.10
Connecting to 127.0.0.1:10001
INFO:neug.session:Opening session at endpoint: http://127.0.0.1:10001/ with timeout: 300, num_threads: 1
INFO:neug.session:Session initialized with endpoint: http://127.0.0.1:10001/ and timeout: 300
INFO:neug:Connection established.
Welcome to the NeuG shell. Type :help for usage hints.

neug > 
```

## Shell Command
List all available commands by running `neug-cli --help`
```
neug-cli --help
Usage: neug-cli [OPTIONS] COMMAND [ARGS]...

  NeuG CLI Tool.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  connect  Connect to a remote database.
  open     Open a local database.
```
### :help
Show built-in command list within the NeuG shell.
```
neug > :help

            Usage hints:
            - Enter Cypher queries directly to execute them on the connected database.
            - Use :help to display this help message.
            - Use :quit to leave the shell.
            - Use :max_rows <number> to set the maximum number of rows to display for query results.
            - Use :ui <endpoint> to start a web ui service on endpoint.
            - Multi-line commands are supported. Use ';' at the end to execute.
            - Command history is supported; use the up/down arrow keys to navigate previous commands.
```
### :quit
Exit from shell. Alternatively, you can use `Ctrl + C` to exit the shell.

### :max_rows [max_rows]
Set maximum number of rows for display.

### :ui [service_endpoint]
Start a web ui service on service_endpoint.(Default endpoint: 127.0.0.1:5000).
After starting the web service, you can access NeuG's web service via a browser at `http://service_endpoint`.