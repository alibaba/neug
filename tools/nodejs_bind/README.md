# The Node.js binding API for NeuG

## Prerequisites

Same system dependencies as NeuG (CMake >= 3.16, C++20 compiler), plus:

- Node.js >= 18.0.0

### Installing Node.js

NeuG Node.js bindings require **Node.js >= 18.0.0** (N-API v8). Install via nvm:

```bash
# Install nvm
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash

# Reload shell in Linux
source ~/.bashrc
# Or MacOS
source ~/.zshrc

# Install Node.js LTS (v22)
nvm install --lts && nvm use --lts
# Or install a specific version (>= 18)
nvm install 18 && nvm use 18

# Verify
node -v   # should be >= 18.0.0
npm -v
```

## Building

### Build

```bash
cd tools/nodejs_bind
make build
```

This will:
1. Install Node.js dependencies (`npm install`)
2. Build the native addon via the main NeuG CMake project (`-DBUILD_NODEJS=ON`)
3. Copy the resulting `neug_node_bind.node` to `build/Release/`


### Pack

```bash
make pack
```

Create a self-contained, distributable npm package tarball (`.tgz`). This will:

1. Build the native addon (same as `make build`)
2. Copy prebuilt binaries into `prebuilds/<platform>/`:
   - `neug_node_bind.node` — the native addon
   - `libneug.so` — core shared library
   - `libmimalloc.so.2` — memory allocator
3. Run `npm pack` to produce `neug-<version>.tgz`

The resulting tarball can be installed without a C++ build environment:

```bash
npm install ./neug-0.1.3.tgz
```

 
### Clean

```bash
make clean
```


## API Example

A complete runnable example is provided in [`example.js`](example.js):

```bash
node example.js
```


```js
const { Database } = require('./lib');

// Open an in-memory database
const db = new Database({ databasePath: '', mode: 'w' });
const conn = db.connect();

// Create schema
conn.execute('CREATE NODE TABLE person(id INT64, name STRING, age INT32, PRIMARY KEY(id));');
conn.execute('CREATE REL TABLE knows(FROM person TO person, since INT64);');

// Insert vertices
conn.execute("CREATE (p:person {id: 1, name: 'Alice', age: 30});");
conn.execute("CREATE (p:person {id: 2, name: 'Bob', age: 25});");

// Insert edge
conn.execute(
  "MATCH (a:person), (b:person) WHERE a.name = 'Alice' AND b.name = 'Bob' " +
  "CREATE (a)-[:knows {since: 2020}]->(b);"
);

// Query
const result = conn.execute(
  'MATCH (a:person)-[r:knows]->(b:person) RETURN a.name, r.since, b.name;'
);
for (const row of result) {
  console.log(row);
}

conn.close();
db.close();
```
