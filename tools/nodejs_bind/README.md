# The Node.js binding API for NeuG

## Prerequisites

Same system dependencies as NeuG (CMake >= 3.16, C++20 compiler), plus:

- Node.js >= 20.0.0
- macOS package builds only: `delocate==0.13.0`
- Linux package builds only: `ldd` and `patchelf==0.19.0`

Install the macOS packaging dependency with:

```bash
python3 -m pip install delocate==0.13.0
```

On Linux, install `patchelf==0.19.0` from the system package manager or the
[official releases](https://github.com/NixOS/patchelf/releases):

```bash
patchelf --version
```

### Installing Node.js

NeuG Node.js bindings require **Node.js >= 20.0.0** (N-API v8). Install via nvm:

```bash
# Install nvm
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash

# Reload shell in Linux
source ~/.bashrc
# Or MacOS
source ~/.zshrc

# Install Node.js LTS (v22)
nvm install --lts && nvm use --lts
# Or install a specific version (>= 20)
nvm install 20 && nvm use 20

# Verify
node -v   # should be >= 20.0.0
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
2. Copy NeuG's first-party binaries into `build/Release/<platform>/`:
   - `neug_node_bind.node` — the native addon
   - `libneug.so` / `libneug.dylib` — core shared library
3. Collect the third-party libraries actually selected at link time:
   - macOS uses `delocate-path` to copy them into `.dylibs/` and rewrite their
     install names.
   - Linux uses `ldd` to resolve the dependency closure, copies non-system
     libraries into `.libs/`, and uses `patchelf` to set package-relative
     `$ORIGIN` RPATHs.
4. Print the bundled native dependencies for inspection.
5. Run `npm pack` to produce `neug-<version>.tgz`

Npm package builds always force `NEUG_PACKAGE_BUILD=ON` and
`NEUG_NATIVE_ARCH=OFF`.
For local source builds, use `NEUG_NATIVE_ARCH=ON make dev`.

The resulting tarball can be installed without a C++ build environment:

```bash
npm install ./neug-0.2.0.tgz
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
const { Database } = require('neug');

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

## Explicit Transactions

Since v0.2, the embedded Node.js `Connection` API supports explicit AP
transactions. Auto-commit remains the default; begin a transaction only when
multiple ordinary queries must share one private view and one final commit.

```js
const conn = db.connect();

conn.beginTransaction();
conn.execute("CREATE (p:person {id: 3, name: 'Carol'});");
// Reads on this connection see the new vertex before publication.
conn.execute('MATCH (p:person) RETURN p.name;');
conn.commit();

conn.beginTransaction({ readOnly: true });
// This transaction pins one read view and rejects writes.
conn.execute('MATCH (p:person) RETURN p.name;');
conn.rollback();
```

`hasActiveTransaction` remains true after a failed statement because the
connection is rollback-only. Call `rollback()` before issuing another query.
Nested transactions, read-to-write upgrades, Cypher `BEGIN`/`COMMIT`/`ROLLBACK`,
and explicit-transaction COPY, batch, index, checkpoint, procedure, and
temporary-schema operations are not supported.
