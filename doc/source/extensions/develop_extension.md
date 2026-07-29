# Developing Out-of-Tree Extensions

NeuG can build extensions that live **outside** the NeuG source tree. This is the recommended layout when NeuG is a git submodule (or sibling checkout) of your extension repository.

## Layout

```text
my-extension/
  neug/                      # NeuG submodule or sibling
  extension_config.cmake     # registers this repo with NeuG
  CMakeLists.txt             # extension build rules
  src/
  include/
```

## Register the extension

Create `extension_config.cmake` at the root of your extension repo:

```cmake
neug_extension_load(my_ext
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
```

`neug_extension_load` records the extension name and source directory so NeuG's build can `add_subdirectory` it.

## Configure and build

Open **NeuG** as the CMake project (not the extension repo root), and pass:

- `BUILD_EXTENSIONS` — semicolon-separated names to build (`my_ext`, and optionally builtins such as `parquet`)
- `NEUG_EXTENSION_CONFIGS` — path(s) to `extension_config.cmake` files

Relative entries in `NEUG_EXTENSION_CONFIGS` are resolved from the NeuG source tree root (`CMAKE_SOURCE_DIR`).

```sh
cmake -S my-extension/neug -B my-extension/build/release \
  -DBUILD_EXTENSIONS=my_ext \
  -DNEUG_EXTENSION_CONFIGS=/absolute/path/to/my-extension/extension_config.cmake

cmake --build my-extension/build/release --target neug_my_ext_extension -j$(nproc)
```

Built-in extensions under `neug/extension/` (`parquet`, `pattern_matching`, `gds`, `httpfs`) do not need `NEUG_EXTENSION_CONFIGS`; list them in `BUILD_EXTENSIONS` only.

If `BUILD_EXTENSIONS` names an extension that is neither built-in nor registered via `neug_extension_load`, configure fails with a clear error.

## Artifacts and LOAD

The shared library is written to:

```text
<build>/extension/<name>/lib<name>.neug_extension
```

At runtime, name-based `LOAD <name>` resolves under `$NEUG_EXTENSION_HOME_PYENV/extension/<name>/`. Point that home at your NeuG build directory (or copy/symlink the artifact into the directory used by your installed NeuG), then:

```cypher
LOAD my_ext;
```

Build the extension against the same NeuG version as the runtime you load into.
