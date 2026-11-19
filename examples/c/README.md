# NeuG Lite C API binary package

Use an embedded NeuG database through one C header and a native shared library.
This package excludes Python/Node bindings, the HTTP server and optional
extensions. Required third-party code, including protobuf, is linked into the
library. Only operating-system libraries are loaded separately. C++ applications
can use the same C API.

Download the archive and its `.sha256` file from the matching
[GitHub Release](https://github.com/alibaba/neug/releases). The release workflow
packages Linux x86_64, macOS ARM64 and Windows x86_64. These packages are not
Android or iOS builds.

Choose a package matching your operating system and CPU architecture. Keep the
header and library from the same release. `manifest.json` records build settings,
system dependencies and file hashes.

| Platform | Runtime library | Additional link-time file |
| --- | --- | --- |
| Linux | `lib/libneug.so` | None |
| macOS | `lib/libneug.dylib` | None |
| Windows | `bin/neug.dll` | `lib/neug.lib` (MSVC import library) |

Every package contains `include/neug/c/neug.h`. No other NeuG headers, protobuf
installation or NeuG source checkout is needed. Windows `.lib` is used when
linking C/C++ applications; it is not another DLL to deploy. Linux and Windows
packages include their C++ runtimes in the library.

## Extract and run

Check the archive against its adjacent SHA256 file before extracting. On Linux,
use `sha256sum -c <archive>.sha256`; on macOS, use `shasum -a 256 -c
<archive>.sha256`. In PowerShell, compare `(Get-FileHash <archive>
-Algorithm SHA256).Hash` with the hash in `<archive>.sha256`.

```sh
tar -xzf <archive>.tar.gz
```

From the extracted directory, with CMake and a C compiler installed:

```sh
cmake -S examples -B example-build -DCMAKE_PREFIX_PATH="$PWD"

cmake --build example-build --config Release

./example-build/neug_c_example
```

On Windows, use a Visual Studio developer PowerShell and run
`.\example-build\Release\neug_c_example.exe` with the default Visual Studio
generator. The example build copies `neug.dll` next to the executable.
The example prints `1 | O'Connor`.

For your own application, add `include` to the header search path and link against
`neug` in `lib`. On Linux/macOS, configure a runtime search path to the deployed
library directory. On Windows, place `neug.dll` beside your executable. Library
objects must be released using the C API functions, never the caller's `free`.

## Verification and compatibility

`examples/verify.c` checks parameters, commit, rollback, recovery after a query
error, and persistence across separate processes. To build it as both C and C++,
add `-DNEUG_VERIFY_PACKAGE=ON` to the CMake command above and rebuild. Run each
verification executable with a separate, new test database directory:

```sh
./example-build/verify_c ./test-c-db <version> write

./example-build/verify_c ./test-c-db <version> read
```

Repeat with `verify_cpp` and another database directory. With the Windows Visual
Studio generator, use `.\example-build\Release\verify_c.exe` or `verify_cpp.exe`.
Use the version in `manifest.json`. Each successful phase prints `PASS: C API
package` followed by `write` or `read`.

- Linux CI builds on Ubuntu 22.04. The binary's minimum glibc version is recorded
  in `manifest.json`; this is not a musl/Alpine package.
- macOS records its deployment target. That setting does not mean every macOS
  version has been tested. The library uses `@rpath/libneug.dylib` with an ad-hoc
  signature; sign it appropriately when distributing your application.
- Windows CI uses MSVC on Windows Server 2022 with a static C/C++ runtime. The
  import library is intended for MSVC-compatible consumers.

See `neug.h` for ownership, error handling and transaction rules.
