# Neug Documentation

```bash
make requirements
make html
```

## Knows Issues

### rpath issue

You may encouter some rpath issues on macos when building the documentation. 
```
Traceback (most recent call last):
    File "/Users/zhanglei/code/nexg/tools/python_bind/neug/__init__.py", line 136, in <module>
    import neug_py_bind
ImportError: dlopen(/Users/zhanglei/code/nexg/tools/python_bind/neug/../build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so, 0x0002): Library not loaded: @rpath/libboost_filesystem.dylib
    Referenced from: <6ED7EBF8-EE2E-42E9-BC3A-585E1EF45217> /Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/neug_py_bind.cpython-313-darwin.so
    Reason: tried: '/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/Users/zhanglei/code/nexg/tools/python_bind/build/lib.macosx-15.0-arm64-cpython-313/libboost_filesystem.dylib' (no such file), '/opt/homebrew/lib/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/opt/homebrew/lib/libboost_filesystem.dylib' (no such file), '/opt/homebrew/lib/libboost_filesystem.dylib' (no such file), '/System/Volumes/Preboot/Cryptexes/OS/opt/homebrew/lib/libboost_filesystem.dylib' (no such file)
```

You could resolve this issue by 

```bash
install_name_tool -add_rpath {GRAPHSCOPE_HOME} ./path/to/tools/python_bind/neug_py_bind.cpython-313-darwin.so
install_name_tool -add_rpath {GRAPHSCOPE_HOME} ./path/to/tools/python_bind/libneug_libraries.dylib
```

Where `$GRAPHSCOPE_HOME}` is the path where the depdencies are installed, and replace `/path/to/tools/python_bind` with the actual path.