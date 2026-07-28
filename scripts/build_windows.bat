@echo off
call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64

REM cd to project root (parent of scripts/)
cd /d "%~dp0.."

set PYTHON_EXE=C:\Users\neng\AppData\Local\Programs\Python\Python311\python.exe
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=C:\Users\neng\vcpkg\scripts\buildsystems\vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-windows-static-md -DOPENSSL_ROOT_DIR=C:\Users\neng\vcpkg\installed\x64-windows-static-md -DBUILD_PYTHON=ON -DPython_EXECUTABLE=%PYTHON_EXE% -DPYTHON_EXECUTABLE=%PYTHON_EXE% .
cmake --build build -j 8 --target neug neug_py_bind
