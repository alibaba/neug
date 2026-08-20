@REM Copyright 2020 Alibaba Group Holding Limited.
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM 	http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.

@echo off
call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64

REM cd to project root (parent of scripts/)
cd /d "%~dp0.."

REM Use PYTHON_EXE from environment if set, otherwise default to "python"
if "%PYTHON_EXE%"=="" set PYTHON_EXE=python
REM Use VCPKG_ROOT from environment if set, otherwise use default path
if "%VCPKG_ROOT%"=="" set VCPKG_ROOT=C:\vcpkg
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE="%VCPKG_ROOT%\scripts\buildsystems\vcpkg.cmake" -DVCPKG_TARGET_TRIPLET=x64-windows-static-md -DOPENSSL_ROOT_DIR="%VCPKG_ROOT%\installed\x64-windows-static-md" -DBUILD_PYTHON=ON -DPython_EXECUTABLE="%PYTHON_EXE%" -DPYTHON_EXECUTABLE="%PYTHON_EXE%" .
cmake --build build -j 8 --target neug neug_py_bind
