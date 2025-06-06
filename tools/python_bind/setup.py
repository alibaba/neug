#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import multiprocessing
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from setuptools import find_packages  # noqa: H301
from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py as _build_py

base_dir = os.path.dirname(__file__)

PLAT_TO_CMAKE = {
    "win32": "Win32",
    "win-amd64": "x64",
    "win-arm32": "ARM",
    "win-arm64": "ARM64",
}


def get_version(file):
    """Get the version of the package from the given file."""
    __version__ = ""

    if os.path.isfile(file):
        with open(file, "r", encoding="utf-8") as fp:
            __version__ = fp.read().strip()
    else:
        __version__ = "0.1.0"

    return __version__


version = get_version(os.path.join(base_dir, "VERSION"))
repo_root = os.path.abspath(os.path.join(base_dir, "..", ".."))


class CMakeExtension(Extension):
    def __init__(self, name: str, sourcedir: str = "") -> None:
        super().__init__(name, sources=[])
        self.sourcedir = os.fspath(Path(sourcedir).resolve())


class CMakeBuild(build_ext):
    
    def initialize_options(self):
        super().initialize_options()
        # We set the build_temp to the local build/ directory
        self.build_temp = Path.cwd() / "build"
        
    def run(self):
        # Currently uncommented to avoid containing the jar in the wheel
        self.download_compiler_jar()
        super().run()
        
    def download_compiler_jar(self):
        resource_ur = "https://graphscope.oss-cn-beijing.aliyuncs.com/compiler/compiler-0.0.1-SNAPSHOT-shade-0521.jar"
        target_dir = "nexg/resources"
        target_file = os.path.join(target_dir, "compiler.jar")
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        if not os.path.exists(target_file):
            try:
                import urllib.request

                urllib.request.urlretrieve(resource_ur, target_file)
            except Exception as e:
                print(f"Failed to download {resource_ur}: {e}")
                raise RuntimeError(
                    f"Failed to download {resource_ur}. Please download it manually and place it in {target_dir}."
                )
        else:
            print(f"{target_file} already exists. Skipping download.")
    
    def build_extension(self, ext: CMakeExtension) -> None:
        # Must be in this form due to bug in .resolve() only fixed in Python 3.10+
        ext_fullpath = Path.cwd() / self.get_ext_fullpath(ext.name)
        extdir = ext_fullpath.parent.resolve()

        # Using this requires trailing slash for auto-detection & inclusion of
        # auxiliary "native" libs

        debug = int(os.environ.get("DEBUG", 0))
        cfg = "DEBUG" if debug else "Release"
        build_executables = "ON" if os.environ.get("BUILD_EXECUTABLES", "OFF") == "ON" else "OFF"
        build_test = "OFF"
        if os.environ.get("BUILD_TEST", "OFF") == "ON":
            build_test = "ON" 
        # cfg is now dynamically set based on the DEBUG environment variable

        # CMake lets you override the generator - we need to check this.
        # Can be set with Conda-Build, for example.
        cmake_generator = os.environ.get("CMAKE_GENERATOR", "")

        # Set Python_EXECUTABLE instead if you use PYBIND11_FINDPYTHON
        # EXAMPLE_VERSION_INFO shows you how to pass a value into the C++ code
        # from Python.
        cmake_args = [
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}{os.sep}",
            f"-DPYTHON_EXECUTABLE={sys.executable}",
            f"-DCMAKE_BUILD_TYPE={cfg}",  # not used on MSVC, but no harm
            f"-DOPTIMIZE_FOR_HOST=OFF",
            f"-DBUILD_EXECUTABLES={build_executables}",
            f"-DBUILD_TEST={build_test}"
        ]
        build_args = []
        # Adding CMake arguments set as environment variable
        # (needed e.g. to build for ARM OSx on conda-forge)
        if "CMAKE_ARGS" in os.environ:
            cmake_args += [item for item in os.environ["CMAKE_ARGS"].split(" ") if item]

        if self.compiler.compiler_type != "msvc":
            # Using Ninja-build since it a) is available as a wheel and b)
            # multithreads automatically. MSVC would require all variables be
            # exported for Ninja to pick it up, which is a little tricky to do.
            # Users can override the generator with CMAKE_GENERATOR in CMake
            # 3.15+.
            if not cmake_generator or cmake_generator == "Ninja":
                try:
                    import ninja

                    ninja_executable_path = Path(ninja.BIN_DIR) / "ninja"
                    cmake_args += [
                        "-GNinja",
                        f"-DCMAKE_MAKE_PROGRAM:FILEPATH={ninja_executable_path}",
                    ]
                except ImportError:
                    pass

        else:
            # Single config generators are handled "normally"
            single_config = any(x in cmake_generator for x in {"NMake", "Ninja"})

            # CMake allows an arch-in-generator style for backward compatibility
            contains_arch = any(x in cmake_generator for x in {"ARM", "Win64"})

            # Specify the arch if using MSVC generator, but only if it doesn't
            # contain a backward-compatibility arch spec already in the
            # generator name.
            if not single_config and not contains_arch:
                cmake_args += ["-A", PLAT_TO_CMAKE[self.plat_name]]

            # Multi-config generators have a different way to specify configs
            if not single_config:
                cmake_args += [
                    f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{cfg.upper()}={extdir}"
                ]
                build_args += ["--config", cfg]

        if sys.platform.startswith("darwin"):
            # Cross-compile support for macOS - respect ARCHFLAGS if set
            archs = re.findall(r"-arch (\S+)", os.environ.get("ARCHFLAGS", ""))
            if archs:
                cmake_args += ["-DCMAKE_OSX_ARCHITECTURES={}".format(";".join(archs))]

        # Set CMAKE_BUILD_PARALLEL_LEVEL to control the parallel build level
        # across all generators.
        if "CMAKE_BUILD_PARALLEL_LEVEL" not in os.environ:
            # self.parallel is a Python 3 only way to set parallel jobs by hand
            # using -j in the build_ext call, not supported by pip or PyPA-build.
            build_args += [f"-j8"]
        else:
            # If the user has set CMAKE_BUILD_PARALLEL_LEVEL, we respect that.
            build_args += [f"-j{os.environ['CMAKE_BUILD_PARALLEL_LEVEL']}"]

        build_temp = Path(self.build_temp) / ext.name
        if not build_temp.exists():
            build_temp.mkdir(parents=True)

        # find cmake executable
        cmake_executable = shutil.which("cmake")
        if cmake_executable is None:
            raise RuntimeError("CMake executable not found in PATH.")

        print(f"cmake command: {cmake_executable}, args: {cmake_args}")
        subprocess.run(
            [cmake_executable, ext.sourcedir, *cmake_args], cwd=build_temp, check=True
        )
        print(f"build args: {build_args}")
        subprocess.run(
            [cmake_executable, "--build", ".", *build_args],
            cwd=build_temp,
            check=True,
        )
        
    def copy_extensions_to_source(self):
        pass


class BuildExtFirst(_build_py):
    # Override the build_py command to build the extension first.
    def run(self):
        self.run_command("build_ext")
        return super().run()


setup(
    name="nexg",
    version=version,
    author="GraphScope Team",
    author_email="graphscope@alibaba-inc.com",
    url="https://github.com/alibaba/nexg",
    # license="Apache License 2.0",
    # classifiers=[
    #     "Development Status :: 5 - Production/Stable",
    #     "Intended Audience :: Developers",
    #     "Intended Audience :: Science/Research",
    #     "License :: OSI Approved :: Apache Software License",
    #     "Topic :: Software Development :: Libraries",
    #     "Operating System :: MacOS :: MacOS X",
    #     "Operating System :: POSIX",
    #     "Programming Language :: Python",
    #     "Programming Language :: Python :: 3",
    #     "Programming Language :: Python :: 3.7",
    #     "Programming Language :: Python :: 3.8",
    #     "Programming Language :: Python :: 3.9",
    #     "Programming Language :: Python :: 3.10",
    #     "Programming Language :: Python :: 3.11",
    # ],
    ext_modules=[CMakeExtension(name="nexg_py_bind", sourcedir=repo_root)],
    description="GraphScope NexG.",
    long_description=open(os.path.join(base_dir, "README.md"), "r").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests"]),
    package_data={"nexg": ["VERSION", "resources/*"]},
    zip_safe=True,
    include_package_data=True,
    cmdclass={
        "build_py": BuildExtFirst,
        "build_ext": CMakeBuild,
    },
)
