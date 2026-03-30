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

import glob
import multiprocessing
import os
import re
import shutil
import subprocess
import sys

if sys.version_info < (3, 12):
    from distutils.cmd import Command
from pathlib import Path

from setuptools import Extension
from setuptools import find_packages  # noqa: H301
from setuptools import setup
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.install_lib import install_lib as _install_lib

if sys.version_info >= (3, 12):
    from setuptools import Command  # noqa: F811

base_dir = os.path.dirname(__file__)
repo_root = os.path.abspath(os.path.join(base_dir, "..", ".."))

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
        pkg_info = os.path.join(base_dir, "PKG-INFO")
        if os.path.isfile(pkg_info):
            with open(pkg_info, "r", encoding="utf-8") as fp:
                for line in fp:
                    if line.startswith("Version: "):
                        __version__ = line.split("Version: ", 1)[1].strip()
                        break
        if not __version__:
            __version__ = "0.1.0"

    return __version__


version = get_version(os.path.join(repo_root, "NEUG_VERSION"))


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
        super().run()

    def build_extension(self, ext: CMakeExtension) -> None:  # noqa: C901
        # Must be in this form due to bug in .resolve() only fixed in Python 3.10+
        ext_fullpath = Path.cwd() / self.get_ext_fullpath(ext.name)
        extdir = ext_fullpath.parent.resolve()

        # Using this requires trailing slash for auto-detection & inclusion of
        # auxiliary "native" libs

        build_type = os.environ.get("BUILD_TYPE")
        if build_type is None:
            # Default to Release if not set
            build_type = "Release"
        build_type = build_type.upper()
        if build_type not in {"DEBUG", "RELEASE"}:
            raise ValueError(
                f"Invalid BUILD_TYPE: {build_type}. Must be one of 'DEBUG' or 'RELEASE'."
            )

        build_executables = (
            "ON" if os.environ.get("BUILD_EXECUTABLES", "OFF") == "ON" else "OFF"
        )
        build_http_server = (
            "ON" if os.environ.get("BUILD_HTTP_SERVER", "ON") == "ON" else "OFF"
        )
        build_compiler = (
            "ON" if os.environ.get("BUILD_COMPILER", "ON") == "ON" else "OFF"
        )
        enable_backtraces = (
            "ON" if os.environ.get("ENABLE_BACKTRACES", "OFF") == "ON" else "OFF"
        )
        with_mimalloc = (
            "ON" if os.environ.get("WITH_MIMALLOC", "OFF") == "ON" else "OFF"
        )
        build_extensions = os.environ.get("BUILD_EXTENSIONS", "")
        cmake_install_prefix = os.environ.get("CMAKE_INSTALL_PREFIX", None)
        use_ninja = os.environ.get("USE_NINJA", "OFF") == "ON"
        build_test = "OFF"
        if os.environ.get("BUILD_TEST", "OFF") == "ON":
            build_test = "ON"
        enable_gcov = "OFF"
        if os.environ.get("ENABLE_GCOV", "OFF") == "ON":
            enable_gcov = "ON"
        # cfg is now dynamically set based on the DEBUG environment variable

        # CMake lets you override the generator - we need to check this.
        # Can be set with Conda-Build, for example.
        cmake_generator = os.environ.get("CMAKE_GENERATOR", "")

        # Set Python_EXECUTABLE instead if you use PYBIND11_FINDPYTHON
        # EXAMPLE_VERSION_INFO shows you how to pass a value into the C++ code
        # from Python.
        cmake_library_output_dir = f"{extdir}{os.sep}"
        cmake_args = [
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={cmake_library_output_dir}",
            f"-DPYTHON_EXECUTABLE={sys.executable}",  # legacy variable
            f"-DPython_EXECUTABLE={sys.executable}",  # used by find_package(Python) / pybind11
            f"-DCMAKE_BUILD_TYPE={build_type}",  # not used on MSVC, but no harm
            "-DOPTIMIZE_FOR_HOST=OFF",
            f"-DBUILD_EXECUTABLES={build_executables}",
            f"-DBUILD_TEST={build_test}",
            f"-DBUILD_COMPILER={build_compiler}",
            f"-DENABLE_BACKTRACES={enable_backtraces}",
            f"-DBUILD_HTTP_SERVER={build_http_server}",
            f"-DWITH_MIMALLOC={with_mimalloc}",
            f"-DENABLE_GCOV={enable_gcov}",
            "-DCMAKE_POLICY_VERSION_MINIMUM=3.5",
        ]
        if build_extensions:
            cmake_args.append(f"-DBUILD_EXTENSIONS={build_extensions}")
        install_extensions = os.environ.get("CI_INSTALL_EXTENSIONS", "")
        if install_extensions:
            cmake_args.append(f"-DBUILD_EXTENSIONS={install_extensions}")
        if cmake_install_prefix:
            cmake_args += [
                f"-DCMAKE_INSTALL_PREFIX={cmake_install_prefix}",
            ]
        if use_ninja:
            cmake_args += ["-GNinja"]
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
                    f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{build_type.upper()}={extdir}"
                ]
                build_args += ["--config", build_type]

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
            build_args += ["-j8"]
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


class BuildProto(Command):
    description = "build protobuf file"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def generate_proto(self, proto_path, output_dir, proto_files=None):
        """
        Generate Python code from protobuf files.

        Args:
            proto_path (str): Directory containing .proto files.
            output_dir (str): Directory to place generated Python files.
            proto_files (list, optional): Specific .proto files to generate.
                If None, all .proto files in `proto_path` are used.

        Raises:
            RuntimeError: If `proto_path` does not exist.

        Note:
            This function is currently a placeholder and does not generate files, as the code is commented out.
            Generated protobuf files are checked into the repository. In the future,
            if proto is no longer used for the physical plan protocol, this code may be removed.

            We avoid requiring protoc in the user's environment to simplify installation.
        """
        pass
        if proto_files is None:
            proto_files = glob.glob(os.path.join(proto_path, "*.proto"))
        os.makedirs(output_dir, exist_ok=True)
        # find protoc executable
        protoc_executable = shutil.which("protoc")
        if protoc_executable is None:
            # trying /opt/neug/bin/protoc
            protoc_executable = "/opt/neug/bin/protoc"
        for proto_file in proto_files:
            if not os.path.exists(proto_file):
                proto_file = os.path.join(proto_path, proto_file)
            cmd = [
                protoc_executable,
                f"--proto_path={proto_path}",
                f"--python_out={output_dir}",
                proto_file,
            ]
            print(f"Running command: {' '.join(cmd)}")
            subprocess.check_call(
                cmd,
                stderr=subprocess.STDOUT,
            )

    def run(self):
        proto_path = os.path.join(repo_root, "proto")
        output_dir = os.path.join(base_dir, "neug", "proto")
        if not os.path.exists(proto_path):
            raise RuntimeError(f"Proto path {proto_path} does not exist.")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        self.generate_proto(
            proto_path,
            output_dir,
            [
                "common.proto",
                "common.proto",
                "expr.proto",
                "type.proto",
                "basic_type.proto",
                "error.proto",
            ],
        )


class BuildExtFirst(_build_py):
    # Override the build_py command to build the extension first.
    def run(self):
        # self.run_command("build_proto")
        self.run_command("build_ext")
        return super().run()


class InstallLib(_install_lib):
    """Ensure extension/* (e.g. extension/json/libjson.neug_extension) is copied.

    CMake writes native extensions to build_lib/extension/<name>/.
    Only runs when CI_INSTALL_EXTENSIONS is set (semicolon-separated, e.g. json;parquet).
    Copies only the listed extensions so the wheel gets site-packages/extension/...
    """

    def run(self):
        super().run()
        # Only copy extensions when INSTALL_EXTENSIONS is set (e.g. json;parquet)
        install_extensions = os.environ.get("CI_INSTALL_EXTENSIONS", "").strip()
        print(
            f"[InstallLib] INSTALL_EXTENSIONS={repr(install_extensions)} "
            f"build_dir={self.build_dir!r} install_dir={self.install_dir!r}"
        )
        sys.stdout.flush()
        if not install_extensions:
            print("[InstallLib] Skip extension copy (INSTALL_EXTENSIONS empty)")
            sys.stdout.flush()
            return
        names = [n.strip() for n in install_extensions.split(";") if n.strip()]
        if not names:
            print("[InstallLib] Skip extension copy (no names after split)")
            sys.stdout.flush()
            return
        ext_src_base = os.path.join(self.build_dir, "extension")
        ext_dst_base = os.path.join(self.install_dir, "extension")
        print(
            f"[InstallLib] ext_src_base={ext_src_base!r} exists={os.path.isdir(ext_src_base)}"
        )
        sys.stdout.flush()
        if not os.path.isdir(ext_src_base):
            print("[InstallLib] Skip (extension source dir missing)")
            sys.stdout.flush()
            return
        for name in names:
            src = os.path.join(ext_src_base, name)
            if not os.path.isdir(src):
                continue
            dst = os.path.join(ext_dst_base, name)
            os.makedirs(dst, exist_ok=True)
            for f in os.listdir(src):
                s = os.path.join(src, f)
                d = os.path.join(dst, f)
                if os.path.isfile(s):
                    shutil.copy2(s, d)
            print(f"[InstallLib] Copied extension: {name} -> {dst}")
            sys.stdout.flush()


setup(
    name="neug",
    version=version,
    author="GraphScope Team",
    author_email="graphscope@alibaba-inc.com",
    url="https://github.com/alibaba/neug",
    ext_modules=[CMakeExtension(name="neug_py_bind", sourcedir=repo_root)],
    description="GraphScope NeuG.",
    long_description=open(os.path.join(base_dir, "README.md"), "r").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["tests"]),
    package_data={"neug": ["resources/*"]},
    zip_safe=False,
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "neug-cli=neug.neug_cli:cli",
        ],
    },
    install_requires=[
        "packaging>=24.2",
        "protobuf==5.29.6",
        "requests",
        "click>=8.0.0",
        "tabulate>=0.9.0",
        "PyYAML>=6.0.2",
        "tqdm",
        "Flask",
        "Flask-Cors",
    ],
    cmdclass={
        "build_py": BuildExtFirst,
        "build_ext": CMakeBuild,
        "build_proto": BuildProto,
        "install_lib": InstallLib,
    },
)
