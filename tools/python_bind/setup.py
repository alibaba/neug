#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import glob
import os
import re
import shutil
import subprocess
import sys

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
            __version__ = "0.1.3"

    return __version__


version = get_version(os.path.join(repo_root, "NEUG_VERSION"))


class CMakeExtension(Extension):
    def __init__(self, name: str, sourcedir: str = "") -> None:
        super().__init__(name, sources=[])
        self.sourcedir = os.fspath(Path(sourcedir).resolve())


# BUILD_* options whose default differs between setup.py (wheel/CI use case)
# and the root CMakeLists.txt. Keep these defaults stable — CI relies on them.
_ENV_FLAGS = {
    "BUILD_EXECUTABLES": "OFF",
    "BUILD_HTTP_SERVER": "ON",
    "BUILD_COMPILER": "ON",
    "ENABLE_BACKTRACES": "OFF",
    "WITH_MIMALLOC": "ON",
    "BUILD_TEST": "OFF",
    "ENABLE_GCOV": "OFF",
}


def _on_off(name: str, default: str) -> str:
    return "ON" if os.environ.get(name, default).upper() == "ON" else "OFF"


class CMakeBuild(build_ext):
    """Drive cmake against the root build tree, honoring env-var build options.

    Reruns `cmake configure + build` on every invocation so env-var changes
    (BUILD_*, WITH_*, CMAKE_*) take effect every time. cmake's reconfigure is
    nearly a no-op when nothing changed.

    Build dir defaults to `<repo>/build/`; override with NEUG_BUILD_DIR=...
    For non-inplace builds (bdist_wheel), copies neug_py_bind*.so and
    libneug.{dylib,so*} into extdir so the wheel ships them together.
    """

    def _write_commit_info(self) -> None:
        commit = os.environ.get("NEUG_COMMIT_SHA", "")
        if not commit:
            try:
                commit = subprocess.check_output(
                    ["git", "rev-parse", "HEAD"],
                    cwd=repo_root,
                    text=True,
                ).strip()
            except Exception:
                commit = "unknown"

        commit_file = Path(base_dir) / "neug" / "_commit.py"
        commit_file.write_text(f'__commit__ = "{commit}"\n', encoding="utf-8")
        print(f"[CMakeBuild] wrote commit info ({commit}) -> {commit_file}")

    def build_extension(self, ext: CMakeExtension) -> None:
        self._write_commit_info()

        build_dir = Path(
            os.environ.get("NEUG_BUILD_DIR", Path(repo_root) / "build")
        ).resolve()
        py_so_dir = build_dir / "tools" / "python_bind"
        core_lib_dir = build_dir / "src"

        self._cmake_build_in_root(build_dir)

        if not list(py_so_dir.glob("neug_py_bind*.so")):
            raise RuntimeError(f"neug_py_bind*.so not found in {py_so_dir} after build")

        if self.inplace:
            return

        extdir = (Path.cwd() / self.get_ext_fullpath(ext.name)).parent.resolve()
        extdir.mkdir(parents=True, exist_ok=True)
        for src in [
            *py_so_dir.glob("neug_py_bind*.so"),
            *core_lib_dir.glob("libneug.dylib"),
            *core_lib_dir.glob("libneug.so"),
            *core_lib_dir.glob("libneug.so.*"),
        ]:
            shutil.copy2(src, extdir, follow_symlinks=True)
            print(f"[CMakeBuild] copied {src.name} -> {extdir}")

        # Mirror <repo>/build/extension/<name>/* into <extdir>/extension/<name>/
        # so they get packaged into the wheel. Triggered by CI_INSTALL_EXTENSIONS
        # (which the wheel-build CI sets); InstallLib's copy then becomes
        # idempotent because the source dir already exists in build_lib.
        ext_names = [
            n.strip()
            for n in os.environ.get("CI_INSTALL_EXTENSIONS", "").split(";")
            if n.strip()
        ]
        ext_src_root = build_dir / "extension"
        for name in ext_names:
            src_dir = ext_src_root / name
            if not src_dir.is_dir():
                continue
            dst_dir = extdir / "extension" / name
            dst_dir.mkdir(parents=True, exist_ok=True)
            for f in src_dir.iterdir():
                if f.is_file():
                    shutil.copy2(f, dst_dir, follow_symlinks=True)
            print(f"[CMakeBuild] copied extension/{name}/ -> {dst_dir}")

    def _cmake_build_in_root(self, build_dir: Path) -> None:
        build_dir.mkdir(parents=True, exist_ok=True)
        cmake = shutil.which("cmake")
        if cmake is None:
            raise RuntimeError("CMake executable not found in PATH.")

        cmake_args, build_args = self._collect_cmake_args()
        print(f"[CMakeBuild] configuring at {build_dir} with {cmake_args}")
        subprocess.run([cmake, repo_root, *cmake_args], cwd=build_dir, check=True)

        # No --target: build everything cmake configured. When BUILD_EXECUTABLES
        # or BUILD_TEST is ON this includes bulk_loader / simple_example /
        # ctest binaries that CI exercises after `make build`.
        print(f"[CMakeBuild] building with {build_args}")
        subprocess.run([cmake, "--build", ".", *build_args], cwd=build_dir, check=True)

    def _collect_cmake_args(self) -> tuple[list[str], list[str]]:
        build_type = (os.environ.get("BUILD_TYPE") or "Release").upper()
        if build_type not in {"DEBUG", "RELEASE"}:
            raise ValueError(
                f"Invalid BUILD_TYPE: {build_type}. Must be 'DEBUG' or 'RELEASE'."
            )

        cmake_args = [
            f"-DPython_EXECUTABLE={sys.executable}",
            f"-DPYTHON_EXECUTABLE={sys.executable}",
            f"-DCMAKE_BUILD_TYPE={build_type}",
            "-DBUILD_PYTHON=ON",
            "-DCMAKE_POLICY_VERSION_MINIMUM=3.5",
            *(
                f"-D{name}={_on_off(name, default)}"
                for name, default in _ENV_FLAGS.items()
            ),
        ]

        # BUILD_EXTENSIONS may come from BUILD_EXTENSIONS or CI_INSTALL_EXTENSIONS.
        # CI sets the latter; both are valid and forwarded.
        for env_name in ("BUILD_EXTENSIONS", "CI_INSTALL_EXTENSIONS"):
            val = os.environ.get(env_name, "")
            if val:
                cmake_args.append(f"-DBUILD_EXTENSIONS={val}")

        if prefix := os.environ.get("CMAKE_INSTALL_PREFIX"):
            cmake_args.append(f"-DCMAKE_INSTALL_PREFIX={prefix}")
        if extra := os.environ.get("CMAKE_ARGS", "").split():
            cmake_args += extra

        cmake_args += [
            f"-DNEUG_PACKAGE_BUILD={_on_off('NEUG_PACKAGE_BUILD', 'ON')}",
            f"-DNEUG_NATIVE_ARCH={_on_off('NEUG_NATIVE_ARCH', 'OFF')}",
        ]

        cmake_generator = os.environ.get("CMAKE_GENERATOR", "")
        build_args: list[str] = []

        if self.compiler.compiler_type != "msvc":
            use_ninja = _on_off("USE_NINJA", "OFF") == "ON"
            if use_ninja or not cmake_generator or cmake_generator == "Ninja":
                try:
                    import ninja

                    cmake_args += [
                        "-GNinja",
                        f"-DCMAKE_MAKE_PROGRAM:FILEPATH={Path(ninja.BIN_DIR) / 'ninja'}",
                    ]
                except ImportError:
                    pass
        else:
            single_config = any(x in cmake_generator for x in {"NMake", "Ninja"})
            contains_arch = any(x in cmake_generator for x in {"ARM", "Win64"})
            if not single_config and not contains_arch:
                cmake_args += ["-A", PLAT_TO_CMAKE[self.plat_name]]
            if not single_config:
                build_args += ["--config", build_type]

        if sys.platform.startswith("darwin"):
            archs = re.findall(r"-arch (\S+)", os.environ.get("ARCHFLAGS", ""))
            if archs:
                cmake_args.append(f"-DCMAKE_OSX_ARCHITECTURES={';'.join(archs)}")

        build_args.append(f"-j{os.environ.get('CMAKE_BUILD_PARALLEL_LEVEL', '8')}")
        return cmake_args, build_args


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
    """Run build_ext before build_py so the wheel sees the freshly-built .so."""

    def run(self):
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
        "prompt_toolkit>=3.0.0",
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
