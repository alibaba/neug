"""Check wheel staging without compiling or importing the native bindings."""

import os
import runpy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from setuptools import Distribution
from setuptools.command.install_lib import install_lib


class ExtensionCopyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_file = Path(__file__).resolve().parents[1] / "tools/python_bind/setup.py"
        with patch("setuptools.setup"):
            cls.backend = runpy.run_path(str(setup_file))

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.build = self.root / "build"
        self.extension = self.build / "extension/fts"
        self.extension.mkdir(parents=True)
        self.expected = {"libfts.neug_extension", "other.neug_extension"}
        for name in self.expected | {
            "fts_extension_test",
            "fts_extension_test.exe",
            "libhelper.a",
            "Makefile",
            "libfts.neug_extension.bak",
        }:
            (self.extension / name).write_bytes(name.encode())
        (self.extension / "directory.neug_extension").mkdir()
        self.environment = patch.dict(
            os.environ,
            NEUG_BUILD_DIR=str(self.build),
            CI_INSTALL_EXTENSIONS="fts",
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def assert_extensions(self, destination):
        self.assertEqual({p.name for p in destination.iterdir()}, self.expected)
        for name in self.expected:
            self.assertEqual((destination / name).read_bytes(), name.encode())

    def test_build_extension_excludes_build_artifacts(self):
        binding_dir = self.build / "tools/python_bind"
        binding_dir.mkdir(parents=True)
        (binding_dir / "neug_py_bind.test.so").write_bytes(b"binding")
        core_dir = self.build / "src"
        core_dir.mkdir()
        (core_dir / "libneug.dylib").write_bytes(b"core")
        stage = self.root / "stage"
        driver = SimpleNamespace(
            _write_commit_info=lambda: None,
            _cmake_build_in_root=lambda path: None,
            inplace=False,
            get_ext_fullpath=lambda name: str(stage / "neug_py_bind.test.so"),
        )
        self.backend["CMakeBuild"].build_extension(
            driver, SimpleNamespace(name="neug_py_bind")
        )
        self.assert_extensions(stage / "extension/fts")
        self.assertEqual((stage / "libneug.dylib").read_bytes(), b"core")
        self.assertEqual((stage / "neug_py_bind.test.so").read_bytes(), b"binding")

    def test_install_lib_excludes_build_artifacts(self):
        command = self.backend["InstallLib"](Distribution())
        command.build_dir = str(self.build)
        command.install_dir = str(self.root / "installed")
        with patch.object(install_lib, "run"):
            command.run()
        self.assert_extensions(self.root / "installed/extension/fts")


if __name__ == "__main__":
    unittest.main()
