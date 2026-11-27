"""Release boundaries: only complete, matching assets may be published or reused."""

import hashlib
import io
import json
import shutil
import tarfile
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from scripts import package_c_api as package
from scripts import release_c_api as release
from scripts import resolve_reuse


class ReleaseChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.assets = self.root / "assets"
        self.assets.mkdir()
        self.platforms = list(release.PLATFORMS)
        self.commit = "a" * 40
        for platform in self.platforms:
            self.make_archive(platform)

    def make_archive(self, platform_id, **overrides):
        platform = platform_id
        name = release.asset_names("v1.2.3", [platform])[0]
        files = {
            p: b"test payload" for p in package.payload(release.PLATFORMS[platform])
        }
        metadata = {
            "version": "1.2.3",
            "platform": platform,
            "variant": "lite",
            "source_commit": self.commit,
            "source_has_local_changes": False,
            "files": {p: hashlib.sha256(data).hexdigest() for p, data in files.items()},
        }
        metadata.update(overrides)
        files["manifest.json"] = json.dumps(metadata).encode()
        archive = self.assets / name
        with tarfile.open(archive, "w:gz") as tar:
            for path, data in files.items():
                info = tarfile.TarInfo("package/" + path)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
        archive.with_name(name + ".sha256").write_text(
            package.sha256(archive) + "  " + name
        )

    def test_complete_set_and_metadata_rejections(self):
        files = release.validate_packages(
            self.assets, "v1.2.3", self.platforms, self.commit
        )
        self.assertEqual(len(files), len(self.platforms) * 2)
        for field, value in (
            ("version", "9.9.9"),
            ("platform", "linux-arm64"),
            ("variant", "full"),
            ("source_commit", "b" * 40),
            ("source_has_local_changes", True),
            ("files", {"../../escape": "0" * 64}),
        ):
            with self.subTest(field=field):
                self.make_archive("linux-x86_64", **{field: value})
                with self.assertRaises(RuntimeError):
                    release.validate_packages(
                        self.assets, "v1.2.3", self.platforms, self.commit
                    )
        self.make_archive("linux-x86_64")
        files[0].write_bytes(b"corrupt archive")
        with self.assertRaisesRegex(RuntimeError, "SHA256 mismatch"):
            release.validate_packages(self.assets, "1.2.3", self.platforms, self.commit)
        files[0].unlink()
        with self.assertRaisesRegex(RuntimeError, "Unexpected release files"):
            release.validate_packages(self.assets, "1.2.3", self.platforms, self.commit)

    def test_publish_rejects_bad_package_before_any_network_call(self):
        self.make_archive("osx-arm64", source_has_local_changes=True)
        with patch.object(release, "gh") as gh:
            with self.assertRaises(RuntimeError):
                release.publish(
                    self.assets, "owner/repo", "v1.2.3", self.platforms, self.commit
                )
            gh.assert_not_called()

    def test_publish_retry_skips_identical_assets_and_refuses_replacement(self):
        name = release.asset_names("1.2.3", self.platforms)[0]
        corrupt = False

        def gh(*args):
            if args[1] == "view":
                return json.dumps({"isDraft": False, "assets": [{"name": name}]})
            if args[1] == "download":
                target = Path(args[args.index("--dir") + 1]) / name
                shutil.copyfile(self.assets / name, target)
                if corrupt:
                    target.write_bytes(b"different published bytes")
            return ""

        with patch.object(release, "gh", side_effect=gh) as command:
            release.publish(
                self.assets, "owner/repo", "v1.2.3", self.platforms, self.commit
            )
            upload = command.call_args.args
            self.assertEqual(upload[:3], ("release", "upload", "v1.2.3"))
            self.assertNotIn(str(self.assets / name), upload)
            self.assertNotIn("--clobber", upload)
            command.reset_mock()
            corrupt = True
            with self.assertRaisesRegex(RuntimeError, "refusing to overwrite"):
                release.publish(
                    self.assets, "owner/repo", "v1.2.3", self.platforms, self.commit
                )
            self.assertFalse(any(c.args[1] == "upload" for c in command.call_args_list))

    def test_download_public_assets_and_validate_checksum(self):
        assets = [
            {"name": p.name, "browser_download_url": "https://example.test/" + p.name}
            for p in self.assets.iterdir()
        ]
        metadata = {"tag_name": "v1.2.3", "assets": assets}

        def response(url, **kwargs):
            return io.BytesIO((self.assets / url.rsplit("/", 1)[-1]).read_bytes())

        with patch.object(release, "public_release", return_value=metadata):
            with patch.object(release.urllib.request, "urlopen", side_effect=response):
                dest = self.root / "downloaded"
                release.download(dest, "owner/repo", "1.2.3", self.platforms)
                self.assertEqual(
                    set(p.name for p in dest.iterdir()),
                    set(p.name for p in self.assets.iterdir()),
                )
                archive = next(self.assets.glob("*.tar.gz"))
                archive.write_bytes(b"bad download")
                with self.assertRaisesRegex(RuntimeError, "SHA256 mismatch"):
                    release.download(
                        self.root / "corrupt", "owner/repo", "1.2.3", self.platforms
                    )
            metadata["assets"] = []
            with self.assertRaisesRegex(RuntimeError, "missing C API assets"):
                release.download(
                    self.root / "missing", "owner/repo", "1.2.3", self.platforms
                )

    def test_public_release_version_fallback(self):
        error = urllib.error.HTTPError("url", 404, "Not Found", {}, None)
        with patch.object(
            release.urllib.request,
            "urlopen",
            side_effect=[error, io.BytesIO(b'{"tag_name":"v1.2.3"}')],
        ) as request:
            self.assertEqual(
                release.public_release("owner/repo", "1.2.3")["tag_name"], "v1.2.3"
            )
            self.assertTrue(request.call_args.args[0].endswith("/tags/v1.2.3"))

    def test_reuse_requires_c_api_packages_not_just_logs(self):
        names = ["nodejs-main-123", "java-publication-123"]
        for platform in self.platforms:
            names.extend(
                [
                    resolve_reuse.wheel_prefix(platform) + "123",
                    f"nodejs-{platform}-123",
                    f"extension-packages-{platform}-123",
                    f"c-api-build-logs-{platform}-123",
                ]
            )
        self.assertTrue(resolve_reuse.is_complete(names, self.platforms, True))
        self.assertFalse(
            resolve_reuse.is_complete(names, self.platforms, True, self.platforms)
        )
        names.extend(f"c-api-{platform}-123" for platform in self.platforms)
        self.assertTrue(
            resolve_reuse.is_complete(names, self.platforms, True, self.platforms)
        )


if __name__ == "__main__":
    unittest.main()
