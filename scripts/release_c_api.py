#!/usr/bin/env python3
"""Publish verified Lite C API assets, or download them from a public release."""

import argparse
import json
import re
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

try:
    from . import package_c_api as package
except ImportError:
    import package_c_api as package

PLATFORMS = {
    "linux-x86_64": "Linux",
    "linux-arm64": "Linux",
    "osx-arm64": "Darwin",
    "windows-x86_64": "Windows",
    "windows-arm64": "Windows",
}


def version_number(tag):
    version = tag.removeprefix("v")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+[A-Za-z0-9.+-]*", version):
        raise ValueError(f"Invalid release version: {tag}")
    return version


def asset_names(version, platforms):
    version = version_number(version)
    names = []
    for platform in platforms:
        if platform not in PLATFORMS:
            raise ValueError(f"Unsupported C API platform: {platform}")
        archive = f"neug-c-api-{version}-lite-{platform}.tar.gz"
        names.extend([archive, archive + ".sha256"])
    if not names or len(names) != len(set(names)):
        raise ValueError("Select at least one platform, without duplicates")
    return names


def validate_packages(directory, version, platforms, commit=None):
    """Check the whole release set before uploading any of its files."""
    names = asset_names(version, platforms)
    actual = {p.name for p in directory.iterdir()}
    if actual != set(names):
        raise RuntimeError(f"Unexpected release files: {actual ^ set(names)}")
    for platform, name in zip(platforms, names[::2]):
        archive = directory / name
        expected = archive.with_name(name + ".sha256").read_text().split()[0]
        if package.sha256(archive) != expected:
            raise RuntimeError(f"Archive SHA256 mismatch: {name}")
        with tempfile.TemporaryDirectory() as temporary:
            root = package.extract_package(archive, Path(temporary) / "unpacked")
            metadata = json.loads((root / "manifest.json").read_text())
            if (
                metadata["version"] != version_number(version)
                or metadata["platform"] != platform
                or metadata["variant"] != "lite"
            ):
                raise RuntimeError(f"Release metadata mismatch: {name}")
            if commit and (
                metadata["source_commit"] != commit
                or metadata["source_has_local_changes"]
            ):
                raise RuntimeError(
                    f"Package was not built from the release commit: {name}"
                )
            files = package.payload(PLATFORMS[platform])
            actual = {
                p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
            }
            if set(metadata["files"]) != files or actual != files | {"manifest.json"}:
                raise RuntimeError(f"Package file list mismatch: {name}")
            for filename, digest in metadata["files"].items():
                if package.sha256(root / filename) != digest:
                    raise RuntimeError(f"Payload SHA256 mismatch: {filename}")
    return [directory / name for name in names]


def gh(*args):
    return subprocess.check_output(["gh", *args], text=True)


def publish(directory, repo, tag, platforms, commit):
    files = validate_packages(directory, tag, platforms, commit)
    release = json.loads(
        gh("release", "view", tag, "--repo", repo, "--json", "assets,isDraft")
    )
    if release["isDraft"]:
        raise RuntimeError("Only an already-published release can receive C API assets")
    existing = {asset["name"] for asset in release["assets"]}
    missing = []
    # Reruns may complete a partial upload, but must never replace a different
    # binary already published under the same name.
    with tempfile.TemporaryDirectory() as temporary:
        for file in files:
            if file.name not in existing:
                missing.append(str(file))
                continue
            gh(
                "release",
                "download",
                tag,
                "--repo",
                repo,
                "--pattern",
                file.name,
                "--dir",
                temporary,
            )
            if package.sha256(file) != package.sha256(Path(temporary) / file.name):
                raise RuntimeError(
                    f"Published asset differs; refusing to overwrite: {file.name}"
                )
    if missing:
        gh("release", "upload", tag, *missing, "--repo", repo)
    print(f"Published {len(files)} C API assets for {repo} {tag}")


def public_release(repo, version):
    endpoint = (
        "latest"
        if version == "latest"
        else "tags/" + urllib.parse.quote(version, safe="")
    )
    url = f"https://api.github.com/repos/{repo}/releases/{endpoint}"
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404 and version != "latest" and not version.startswith("v"):
            return public_release(repo, "v" + version_number(version))
        raise


def download(directory, repo, version, platforms):
    release = public_release(repo, version)
    tag = release["tag_name"]
    if version != "latest" and version_number(tag) != version_number(version):
        raise RuntimeError("Release tag differs from the requested version")
    names = asset_names(tag, platforms)
    assets = {
        asset["name"]: asset["browser_download_url"] for asset in release["assets"]
    }
    missing = set(names) - assets.keys()
    if missing:
        raise RuntimeError(f"Release is missing C API assets: {sorted(missing)}")
    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise RuntimeError("Download directory must be empty")
    for name in names:
        # No credentials: exercise the same public download path as users.
        with urllib.request.urlopen(assets[name], timeout=120) as response:
            with (directory / name).open("wb") as target:
                shutil.copyfileobj(response, target)
    validate_packages(directory, tag, platforms)
    print(f"Downloaded C API assets from {repo} {tag} to {directory}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("publish", "download"))
    parser.add_argument("--repo", required=True)
    parser.add_argument(
        "--version", required=True, help="Release tag, or latest for download"
    )
    parser.add_argument(
        "--platforms", required=True, help="Comma-separated C API platform IDs"
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--commit", help="Required source commit when publishing")
    args = parser.parse_args()
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", args.repo):
        parser.error("repo must be owner/name")
    platforms = args.platforms.split(",")
    if args.command == "publish":
        if not args.commit:
            parser.error("publish requires --commit")
        publish(args.directory, args.repo, args.version, platforms, args.commit)
    else:
        download(args.directory, args.repo, args.version, platforms)


if __name__ == "__main__":
    main()
