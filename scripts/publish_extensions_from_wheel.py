#!/usr/bin/env python3
"""Extract repaired extensions from a cibuildwheel wheel and publish them."""

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--extensions", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--skip-upload", action="store_true")
    return parser.parse_args()


def parse_extensions(value: str) -> list[str]:
    extensions = [name.strip() for name in value.split(";") if name.strip()]
    if not extensions:
        raise ValueError("At least one extension must be specified")
    invalid = []
    for name in extensions:
        if not re.fullmatch(r"[A-Za-z0-9_]+", name):
            invalid.append(name)
    if invalid:
        raise ValueError(f"Invalid extension name: {invalid[0]}")
    return extensions


def align_macos_mimalloc_dependency(
    extension_file: Path, wheel_members: set[str]
) -> None:
    """Point an extracted extension at mimalloc shipped by the NeuG wheel."""
    mimalloc_members = sorted(
        member
        for member in wheel_members
        if re.fullmatch(r"neug/\.dylibs/libmimalloc\.\d+(?:\.\d+)+\.dylib", member)
    )
    if not mimalloc_members:
        raise FileNotFoundError("Repaired wheel does not contain macOS mimalloc")

    target = f"@loader_path/../../neug/.dylibs/{Path(mimalloc_members[-1]).name}"
    output = subprocess.run(
        ["otool", "-L", extension_file],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    dependencies = [
        line.strip().split(" ", 1)[0]
        for line in output.splitlines()[1:]
        if "libmimalloc." in line
    ]
    if not dependencies:
        return

    for dependency in dependencies:
        if dependency != target:
            subprocess.run(
                ["install_name_tool", "-change", dependency, target, extension_file],
                check=True,
            )
    subprocess.run(["codesign", "--force", "--sign", "-", extension_file], check=True)


def extract_extensions(
    wheel: Path, extensions: list[str], platform: str, output_dir: Path
) -> dict[str, tuple[Path, Path]]:
    """Extract repaired extension libraries and generate their checksums."""
    if not wheel.is_file():
        raise FileNotFoundError(f"Repaired wheel not found: {wheel}")

    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts = {}
    with zipfile.ZipFile(wheel) as archive:
        members = set(archive.namelist())
        for extension in extensions:
            filename = f"lib{extension}.neug_extension"
            member = f"extension/{extension}/{filename}"
            if member not in members:
                message = (
                    f"Extension {extension!r} is missing from repaired wheel "
                    f"{wheel}"
                )
                raise FileNotFoundError(message)

            extension_file = output_dir / filename
            source = archive.open(member)
            target = extension_file.open("wb")
            with source, target:
                shutil.copyfileobj(source, target)

            if platform.startswith("osx"):
                align_macos_mimalloc_dependency(extension_file, members)

            checksum_file = output_dir / f"{filename}.sha256"
            checksum_file.write_text(
                hashlib.sha256(extension_file.read_bytes()).hexdigest(),
                encoding="utf-8",
            )
            artifacts[extension] = (extension_file, checksum_file)
            print(f"Packaged repaired extension: {member} -> {extension_file}")
    return artifacts


def upload_artifacts(
    artifacts: dict[str, tuple[Path, Path]], version: str, platform: str
) -> None:
    required_env = (
        "OSS_ACCESS_KEY_ID",
        "OSS_ACCESS_KEY_SECRET",
        "OSS_ENDPOINT",
        "OSS_BUCKET_NAME",
    )
    missing = [name for name in required_env if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing OSS configuration: {', '.join(missing)}")

    try:
        import oss2
    except ImportError as error:
        message = "Install oss2 before publishing extensions"
        raise RuntimeError(message) from error

    auth = oss2.Auth(
        os.environ["OSS_ACCESS_KEY_ID"], os.environ["OSS_ACCESS_KEY_SECRET"]
    )
    bucket_name = os.environ["OSS_BUCKET_NAME"]
    bucket = oss2.Bucket(auth, os.environ["OSS_ENDPOINT"], bucket_name)
    normalized_version = version.removeprefix("v")
    for extension, files in artifacts.items():
        for artifact in files:
            object_name = (
                f"neug/extensions/v{normalized_version}/{platform}/"
                f"{extension}/{artifact.name}"
            )
            print(f"Uploading {artifact} to oss://{bucket_name}/{object_name}")
            bucket.put_object_from_file(object_name, str(artifact))


def main() -> int:
    args = parse_args()
    try:
        extensions = parse_extensions(args.extensions)
        artifacts = extract_extensions(
            args.wheel, extensions, args.platform, args.output_dir
        )
        if not args.skip_upload:
            upload_artifacts(artifacts, args.version, args.platform)
    except (OSError, RuntimeError, ValueError, zipfile.BadZipFile) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
