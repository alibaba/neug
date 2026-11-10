#!/usr/bin/env python3
"""Extract repaired extensions from a cibuildwheel wheel and publish them."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    # Wheel mode (extract from a repaired wheel) and artifact mode (publish
    # pre-built artifacts downloaded from every platform job) share --version;
    # the remaining flags are validated per mode in main().
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--extensions")
    parser.add_argument("--version", required=True)
    parser.add_argument("--platform")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument(
        "--from-artifacts",
        type=Path,
        help=(
            "Publish pre-built extension artifacts downloaded from every "
            "platform job (a directory holding extension-packages-<platform>-* "
            "subdirectories) instead of extracting them from a wheel."
        ),
    )
    parser.add_argument(
        "--matrix",
        help=(
            "ci-plan matrix JSON mapping each platform to its oss_platform; "
            "required together with --from-artifacts."
        ),
    )
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


def oss_object_name(version: str, platform: str, extension: str, filename: str) -> str:
    """Build the OSS object key for one extension artifact file.

    Shared by the real upload and the --skip-upload dry run so both emit the
    exact same ``neug/extensions/v<version>/<oss_platform>/<extension>/<file>``
    key (a leading ``v`` on the release tag is normalized away first).
    """
    normalized_version = version.removeprefix("v")
    return f"neug/extensions/v{normalized_version}/{platform}/{extension}/{filename}"


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
    for extension, files in artifacts.items():
        for artifact in files:
            object_name = oss_object_name(version, platform, extension, artifact.name)
            print(f"Uploading {artifact} to oss://{bucket_name}/{object_name}")
            bucket.put_object_from_file(object_name, str(artifact))


def publish_from_artifacts(
    artifacts_root: Path,
    matrix: list[dict],
    version: str,
    skip_upload: bool = False,
) -> list[str]:
    """Publish pre-built extension artifacts gathered from every platform job.

    ``artifacts_root`` holds one subdirectory per downloaded artifact, named
    ``extension-packages-<platform>-*``. A platform may carry a default and a
    cxx11abi variant; both share the same logical extension directory on OSS and
    are told apart only by the ``-cxx11abi`` filename suffix. The platform ->
    oss_platform mapping is taken from the ci-plan matrix rather than parsed out
    of directory names. With ``skip_upload`` the resolved object keys are printed
    instead of pushed, so the layout can be verified without OSS credentials.
    """
    published = []
    for entry in matrix:
        platform = entry["platform"]
        oss_platform = entry["oss_platform"]
        variants: dict[str, dict[str, tuple[Path, Path]]] = {
            "default": {},
            "cxx11abi": {},
        }
        pattern = f"extension-packages-{platform}-*"
        for artifact_dir in sorted(artifacts_root.glob(pattern)):
            for path in sorted(artifact_dir.glob("*.neug_extension")):
                name = path.name.removeprefix("lib").removesuffix(".neug_extension")
                abi = "cxx11abi" if name.endswith("-cxx11abi") else "default"
                extension = name.removesuffix("-cxx11abi")
                checksum = path.with_name(path.name + ".sha256")
                variants[abi][extension] = (path, checksum)
        if not any(variants.values()):
            raise RuntimeError(f"No extension artifacts for platform {platform}")
        for files in variants.values():
            if not files:
                continue
            if skip_upload:
                bucket_name = os.environ.get("OSS_BUCKET_NAME", "<OSS_BUCKET_NAME>")
                for extension, pair in sorted(files.items()):
                    for artifact in pair:
                        key = oss_object_name(
                            version, oss_platform, extension, artifact.name
                        )
                        print(f"[dry-run] {artifact} -> oss://{bucket_name}/{key}")
            else:
                upload_artifacts(files, version, oss_platform)
        published.append(platform)
    return published


def main() -> int:
    args = parse_args()
    try:
        if args.from_artifacts is not None:
            if not args.matrix:
                raise ValueError("--matrix is required with --from-artifacts")
            published = publish_from_artifacts(
                args.from_artifacts,
                json.loads(args.matrix),
                args.version,
                args.skip_upload,
            )
            print(f"Published extensions for platforms: {published}")
        else:
            missing = [
                flag
                for flag, value in (
                    ("--wheel", args.wheel),
                    ("--extensions", args.extensions),
                    ("--platform", args.platform),
                    ("--output-dir", args.output_dir),
                )
                if value is None
            ]
            if missing:
                raise ValueError(f"Missing required arguments: {', '.join(missing)}")
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
