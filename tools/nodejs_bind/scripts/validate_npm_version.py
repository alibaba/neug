#!/usr/bin/env python3
"""Validate npm package versions in a tarball or package directory."""

import argparse
import json
import sys
import tarfile
from pathlib import Path


def load_package_json(package_path: str) -> dict:
    path = Path(package_path)
    if path.is_dir():
        with (path / "package.json").open() as package_json:
            return json.load(package_json)

    with tarfile.open(path, "r:gz") as archive:
        package_json = archive.extractfile("package/package.json")
        if package_json is None:
            raise FileNotFoundError("package/package.json")
        return json.load(package_json)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate package versions in an npm tarball or package directory."
    )
    parser.add_argument("package_path")
    parser.add_argument("expected_name")
    parser.add_argument("expected_version")
    parser.add_argument(
        "--optional-dependency",
        action="append",
        default=[],
        metavar="PACKAGE",
        help="Optional dependency package that must use the expected version.",
    )
    args = parser.parse_args()

    try:
        package = load_package_json(args.package_path)
    except Exception as exc:
        print(f"{args.package_path}: failed to read package.json: {exc}", file=sys.stderr)
        return 1

    actual_name = package.get("name")
    actual_version = package.get("version")
    if actual_name != args.expected_name or actual_version != args.expected_version:
        print(
            f"{args.package_path}: expected {args.expected_name}@{args.expected_version}, "
            f"got {actual_name}@{actual_version}",
            file=sys.stderr,
        )
        return 1

    optional_dependencies = package.get("optionalDependencies", {})
    for dependency in args.optional_dependency:
        actual_dependency_version = optional_dependencies.get(dependency)
        if actual_dependency_version != args.expected_version:
            print(
                f"{args.package_path}: expected optional dependency "
                f"{dependency}@{args.expected_version}, got {actual_dependency_version}",
                file=sys.stderr,
            )
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
