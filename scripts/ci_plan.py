#!/usr/bin/env python3
"""Resolve the build selection shared by package, verify, and release."""

import argparse
import json
import os
import re
from pathlib import Path

PLATFORMS = {
    "linux-x86_64": ("x86_64", "linux_x86_64", "ubuntu-22.04"),
    "linux-arm64": ("arm64", "linux_arm64", "ubuntu-22.04-arm"),
    "osx-arm64": ("arm64", "osx_arm64", "macos-15"),
    "windows-x86_64": ("x86_64", "win_x86_64", "windows-2022"),
    "windows-arm64": ("arm64", "win_arm64", "windows-11-arm"),
}
EXTENSIONS = "parquet;httpfs;gds;pattern_matching;vector_search;fts"
REGISTRY = "neug-registry.cn-hongkong.cr.aliyuncs.com/neug"


def resolve(platforms="all", python_versions="all"):
    selected = (
        list(PLATFORMS) + ["java"]
        if platforms == "all"
        else (
            json.loads(platforms)
            if platforms.startswith("[")
            else [p for p in platforms.split(",") if p]
        )
    )
    if set(selected) - set(PLATFORMS) - {"java"}:
        raise ValueError(f"Unsupported platforms: {selected}")
    supported = (
        re.search(
            r'^build = "(.*)"',
            (
                Path(__file__).resolve().parents[1] / "tools/python_bind/pyproject.toml"
            ).read_text(),
            re.M,
        )
        .group(1)
        .split()
    )
    supported = ["3." + pattern[3:-2] for pattern in supported]
    if python_versions == "all":
        versions = supported
    elif python_versions.startswith("["):
        versions = json.loads(python_versions)
    elif "-" in python_versions:
        first, last = python_versions.split("-")
        versions = [f"3.{n}" for n in range(int(first[2:]), int(last[2:]) + 1)]
    else:
        versions = [python_versions]
    if not versions or set(versions) - set(supported):
        raise ValueError(f"Unsupported Python versions: {versions}")
    rows = []
    for platform, (arch, oss_platform, runner) in PLATFORMS.items():
        if platform not in selected:
            continue
        row = dict(
            platform=platform,
            arch=arch,
            oss_platform=oss_platform,
            runner=runner,
            build_runner=runner,
            extensions=EXTENSIONS,
        )
        if platform.startswith("linux-"):
            row.update(
                wheel_image=f"{REGISTRY}/neug-manylinux:2026.03.13-2-{arch}",
                node_image=f"{REGISTRY}/neug-manylinux:2026.03.13-2-{arch}-node",
                cibw_arch="aarch64" if arch == "arm64" else arch,
            )
            if os.environ.get("GITHUB_REPOSITORY") == "alibaba/neug":
                row["build_runner"] = [
                    "self-hosted",
                    "daily",
                    "linux",
                    "arm64" if arch == "arm64" else "x64",
                ]
        rows.append(row)
    return dict(
        platforms=[r["platform"] for r in rows],
        matrix=rows,
        linux=[r for r in rows if r["platform"].startswith("linux-")],
        windows_architectures=[
            r["arch"] for r in rows if r["platform"].startswith("windows-")
        ],
        python_versions=versions,
        python_spec=(
            versions[0] if len(versions) == 1 else f"{versions[0]}-{versions[-1]}"
        ),
        cibw_build=" ".join("cp" + v.replace(".", "") + "-*" for v in versions),
        extensions=EXTENSIONS,
        dev_image=f"{REGISTRY}/neug-dev:2026.03.13-2",
        verify_java="java" in selected,
        packaging_python="3.13",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platforms", default="all")
    parser.add_argument("--python-versions", default="all")
    parser.add_argument("--plan", default="")
    args = parser.parse_args()
    plan = (
        json.loads(args.plan)
        if args.plan
        else resolve(args.platforms, args.python_versions)
    )
    encoded = json.dumps(plan, separators=(",", ":"))
    print(json.dumps(plan, indent=2))
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"plan={encoded}\n")


if __name__ == "__main__":
    main()
