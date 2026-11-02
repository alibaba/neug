#!/usr/bin/env python3
"""Bump the NeuG project version across all files that hardcode it.

The project version is NOT single-sourced: NEUG_VERSION is only consumed by
include/neug/version.h.in, while ~12 other files hardcode the same string.
This script keeps them in sync.

Usage:
  python3 scripts/bump_version.py X.Y.Z     # bump NEUG_VERSION -> X.Y.Z
  python3 scripts/bump_version.py --check   # verify consistency, change nothing
  python3 scripts/bump_version.py --list    # show current version + managed files

Design rules:
  1. Bump mode only rewrites the explicit MANAGED_FILES whitelist below --
     never a blind repo-wide replace. This protects dependency pins that may
     coincidentally equal the project version (e.g. `pyarrow==X.Y.Z`) and
     third-party vendored copies of neug docs.
  2. Lines matching IMAGE_TAG_RE are skipped: build-image tags
     (neug-dev:v*, neug-manylinux*:v*, manifest.sh args) are decoupled from
     the release version and must NOT be bumped.
  3. After bumping, a repo-wide residual scan for the old version must come
     up empty (outside skipped image-tag lines); any hit is reported as a
     warning for manual review.
  4. --check mode (CI gate) asserts every managed file contains the current
     NEUG_VERSION at least once outside image-tag lines -- this catches a
     partial bump (NEUG_VERSION changed, satellites forgotten, or vice
     versa). A repo-wide "any old version string" gate is deliberately NOT
     run in --check: historic versions legitimately appear in dependency
     pins (neug>=0.1.0), `@since v0.1.0` doc comments, etc. The repo-wide
     residual scan runs in bump mode instead, where the old version is known
     precisely and hits can be reviewed interactively.

Exit codes: 0 = success/consistent, 1 = inconsistency or bad arguments.
"""
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
# Matches a neug version string, bare or v-prefixed:
#   - bare form: preceded by a non-word, non-dot char (hyphen OK), so
#     `neug-0.3.0.tgz` and `0.3.0-SNAPSHOT` match, while `10.3.0` and
#     `csv0.3.0` never do;
#   - v-prefixed form: `v` not preceded by a word char;
#   - trailing guard rejects only digit continuations (0.3.01, 0.3.0.1),
#     so `.tgz` / `-SNAPSHOT` suffixes still match -- the X.Y.Z substring
#     is what gets replaced.
VERSION_OCCURRENCE_RE_TMPL = (
    r"(?:(?<![A-Za-z0-9_.]){ver}|(?<![A-Za-z0-9_])v{ver})(?![0-9]|\.\d)"
)

# Build-image references: tags here follow the content-version scheme and are
# intentionally decoupled from the release version.
IMAGE_TAG_RE = re.compile(
    r"neug-(?:dev|manylinux|release)(?:[^\s:\"'/]*):v?\d+\.\d+\.\d+"
    r"|manifest\.sh\s+\S+\s+v?\d+\.\d+\.\d+"
)

# Files whose version occurrences are bumped. Grouped for reporting only.
MANAGED_FILES = {
    "core": [
        "NEUG_VERSION",
    ],
    "python": [
        "tools/python_bind/pyproject.toml",
        "tools/python_bind/setup.py",
        "tools/python_bind/neug/neug_cli.py",
        "tools/python_bind/README.md",
    ],
    "node": [
        "tools/nodejs_bind/package.json",
        "tools/nodejs_bind/assets/package.json",
        "tools/nodejs_bind/package-lock.json",
        "tools/nodejs_bind/README.md",
    ],
    "java": [
        # pom.xml/README carry X.Y.Z-SNAPSHOT; replacing the X.Y.Z substring
        # preserves the -SNAPSHOT suffix.
        "tools/java_driver/pom.xml",
        "tools/java_driver/README.md",
    ],
    "scripts": [
        "scripts/build_and_upload_extensions.sh",
    ],
}

# Files excluded from the repo-wide --check scan (vendored license texts and
# this script itself legitimately mention other version strings).
CHECK_EXCLUDES = {"LICENSE", "NOTICE.txt", "scripts/bump_version.py"}


def read_version() -> str:
    ver = (REPO_ROOT / "NEUG_VERSION").read_text().strip()
    if not VERSION_RE.match(ver):
        die(f"NEUG_VERSION does not contain a valid X.Y.Z version: {ver!r}")
    return ver


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def version_tuple(ver: str):
    return tuple(int(p) for p in ver.split("."))


def is_binary(path: Path) -> bool:
    try:
        return b"\x00" in path.read_bytes()[:8192]
    except OSError:
        return True


def tracked_files() -> list:
    out = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
    ).stdout.decode()
    return [
        f
        for f in out.split("\0")
        if f and not f.startswith("third_party/") and f not in CHECK_EXCLUDES
    ]


def replace_in_file(rel: str, old: str, new: str, apply: bool):
    """Replace old->new on non-image-tag lines. Returns (n_replaced, skipped_image_lines)."""
    path = REPO_ROOT / rel
    if not path.exists():
        die(f"managed file missing: {rel}")
    text = path.read_text(encoding="utf-8")
    old_re = re.compile(VERSION_OCCURRENCE_RE_TMPL.format(ver=re.escape(old)))
    out_lines, n, skipped = [], 0, 0
    for line in text.splitlines(keepends=True):
        if IMAGE_TAG_RE.search(line):
            if old_re.search(line):
                skipped += 1
            out_lines.append(line)
            continue
        line_new, k = old_re.subn(new, line)
        n += k
        out_lines.append(line_new)
    if apply and n:
        path.write_text("".join(out_lines), encoding="utf-8")
    return n, skipped


def scan_repo(version: str):
    """Yield (rel, lineno, line) for every non-image-tag occurrence of `version`
    (or vX.Y.Z) in tracked, non-binary files."""
    occ = re.compile(VERSION_OCCURRENCE_RE_TMPL.format(ver=re.escape(version)))
    for rel in tracked_files():
        path = REPO_ROOT / rel
        if is_binary(path):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if occ.search(line) and not IMAGE_TAG_RE.search(line):
                yield rel, i, line.strip()


def do_bump(new: str) -> None:
    old = read_version()
    if not VERSION_RE.match(new):
        die(f"new version must be X.Y.Z (got {new!r})")
    if version_tuple(new) <= version_tuple(old):
        die(f"new version {new} is not greater than current {old}")
    if new == old:
        die(f"already at {old}")

    print(f"Bumping {old} -> {new}")
    managed = [f for group in MANAGED_FILES.values() for f in group]
    total, total_skipped = 0, 0
    for group, files in MANAGED_FILES.items():
        for rel in files:
            n, skipped = replace_in_file(rel, old, new, apply=True)
            total += n
            total_skipped += skipped
            flag = "" if n else "  (no change)"
            print(f"  [{group:7s}] {rel}: {n} replacement(s){flag}")
    print(
        f"Total: {total} replacement(s) in {len(managed)} managed files; "
        f"{total_skipped} image-tag line(s) skipped (decoupled from the release version)"
    )

    # Residual scan: nothing outside image-tag lines may still carry the old version.
    residuals = list(scan_repo(old))
    if residuals:
        print(
            f"\nWARNING: {len(residuals)} residual occurrence(s) of {old} "
            f"outside managed files -- review manually:"
        )
        for rel, i, line in residuals:
            print(f"  {rel}:{i}: {line[:120]}")
        sys.exit(1)
    print(f"Residual scan clean: no non-image-tag occurrence of {old} remains.")
    print(
        "\nDone. Review with `git diff`, then run "
        "`python3 scripts/bump_version.py --check` to re-verify."
    )


# Per-managed-file regexes capturing ONLY neug-version spots (context-
# anchored, so third-party pins like pyarrow==18.0.0 or <version>4.11.0</
# version> plugin entries never match). --check asserts every capture equals
# the current NEUG_VERSION and each file has at least one capture.
_V = r"(\d+\.\d+\.\d+)"
VERSION_CAPTURE_RE = {
    "tools/python_bind/pyproject.toml": re.compile(rf"^version = \"{_V}\"", re.M),
    "tools/python_bind/setup.py": re.compile(rf"__version__ = \"{_V}\""),
    "tools/python_bind/neug/neug_cli.py": re.compile(
        rf"version_option\(version=\"{_V}\""
    ),
    "tools/python_bind/README.md": re.compile(rf"neug-cli, version {_V}"),
    "tools/nodejs_bind/package.json": re.compile(rf"^  \"version\": \"{_V}\"", re.M),
    "tools/nodejs_bind/assets/package.json": re.compile(
        rf"\"version\": \"{_V}\"|\"@neug-ai/[^\"]+\": \"{_V}\""
    ),
    # package-lock.json is checked structurally (JSON parse) -- indentation-
    # based regex cannot distinguish the root/packages."" version from nested
    # node_modules entries.
    "tools/nodejs_bind/README.md": re.compile(rf"neug-{_V}\.tgz"),
    # pom.xml: anchor on the project's own coordinate block so plugin and
    # dependency <version> tags (4.11.0 etc.) never match. README snippet is
    # a neug dependency example, matched directly.
    "tools/java_driver/pom.xml": re.compile(
        rf"<artifactId>neug-java-driver</artifactId>\s*\n\s*<version>{_V}(?:-SNAPSHOT)?</version>"
    ),
    "tools/java_driver/README.md": re.compile(
        rf"<version>{_V}(?:-SNAPSHOT)?</version>"
    ),
    "scripts/build_and_upload_extensions.sh": re.compile(
        rf"^VERSION=\"{_V}\"|--version v{_V}|extensions/v{_V}|\bneug/extensions/v{_V}",
        re.M,
    ),
}


def check_package_lock(current: str, rel: str, missing: list, stale: list) -> int:
    """Structural check for package-lock.json: root version and packages.""
    version must equal `current`. Returns the number of verified spots."""
    import json

    text = (REPO_ROOT / rel).read_text(encoding="utf-8")
    data = json.loads(text)
    seen = 0
    spots = [("version", data.get("version"))]
    root_pkg = data.get("packages", {}).get("", {})
    spots.append(('packages."".version', root_pkg.get("version")))
    for name, ver in spots:
        if ver == current:
            seen += 1
        else:
            stale.append((rel, 0, str(ver), f"{name} != {current}"))
    return seen


def do_check() -> None:
    current = read_version()
    print(
        f"NEUG_VERSION = {current}; verifying all managed files carry it "
        f"(image-tag lines excluded) ..."
    )

    managed = [f for group in MANAGED_FILES.values() for f in group]
    missing, stale, count = [], [], 0
    for rel in managed:
        path = REPO_ROOT / rel
        if not path.exists():
            missing.append((rel, "file does not exist"))
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if rel == "NEUG_VERSION":
            if text.strip() == current:
                count += 1
            else:
                missing.append((rel, f"content is {text.strip()!r}, not {current}"))
            continue
        if rel == "tools/nodejs_bind/package-lock.json":
            seen = check_package_lock(current, rel, missing, stale)
            if seen == 0:
                missing.append((rel, f"no occurrence of {current}"))
            count += seen
            continue
        rx = VERSION_CAPTURE_RE[rel]
        seen = 0
        if "\\n" in rx.pattern:
            # Multi-line pattern (pom.xml coordinate block): search whole text.
            for m in rx.finditer(text):
                ver = next(g for g in m.groups() if g)
                if ver == current:
                    seen += 1
                else:
                    lineno = text.count("\n", 0, m.start()) + 1
                    stale.append((rel, lineno, ver, m.group(0).strip()))
        else:
            for i, line in enumerate(text.splitlines(), 1):
                if IMAGE_TAG_RE.search(line):
                    continue
                for m in rx.finditer(line):
                    ver = next(g for g in m.groups() if g)
                    if ver == current:
                        seen += 1
                    else:
                        stale.append((rel, i, ver, line.strip()))
        if seen == 0:
            missing.append((rel, f"no occurrence of {current}"))
        count += seen

    ok = True
    if missing:
        ok = False
        print(f"\nFAIL: managed file(s) not carrying {current}:")
        for rel, why in missing:
            print(f"  {rel}: {why}")
    if stale:
        ok = False
        print(
            f"\nFAIL: managed file(s) with a version != {current} "
            f"(missed by a bump?):"
        )
        for rel, i, ver, line in stale:
            print(f"  {rel}:{i}: found {ver}: {line[:120]}")
    if not ok:
        sys.exit(1)
    print(
        f"OK: {count} occurrence(s) of {current} across {len(managed)} "
        f"managed files; no stray versions among them."
    )


def do_list() -> None:
    current = read_version()
    print(f"current : {current}")
    print("managed files:")
    for group, files in MANAGED_FILES.items():
        for f in files:
            print(f"  [{group}] {f}")


def main() -> None:
    args = sys.argv[1:]
    if len(args) == 1 and args[0] == "--check":
        do_check()
    elif len(args) == 1 and args[0] == "--list":
        do_list()
    elif len(args) == 1 and VERSION_RE.match(args[0]):
        do_bump(args[0])
    else:
        print(__doc__)
        sys.exit(1 if args else 0)


if __name__ == "__main__":
    main()
