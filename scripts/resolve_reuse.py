#!/usr/bin/env python3
"""Resolve a reusable package.yml dry-run for the current release commit.

Best-effort: writes ``reuse_run_id=<id>`` to ``$GITHUB_OUTPUT`` only when a
complete, successful ``package.yml`` ``workflow_dispatch`` dry-run exists for
this exact commit within artifact retention (6 days). Otherwise -- or on any
lookup failure -- ``reuse_run_id`` stays empty so the release rebuilds.
"""

import datetime
import json
import os
import shutil
import subprocess
import tempfile


def gh(path, *fields):
    # gh api defaults to POST whenever -f/-F fields are present; the dry-run
    # lookups below are GET requests whose fields are query parameters, so the
    # method must be pinned explicitly or GitHub answers 404 and reuse never
    # matches.
    args = ["gh", "api", "--method", "GET", path]
    for key, value in fields:
        args += ["-f", f"{key}={value}"]
    return subprocess.run(args, capture_output=True, text=True)


def write_output(key, value):
    with open(os.environ["GITHUB_OUTPUT"], "a") as handle:
        handle.write(f"{key}={value}\n")


def wheel_prefix(platform):
    """Wheel-artifact name prefix for a plan platform id.

    macOS wheels are uploaded under a ``macos-`` prefix while the plan (and the
    npm/extension artifacts) identify that platform as ``osx-``; every other
    platform's wheel name matches its id.
    """
    if platform.startswith("osx-"):
        platform = "macos-" + platform.removeprefix("osx-")
    return f"wheels-{platform}-"


def is_complete(names, platforms, verify_java):
    """True when a run's artifacts cover every part of the release plan."""

    def has(prefix):
        return any(name.startswith(prefix) for name in names)

    return (
        all(has(wheel_prefix(p)) for p in platforms)
        and has("nodejs-main-")
        and all(has(f"nodejs-{p}-") for p in platforms)
        and all(has(f"extension-packages-{p}-") for p in platforms)
        and (has("java-publication-") if verify_java else True)
    )


def manifest_name(run_id):
    """Reuse-manifest artifact name package.yml records for a run."""
    return f"reuse-manifest-{run_id}"


def fetch_dry_run_versions(repo, run_id, artifacts):
    """Python versions a candidate dry-run actually built, or None.

    Downloads the run's ``reuse-manifest`` artifact (the plan package.yml
    resolved) and returns its ``python_versions`` as a set. A missing manifest
    or any download/parse failure yields None, so the caller rebuilds rather
    than reusing a run whose Python coverage it cannot confirm.
    """
    name = manifest_name(run_id)
    if not any(a["name"] == name for a in artifacts):
        return None
    dest = tempfile.mkdtemp()
    try:
        cmd = ["gh", "run", "download", str(run_id), "--repo", repo]
        cmd += ["-n", name, "-D", dest]
        download = subprocess.run(cmd, capture_output=True, text=True)
        if download.returncode != 0:
            return None
        with open(os.path.join(dest, "plan.json")) as handle:
            return set(json.load(handle).get("python_versions", []))
    except (OSError, ValueError):
        return None
    finally:
        shutil.rmtree(dest, ignore_errors=True)


def find_reusable_run():
    """Return the id of the newest complete dry-run for this commit, else None."""
    repo = os.environ["GITHUB_REPOSITORY"]
    sha = os.environ["GITHUB_SHA"]
    plan = json.loads(os.environ["PLAN"])
    platforms = plan["platforms"]
    verify_java = bool(plan.get("verify_java"))
    python_versions = set(plan.get("python_versions", []))

    runs = gh(
        f"repos/{repo}/actions/workflows/package.yml/runs",
        ("head_sha", sha),
        ("event", "workflow_dispatch"),
        ("status", "completed"),
        ("per_page", "20"),
    )
    if runs.returncode != 0:
        return None

    # Runs come back newest-first. Return the first one whose artifacts fully
    # cover the plan, so a later partial dry-run cannot shadow an earlier
    # complete one for the same commit.
    now = datetime.datetime.now(datetime.timezone.utc)
    for run in json.loads(runs.stdout).get("workflow_runs", []):
        if run.get("conclusion") != "success":
            continue
        created = datetime.datetime.fromisoformat(
            run["created_at"].replace("Z", "+00:00")
        )
        if (now - created).days > 6:
            continue
        run_id = run["id"]
        arts = gh(f"repos/{repo}/actions/runs/{run_id}/artifacts", ("per_page", "200"))
        if arts.returncode != 0:
            continue
        artifacts = json.loads(arts.stdout).get("artifacts", [])
        names = [a["name"] for a in artifacts]
        if not is_complete(names, platforms, verify_java):
            continue
        # Artifact names carry no Python version, so a dry-run dispatched with a
        # narrowed python_versions still looks complete by name. Require the
        # recorded plan to match this release's Python coverage before reusing.
        if fetch_dry_run_versions(repo, run_id, artifacts) != python_versions:
            continue
        return run_id
    return None


def main():
    # Default: rebuild in this run. Overridden only when a complete, successful
    # dry-run exists for this commit within artifact retention.
    write_output("reuse_run_id", "")
    run_id = find_reusable_run()
    if run_id is not None:
        sha = os.environ["GITHUB_SHA"]
        print(f"Reusing package dry-run {run_id} for commit {sha}")
        write_output("reuse_run_id", run_id)


if __name__ == "__main__":
    main()
