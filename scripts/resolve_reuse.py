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
import subprocess


def gh(path, *fields):
    args = ["gh", "api", path]
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


def find_reusable_run():
    """Return the id of the newest complete dry-run for this commit, else None."""
    repo = os.environ["GITHUB_REPOSITORY"]
    sha = os.environ["GITHUB_SHA"]
    plan = json.loads(os.environ["PLAN"])
    platforms = plan["platforms"]
    verify_java = bool(plan.get("verify_java"))

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
        names = [a["name"] for a in json.loads(arts.stdout).get("artifacts", [])]
        if is_complete(names, platforms, verify_java):
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
