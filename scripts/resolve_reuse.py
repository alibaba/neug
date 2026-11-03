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


def find_reusable_run():
    """Return the id of a complete, recent dry-run for this commit, else None."""
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

    now = datetime.datetime.now(datetime.timezone.utc)
    chosen = None
    for run in json.loads(runs.stdout).get("workflow_runs", []):
        if run.get("conclusion") != "success":
            continue
        created = datetime.datetime.fromisoformat(
            run["created_at"].replace("Z", "+00:00")
        )
        if (now - created).days > 6:
            continue
        chosen = run
        break
    if chosen is None:
        return None

    run_id = chosen["id"]
    arts = gh(f"repos/{repo}/actions/runs/{run_id}/artifacts", ("per_page", "200"))
    if arts.returncode != 0:
        return None
    names = [a["name"] for a in json.loads(arts.stdout).get("artifacts", [])]
    wheels = [n for n in names if n.startswith("wheels-")]
    nodejs_main = [n for n in names if n.startswith("nodejs-main-")]
    javas = [n for n in names if n.startswith("java-publication-")]
    complete = (
        len(wheels) == len(platforms)
        and len(nodejs_main) == 1
        and all(any(n.startswith(f"nodejs-{p}-") for n in names) for p in platforms)
        and all(
            any(n.startswith(f"extension-packages-{p}-") for n in names)
            for p in platforms
        )
        and (len(javas) >= 1 if verify_java else True)
    )
    return run_id if complete else None


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
