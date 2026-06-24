#!/usr/bin/env python3
"""
Issue triage worker.

Inputs (env):
  GITHUB_EVENT_PATH  - path to GitHub Actions event payload
  DASHSCOPE_API_KEY  - Qwen API key (DashScope)
  GH_TOKEN           - GitHub token for API calls
  CONFIG_PATH        - path to issue-triage-config.yml (default: .github/issue-triage-config.yml)

Behavior:
  1. Read the new/edited issue from the event payload.
  2. Skip if it's an umbrella tracker (title contains "[Tracking]" or matches a configured umbrella number).
  3. Build a prompt with each umbrella's scope_in/scope_out.
  4. Call Qwen via OpenAI-compatible endpoint.
  5. Post a comment with the suggestion.
  6. (Optional) If config.auto_link_threshold is met, add as sub-issue.

The script is intentionally single-file with no third-party deps beyond
`pyyaml` and `requests` (installed in the workflow).
"""
from __future__ import annotations

import json
import os
import sys
import textwrap
from pathlib import Path

import requests
import yaml

QWEN_ENDPOINT = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen-plus")

GITHUB_API = "https://api.github.com"

CONFIDENCE_LEVELS = {"high": 3, "medium": 2, "low": 1}

ISSUE_TYPES = {"Bug": 242038, "Feature": 242040, "Task": 242035}

SUBSYSTEM_LABELS = [
    "compiler",
    "executor",
    "store",
    "transaction",
    "extension",
    "client",
    "ci",
]

SYSTEM_PROMPT = """\
You are an issue-triage assistant for the NeuG graph database project.

Given a new GitHub issue, you must determine THREE things:

## 1. Parent umbrella
Choose the single best umbrella from the provided list, OR return null if none fits.
- Match against scope_in. If the issue matches another umbrella's scope_out, exclude it.
- Prefer the most specific fit. Use #257 (Adhoc) only when nothing else fits.
- If the issue is itself a tracking/umbrella issue, set umbrella to null.
- Confidence: "high" = unambiguous; "medium" = good fit but some overlap; "low" = unclear.

## 2. Issue type
Classify as exactly one of:
- "Bug" — something is broken, crashes, produces wrong results, memory leaks
- "Feature" — new capability, new API, new extension, new format support
- "Task" — refactoring, cleanup, performance optimization, documentation, CI/build improvement

## 3. Subsystem labels
Pick one or more from: compiler, executor, store, transaction, extension, client, ci.
- "compiler" — ANTLR parser, binder, logical/physical planner, gopt converter
- "executor" — physical operators, scan, filter, project, join, aggregation, runtime
- "store" — CSR storage, schema, property columns, vertex/edge tables, indexer
- "transaction" — COW, WAL, rollback, concurrency, UpdateTransaction
- "extension" — httpfs, parquet, JSON, GDS algorithm extensions
- "client" — Python/Java/Node.js bindings, SDK
- "ci" — CI/CD, build system, testing infrastructure, GitHub Actions
Return an empty list if none clearly applies.

Output ONLY a JSON object:
{"umbrella": <number_or_null>, "confidence": "high"|"medium"|"low"|"n/a", "reason": "<one sentence>", "type": "Bug"|"Feature"|"Task", "labels": ["label1", ...]}
"""


def gh_headers() -> dict:
    token = os.environ.get("GH_TOKEN", "").strip()
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def load_config() -> dict:
    path = os.getenv("CONFIG_PATH", ".github/issue-triage-config.yml")
    return yaml.safe_load(Path(path).read_text())


def build_user_prompt(cfg: dict, issue: dict) -> str:
    parts = ["Umbrellas:\n"]
    for u in cfg["umbrellas"]:
        parts.append(f"#{u['number']} — {u['title']}")
        parts.append(f"scope_in:\n{textwrap.indent(u['scope_in'].strip(), '  ')}")
        parts.append(f"scope_out:\n{textwrap.indent(u['scope_out'].strip(), '  ')}")
        parts.append("")
    body = (issue.get("body") or "").strip()
    if len(body) > 4000:
        body = body[:4000] + "\n\n[...truncated]"
    parts.append("\nNew issue:")
    parts.append(f"Title: {issue['title']}")
    parts.append(f"Body:\n{body}")
    return "\n".join(parts)


def call_qwen(api_key: str, system: str, user: str) -> dict:
    resp = requests.post(
        QWEN_ENDPOINT,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": QWEN_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.0,
        },
        timeout=60,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]
    return json.loads(content)


def is_umbrella(cfg: dict, title: str, number: int) -> bool:
    if "[Tracking]" in title:
        return True
    return any(u["number"] == number for u in cfg["umbrellas"])


def should_auto_link(cfg: dict, confidence: str) -> bool:
    threshold = cfg.get("auto_link_threshold", "never")
    if threshold == "never":
        return False
    return CONFIDENCE_LEVELS.get(confidence, 0) >= CONFIDENCE_LEVELS.get(threshold, 99)


def check_has_parent(repo: str, number: int) -> bool:
    resp = requests.get(
        f"{GITHUB_API}/repos/{repo}/issues/{number}",
        headers=gh_headers(),
        timeout=15,
    )
    if resp.status_code != 200:
        return False
    data = resp.json()
    return bool(data.get("parent"))


def link_sub_issue(repo: str, parent_number: int, child_issue_id: int) -> bool:
    resp = requests.post(
        f"{GITHUB_API}/repos/{repo}/issues/{parent_number}/sub_issues",
        headers=gh_headers(),
        json={"sub_issue_id": child_issue_id},
        timeout=15,
    )
    if resp.status_code not in (200, 201):
        print(f"Failed to link sub-issue: {resp.status_code} {resp.text[:200]}")
        return False
    return True


def set_issue_type(repo: str, number: int, type_name: str) -> bool:
    if type_name not in ISSUE_TYPES:
        print(f"Unknown issue type: {type_name}")
        return False
    resp = requests.patch(
        f"{GITHUB_API}/repos/{repo}/issues/{number}",
        headers=gh_headers(),
        json={"type": type_name},
        timeout=15,
    )
    if resp.status_code != 200:
        print(f"Failed to set issue type: {resp.status_code} {resp.text[:200]}")
        return False
    return True


def set_issue_labels(repo: str, number: int, labels: list[str]) -> bool:
    valid = [l for l in labels if l in SUBSYSTEM_LABELS]
    if not valid:
        return False
    resp = requests.post(
        f"{GITHUB_API}/repos/{repo}/issues/{number}/labels",
        headers=gh_headers(),
        json={"labels": valid},
        timeout=15,
    )
    if resp.status_code != 200:
        print(f"Failed to set labels: {resp.status_code} {resp.text[:200]}")
        return False
    return True


def post_comment(repo: str, number: int, body: str) -> bool:
    resp = requests.post(
        f"{GITHUB_API}/repos/{repo}/issues/{number}/comments",
        headers=gh_headers(),
        json={"body": body},
        timeout=15,
    )
    if resp.status_code != 201:
        print(f"Failed to post comment: {resp.status_code} {resp.text[:200]}")
        return False
    return True


def has_triage_comment(repo: str, number: int) -> bool:
    """Check if a triage comment was already posted by the bot."""
    resp = requests.get(
        f"{GITHUB_API}/repos/{repo}/issues/{number}/comments",
        headers=gh_headers(),
        params={"per_page": 100},
        timeout=15,
    )
    if resp.status_code != 200:
        return False
    for c in resp.json():
        user = c.get("user", {})
        if user.get("type") == "Bot" and "🤖" in c.get("body", ""):
            return True
    return False


def render_comment(
    cfg: dict, result: dict, auto_linked: bool, type_set: bool, labels_set: list[str]
) -> str:
    umbrella = result.get("umbrella")
    confidence = result.get("confidence", "low")
    reason = result.get("reason", "")

    lines = []

    if umbrella is None:
        lines.append(
            "🤖 **Auto-triage**: could not confidently classify this issue "
            "under any current v0.2 umbrella."
        )
    else:
        match = next((u for u in cfg["umbrellas"] if u["number"] == umbrella), None)
        title = match["title"] if match else "(unknown)"
        if auto_linked:
            lines.append(
                f"🤖 **Auto-triage**: linked as sub-issue of "
                f"**#{umbrella} — {title}** (confidence: `{confidence}`)."
            )
        else:
            lines.append(
                f"🤖 **Auto-triage suggestion**: this looks like a sub-issue of "
                f"**#{umbrella} — {title}** (confidence: `{confidence}`)."
            )

    lines.append(f"\n_Reason_: {reason}")

    actions = []
    if type_set:
        actions.append(f"type → **{result.get('type', '')}**")
    if labels_set:
        actions.append(f"labels → {', '.join(f'`{l}`' for l in labels_set)}")
    if actions:
        lines.append(f"\n_Auto-applied_: {'; '.join(actions)}")

    if auto_linked:
        lines.append(
            "\nIf this is wrong, please unlink and re-assign — "
            "your correction improves this bot."
        )
    elif umbrella is not None:
        lines.append(
            "\nIf this is correct, a maintainer can link it via the GitHub UI "
            "(Sub-issues → Add). If wrong, please correct — your correction is "
            "the training signal that improves this bot."
        )
    else:
        lines.append(
            "\nMaintainers: please pick a parent manually, or close if out of scope."
        )

    return "\n".join(lines)


def main() -> int:
    event_path = os.environ["GITHUB_EVENT_PATH"]
    event = json.loads(Path(event_path).read_text())
    issue = event["issue"]
    repo = event["repository"]["full_name"]
    number = issue["number"]
    title = issue["title"]

    api_key = os.environ.get("DASHSCOPE_API_KEY", "").strip()
    if not api_key:
        print("DASHSCOPE_API_KEY not set; skipping triage.")
        return 0

    cfg = load_config()

    if is_umbrella(cfg, title, number):
        print(f"#{number} looks like an umbrella issue; skipping.")
        return 0

    if has_triage_comment(repo, number):
        print(f"#{number} already has a triage comment; skipping.")
        return 0

    user_prompt = build_user_prompt(cfg, issue)
    try:
        result = call_qwen(api_key, SYSTEM_PROMPT, user_prompt)
    except Exception as e:
        print(f"Qwen API call failed: {e}; skipping triage.")
        return 0

    print("Qwen result:", json.dumps(result, ensure_ascii=False))

    umbrella = result.get("umbrella")
    confidence = result.get("confidence", "low")
    auto_linked = False

    has_parent = bool(issue.get("sub_issue_summary") or issue.get("parent"))
    if not has_parent:
        has_parent = check_has_parent(repo, number)

    if has_parent:
        print(f"Issue #{number} already has a parent; skipping auto-link")
    elif umbrella and should_auto_link(cfg, confidence):
        child_id = issue["id"]
        auto_linked = link_sub_issue(repo, umbrella, child_id)
        if auto_linked:
            print(f"Auto-linked #{number} as sub-issue of #{umbrella}")

    # set issue type (skip if already assigned)
    issue_type = result.get("type", "")
    type_set = False
    existing_type = issue.get("type")
    if existing_type:
        print(
            f"Issue already has type '{existing_type.get('name', '')}'; skipping type assignment"
        )
    elif issue_type in ISSUE_TYPES:
        type_set = set_issue_type(repo, number, issue_type)
        if type_set:
            print(f"Set issue type to {issue_type}")

    # set subsystem labels (only add labels not already present)
    pred_labels = result.get("labels", [])
    existing_labels = {l["name"] for l in issue.get("labels", [])}
    labels_to_add = [
        l for l in pred_labels if l in SUBSYSTEM_LABELS and l not in existing_labels
    ]
    labels_set = []
    if labels_to_add:
        if set_issue_labels(repo, number, labels_to_add):
            labels_set = labels_to_add
            print(f"Set labels: {labels_set}")
    elif pred_labels:
        already = [l for l in pred_labels if l in existing_labels]
        if already:
            print(f"Labels already present: {already}; skipping")

    comment = render_comment(cfg, result, auto_linked, type_set, labels_set)
    if post_comment(repo, number, comment):
        print(f"Posted triage comment on #{number}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
