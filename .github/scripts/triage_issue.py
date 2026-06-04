#!/usr/bin/env python3
"""
Issue triage worker.

Inputs (env):
  GITHUB_EVENT_PATH  - path to GitHub Actions event payload
  DASHSCOPE_API_KEY  - Qwen API key (DashScope)
  GITHUB_TOKEN       - for posting comments via gh CLI
  CONFIG_PATH        - path to issue-triage-config.yml (default: .github/issue-triage-config.yml)

Behavior:
  1. Read the new/edited issue from the event payload.
  2. Skip if it's an umbrella tracker (title contains "[Tracking]" or matches a configured umbrella number).
  3. Build a prompt with each umbrella's scope_in/scope_out.
  4. Call Qwen (qwen3-max via OpenAI-compatible endpoint).
  5. Post a comment with the suggestion.
  6. (Optional) If config.auto_link_threshold is met, add as sub-issue.

The script is intentionally single-file with no third-party deps beyond
`pyyaml` and `requests` (installed in the workflow). It does NOT import
the `openai` SDK to keep the dependency footprint small.
"""
from __future__ import annotations
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import requests
import yaml

QWEN_ENDPOINT = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen3-max")

CONFIDENCE_LEVELS = {"high": 3, "medium": 2, "low": 1}

SYSTEM_PROMPT = """\
You are an issue-triage assistant for the NeuG graph database project.

Given a new GitHub issue and a list of tracking umbrellas (each with scope_in
and scope_out), choose the single best umbrella, OR return null if no umbrella
clearly fits.

Rules:
- Match against scope_in. If the issue matches another umbrella's scope_out, exclude it.
- Prefer the most specific fit. Use #257 (Adhoc) only when nothing else fits.
- If the issue is itself a tracking/umbrella issue, return {"umbrella": null, "confidence": "n/a", "reason": "looks like an umbrella issue itself"}.
- Confidence: "high" = unambiguous; "medium" = good fit but some overlap; "low" = unclear.

Output ONLY a JSON object:
{"umbrella": <number_or_null>, "confidence": "high"|"medium"|"low"|"n/a", "reason": "<one sentence>"}
"""


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


def link_sub_issue(repo: str, parent_number: int, child_issue_id: int) -> bool:
    result = subprocess.run(
        ["gh", "api", f"repos/{repo}/issues/{parent_number}/sub_issues",
         "-X", "POST", "-F", f"sub_issue_id={child_issue_id}"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"Failed to link sub-issue: {result.stderr.strip()}")
        return False
    return True


def render_comment(cfg: dict, result: dict, auto_linked: bool) -> str:
    umbrella = result.get("umbrella")
    confidence = result.get("confidence", "low")
    reason = result.get("reason", "")
    if umbrella is None:
        return (
            f"🤖 **Auto-triage**: could not confidently classify this issue under any "
            f"current v0.2 umbrella.\n\n"
            f"_Reason_: {reason}\n\n"
            f"Maintainers: please pick a parent manually, or close if out of scope."
        )
    match = next((u for u in cfg["umbrellas"] if u["number"] == umbrella), None)
    label = match["title"] if match else "(unknown)"
    if auto_linked:
        return (
            f"🤖 **Auto-triage**: linked as sub-issue of "
            f"**#{umbrella} — {label}** (confidence: `{confidence}`).\n\n"
            f"_Reason_: {reason}\n\n"
            f"If this is wrong, please unlink and re-assign — your correction "
            f"improves this bot."
        )
    return (
        f"🤖 **Auto-triage suggestion**: this looks like a sub-issue of "
        f"**#{umbrella} — {label}** (confidence: `{confidence}`).\n\n"
        f"_Reason_: {reason}\n\n"
        f"If this is correct, a maintainer can link it via the GitHub UI "
        f"(Sub-issues → Add). If wrong, please correct — your correction is "
        f"the training signal that improves this bot."
    )


def post_comment(repo: str, number: int, body: str) -> None:
    subprocess.run(
        ["gh", "issue", "comment", str(number), "--repo", repo, "--body-file", "-"],
        input=body, text=True, check=True,
    )


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

    if umbrella and should_auto_link(cfg, confidence):
        child_id = issue["id"]
        auto_linked = link_sub_issue(repo, umbrella, child_id)
        if auto_linked:
            print(f"Auto-linked #{number} as sub-issue of #{umbrella}")

    comment = render_comment(cfg, result, auto_linked)
    post_comment(repo, number, comment)
    print(f"Posted triage comment on #{number}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
