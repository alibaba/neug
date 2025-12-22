---
description: Check comments from the Pull Request on the GitHub and update code.
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).


## Process

1. Locate the Pull Request.

    1. The branch name contains the feature issue id.
    2. Find the related PR, e.g., `Specify: #10`.
    3. Check this PR is merging into main from the current branch. If not match, **STOP** and report to users.

2. Get PR information.

    Use command `gh pr view` to obtain all comments.

3. Update code.

    For each comment, fix problem in the workspace.

4. Report summary.

    Generate a summary to show what changes.