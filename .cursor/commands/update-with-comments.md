---
description: Check comments from the Pull Request on the GitHub and update code.
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).


## Outline


1. **Locate the Pull Request**:
    * If user explicitly provides the PR ID
        - Stash current changes and checkout to the PR branch.
    * If user does not provide the PR ID
        - Search the PR on the current branch
    If there is multiple PRs or no PR, **STOP** and report to users.

2. **Get PR information**: Use command `gh pr view` to obtain all comments.

3. **Update code**: For each comment, fix problem in the workspace.

4. **Report summary**: Generate a summary to show what changes.

5. **Submit Commits**: Commit the changes and push to the PR branch.