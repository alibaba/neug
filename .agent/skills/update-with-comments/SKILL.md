---
name: update-with-comments
description: Check comments from the Pull Request on the GitHub and update code.
disable-model-invocation: true
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).


## Outline


1. **Locate the Pull Request**:
    - If user explicitly provides the PR ID
        - Stash current changes and checkout to the PR branch.
    - If user does not provide the PR ID
        - Search the PR on the current branch
    - If there is multiple PRs or no PR, **STOP** and report to users.

2. **Get PR comments**:
    - Obtain conversation comments: `gh pr view <pr-id> --comments`.
    - Obtain inline comments: `gh api repos/:owner/:repo/pulls/<pr-id>/comments`.
    - Analysis all comments and align intention, e.g., Approve, Question, Request Changes, etc.

3. **Update Code**:
    - If a comment intention is `Approve`, skip the update.
    - If a comment intention is `Question`, you have to explain your understanding based on the code and comments.
    - If a comment intention is `Request Changes`, e.g., typos, code style, potential bugs, optimization chances, etc.
        - Try to update the code to fix the problem in the workspace.
        - If the update is successfully fixed and pass tests, add a new comments to confirm the update.
        - Otherwise, report the problem and ask users to review the comment.

4. **Report & Submit**: Generate a summary to show all comments and their updates. Then, commit the changes and push to the PR branch.