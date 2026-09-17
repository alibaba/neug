---
name: create-pr
description: Submit a new Pull Request to the project.
disable-model-invocation: true
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Create PR**: Create a new PR from the current branch. If current branch is `main`, checkout to a new branch. Load the template in a temporary file `templates/pull-request.md`.

2. **Fill Metadata**:
    - Choose a prefix for the PR title. (available prefixes: feat, fix, refactor, docs, test, ci)
    - Generate a good title for the PR. You can use the related issue title as a reference.
    - Choose reviewers. Leave empty as default.
    - Fill the content of the PR body.

3. **User Revision**:
    - Open the temporary file and let the user review the PR content.
    - Modify the PR content by the user's feedback until the user is approved.

4. **Submit PR**: Submit the PR to the project.
    - Note the initial lines are PR settings and the remaining part is the real PR body.
    - All PR settings must be set when creating the PR.
    ```
    gh pr create \
        --title "<pr-title>" \
        --body "<pr-content>" \
        --reviewer "<reviewer>"
    ```
