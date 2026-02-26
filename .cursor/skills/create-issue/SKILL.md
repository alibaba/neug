---
name: create-issue
description: Submit a new issue to the project.
disable-model-invocation: true
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Determine Issue Type**: Check the user input and determine the issue type.
    - Bug Report. Report an existing bug, error, or failure during the execution.
    - Feature Request. Request a new feature or improvement for the project.

2. **Load Template**: Load the template in a temporary file.
    - Bug Report Template. Load `templates/bug-issue.md`.
    - Feature Request Template. Load `templates/feature-issue.md`.
    

3. **Anlyze User Input**: Analyze the user input and fill the template.
    - Issue Title should be brief, e.g., "Error in executing xxx work/function/file", "Failed to execute xxx".
    - The assignee and labels should be inferred. You can use GitHub CLI commands to get available assignee and label list. `@me` can be directly used.
    - Then the issue content should be filled with the user input and context.
    - The error message should only contains important information, i.e., traceback and error log. Skip massive logs.
    - For Bug Report, must attach the context, i.e., the error message in the terminal.

4. **User Revision**:
    - Open the temporary file and let the user review the issue content.
    - Modify the issue content by the user's feedback until the user is approved.

5. **Submit Issue**: Submit the issue to the project.
    - Note the initial lines are issue settings and the remaining part is the real issue content.
    - All issue settings must be set when creating the issue.
    ```
    gh issue create \
        --title "<issue-title>" \
        --body "<issue-content>" \
        --label "<label>" \
        --assignee "<assignee>" \
        --project "<project>"
    ```
    - Using the following command to link the parent issue if required:
    ```
    gh api graphql -f query='mutation { addSubIssue(input: {issueId: "xxx", subIssueId: "xxx"}) { issue { id title } subIssue { id title } } }'
    ```

6. **Delete Temporary File**: Delete the temporary file After the issue is created.

**Appendix**: Update GitHub CLI For Projects
The default GitHub CLI is too old and not support the new projects feature. If report `Projects (classic) is being deprecated`, you need to update the GitHub CLI. The script `scripts/gh-update.sh` will update the GitHub CLI and refresh authentication for projects.