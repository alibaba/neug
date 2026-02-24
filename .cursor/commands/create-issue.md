---
description: Submit a new issue to the project.
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
    - Bug Report Template.
    ```
    Issue Type: Bug
    Issue Title: [BUG] <title>
    Assignee: <assignee>
    Labels: bug, <additional labels>
    Project: NeuG v0.1
    Parent Issue: #<parent-issue-id>

    **Describe the bug**
    A clear and concise description of what the bug is.

    **Execution Logs**
    The commands that can reproduce the issue.
    1. Command 1: ...
    2. Command 2: ...
    ...

    **Expected behavior**
    A clear and concise description of what you expected to happen.

    **Error Message**
    The error message in the terminal.
    ```
    
    - Feature Request Template.
    ```
    Issue Type: Feature
    Issue Title: [Feature] <title>
    Assignee: <assignee>
    Labels: <labels>
    Project: NeuG v0.1

    **Is your feature request related to a problem? Please describe.**
    A clear and concise description of what the problem is. Ex. I'm always frustrated when [...]

    **Describe the solution you'd like**
    A clear and concise description of what you want to happen.

    **Describe alternatives you've considered**
    A clear and concise description of any alternative solutions or features you've considered.

    **Additional context**
    Add any other context or screenshots about the feature request here.
    ```

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
The default GitHub CLI is too old and not support the new projects feature. If report `Projects (classic) is being deprecated`, you need to update the GitHub CLI.
```
(type -p wget >/dev/null || (sudo apt update && sudo apt install wget -y)) \
	&& sudo mkdir -p -m 755 /etc/apt/keyrings \
	&& out=$(mktemp) && wget -nv -O$out https://cli.github.com/packages/githubcli-archive-keyring.gpg \
	&& cat $out | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null \
	&& sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
	&& sudo mkdir -p -m 755 /etc/apt/sources.list.d \
	&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
	&& sudo apt update \
	&& sudo apt install gh -y
```
And run `gh auth refresh -s project` to authorize (request users to execute in their own interactive terminal).