---
name: sync-modules
description: Cerate or Update a GitHub Issue for each Module in the task document.
disable-model-invocation: true
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Locate Feature & Tasks**:
   - Extract the feature name from the branch name or user input like `###-feature-name`, e.g., `001-feature-name`.
   - Find the most likely feature in the specs directory, i.e., `./specs/[###-feature-name]`. The feature id and name must be exact match.
   - The feature spec must has a task directory (i.e, `tasks/`) including a `metadata.md` file and multiple `module_i.md` files. All paths must be absolute.
   - Find the feature issue link in the `tasks/metadata.md` file. If not exists, search the feature issue on the GitHub by the prefix `[###-feature-name]`. The feature id and name must be exact match. If not found, stop and report error "Feature issue not found: [###-feature-name]".

2. **Check Current Modules**:
    - View the feature issue and its sub-issues on the GitHub to confirm the current modules.
    - If some modules are missing, go to Step 3 to create them.
    - If some modules are modified (local files and remote issues are different), synchronize them.

3. **Create Module Issue**: (For each `module_i.md` file)
    - Create a new issue on the GitHub.
    - The issue title is `[###-feature-name] Module <i>: <description>`, e.g., `[001-add-frontend] Module 1: Setup for biagent`.
    - The body is the `Goal` (at the beginning of the `module_i.md` file) and tasks list (at `metadata.md`) of the module. 
    - The Assignee is given in the `module_i.md` file. `@me` by default.
    - The Labels are given in the `module_i.md` file.
    - The Milestone is given in the `module_i.md` file. Skip if `None` is given.
    - The Project is given in the `module_i.md` file. Skip if `None` is given.

4. **Link Sub-issue** (Use GitHub Sub-issue api):
    - Set each Module Issue as the sub-issue of the Feature Issue.
    - First find the issueId.
    - Then link the sub-issue using the following commands:
        ```
        gh api graphql -f query='mutation { addSubIssue(input: {issueId: "xxx", subIssueId: "xxx"}) { issue { id title } subIssue { id title } } }'
        ```
