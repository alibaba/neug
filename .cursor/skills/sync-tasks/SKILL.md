---
name: sync-tasks
description: Cerate or Update a GitHub Issue for each Task in the task document.
disable-model-invocation: true
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Task Confirmation**:
    - User must explicitly provides the **Task ID**.
    - The format is `<FeatureID>-<TaskID>`, e.g., `F001-T101`.
    - Confirm Feature and Module. The Feature ID is given above. The Module ID is the first digit of the Task ID, e.g., `F001-T101` -> Feature 001, Module 1.
    - Locate the task file, i.e., `./specs/<FeatureID>-<FeatureName>/tasks/module_i.md`.
    - Extract the GitHub Feature Issue from the `tasks/metadata.md` file.
    - Note a submission might relates to multiple tasks.

3. **Check Current Modules**:
    - View the feature issue and its sub-issues to locate the module issue that related to the task.
    - If the module issue is not existing, STOP and ask users to create modules first by `/sync-modules`.

4. **Create Task Issue**:
    - For each task required by users, create a new issue on the GitHub.
    - The issue title is the task title in the `module_i.md` file, e.g., `[Fxxx-Txxx] <task-title>`.
    - The body is all content in the task region including `description` and `details`.
    - Assignee, Label, Project are listed in the initial of the `module_i.md` file. Do NOT set Milestone.

5. **Update GitHub Information**:
    - If a new task is created or the content is modified, update the feature issue and module issue on the GitHub.

4. **Link Sub-issue** (Use GitHub Sub-issue api):
    - Set created Task Issue as the sub-issue of the Module Issue.
    - First find the issueId.
    - Then link the sub-issue using the following commands:
        ```
        gh api graphql -f query='mutation { addSubIssue(input: {issueId: "xxx", subIssueId: "xxx"}) { issue { id title } subIssue { id title } } }'
        ```


