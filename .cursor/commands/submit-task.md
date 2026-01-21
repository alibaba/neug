---
description: Submit current implementation to finish a task.
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
    - Locate the task file, i.e., `./specs/<FeatureID>-<FeatureName>(#<IssueID>)/tasks/module_i.md`.
    - Note a submission might relates to multiple tasks.

2. **Create Task Issues**:
    - Search in the feature issue (i.e., its sub-issues) to check if the task is already assoicated with an task issue.
    - If the task issue exists, skip to the next step.
    - If not, create a new task issue for the task and link it to the module issue.
    - The issue title is the task title in the `module_i.md` file, e.g., `[Fxxx-Txxx] <task-title>`.
    - The body is all content in the task region including `description` and `details`.
    - Set Assignee, Label, Project as same as the module issue. Do NOT set Milestone.
    - Link the task issue to the module issue. First find the issueId. Then link the sub-issue using the following commands:
        ```
        gh api graphql -f query='mutation { addSubIssue(input: {issueId: "xxx", subIssueId: "xxx"}) { issue { id title } subIssue { id title } } }'
        ```

3. **Create PR**:
    - Create a new PR about the task.
    - The PR title is `feat: <task-title>`. The title should be related to the task. Note if there is multiple tasks, the title should be the combination of all task titles.
    - The PR body is the summary of commited changes. Must explicitly point out how the task is implemented.
    - The first line of the PR message should be "fix #[task-issue-id]" to link to the task issue. One line per task.