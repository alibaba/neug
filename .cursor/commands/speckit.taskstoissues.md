---
description: Cerate a GitHub Issue for each Module in the task document.
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Locate Feature**:
    - If the current branch name can match a specs directory, i.e., `./specs/001-feature-name(#id)`, use this sepcs
    - Otherwise, user must given the feature explicity
    - The feature issue id is at the end of the directory name, i.e., `#id`

2. Run `.specify/scripts/bash/check-prerequisites.sh --json --require-tasks --include-tasks` from repo root and parse FEATURE_DIR and AVAILABLE_DOCS list. All paths must be absolute. For single quotes in args like "I'm Groot", use escape syntax: e.g 'I'\\''m Groot' (or double-quote if possible: "I'm Groot").

3. From the executed script, extract the path to **tasks**.

4. **Milestone Request**:
    - Ask users if they need to create a new or link to an existing milestone. User Approval Required. Wait For Response.
    - Using `gh api` to create a new milestone or find the existing milestone
    - All `module issue` afterwards will be added to this milestone

5. **Create Module Issue**: (For each module)
    - Create a new issue on the GitHub.
    - The issue label is `Impl` by default. Ask users for more labels. User Approval Required. Wait For Response.
    - The issue title is `[<N>-<short-name>][Impl] Module <i>: <description>`, e.g., `[001-add-frontend][Impl] Module 1: Setup for biagent`.
    - The body is about this module.
    - This `Module Issue` should be added to the milestone.
    - Update the Implement information in the `feature issue` like:
    ```
    - [ ] Implement
        - [ ] #[Module-issue-id]
        - [ ] #[Module-issue-id]
        - [ ] #[Module-issue-id]
    ```

6. **Create Task Issue** (for each task in a module):
    - Ask users whether to create an independent issue for each task. User Approval Required. Wait For Response.
    - The issue label is as same as the module issue.
    - The issue title is `[<N>-<short-name>][Impl] Task <i>: <description>`, e.g., `[001-add-frontend][Impl] Task 101: Setup for biagent`.
    - The body is about this task.
    - This `Task Issue` should NOT be added to the milestone.
    - Update the Implement information in the `feature issue` like:
    ```
    - [ ] Implement
        - [ ] #[Module-issue-id]
            - [ ] #[Task-issue-id]
            - [ ] #[Task-issue-id]
        - [ ] #[Module-issue-id]
            - [ ] #[Task-issue-id]
        - [ ] #[Module-issue-id]
            - [ ] #[Task-issue-id]
    ```
    Do NOT add extra context after "#[id]".

7. **Link Sub-issue** (Use GitHub Sub-issue api):
    - For each task in a module, set the `Task Issue` as the sub-issue of the `Module Issue`.
    - First find the issueId.
    - Then link the sub-issue using the following commands:
    ```
    gh api graphql -f query='mutation { addSubIssue(input: {issueId: "xxx", subIssueId: "xxx"}) { issue { id title } subIssue { id title } } }'
    ```

8. **Task Assign** (for each task issue and module issue):
    - List all potential assignees, using `gh api /repos/{owner}/{repos}/assignees` to get.
    - Ask users for assignment. User Approval Required. Wait For Response.
    - Set assignees for each issue as user specified.