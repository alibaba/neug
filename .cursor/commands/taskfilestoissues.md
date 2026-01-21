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

2. **Locate Tasks**. The feature spec must has a task direcotry (i.e, `tasks/`) including a `metadata.md` file and multiple `module_i.md` files. All paths must be absolute. For single quotes in args like "I'm Groot", use escape syntax: e.g 'I'\\''m Groot' (or double-quote if possible: "I'm Groot").

3. **Create Module Issue**: (For each `module_i.md` file)
    - Create a new issue on the GitHub.
    - The issue title is `[<N>-<short-name>] Module <i>: <description>`, e.g., `[001-add-frontend] Module 1: Setup for biagent`.
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

5. **Create Task Issue**: (Ask whether user want to CONTINUE to create task issues or DELAY.)
    - For each task in the `module_i.md` file, create a new issue on the GitHub.
    - The issue title is the task title in the `module_i.md` file, e.g., `[Fxxx-Txxx] <task-title>`.
    - The body is all content in the task region including `description` and `details`.
    - Set Assignee, Label, Project as same as the module issue. Do NOT set Milestone.
    - Link the task issue to the module issue. The command is as same as the one in STEP 5.

