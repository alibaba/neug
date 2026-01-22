---
description: Execute the implementation planning workflow using the plan template to generate design artifacts.
handoffs: 
  - label: Create Tasks
    agent: speckit.tasks
    prompt: Break the plan into tasks
    send: true
  - label: Create Checklist
    agent: speckit.checklist
    prompt: Create a checklist for the following domain...
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Locate Feature**:
   - User must explicitly given the feature name, find the most likely feature in the specs directory, i.e., `./specs/001-feature-name(#id)`
   - Checkout to a new branch with a `-plan` suffix, e.g., `001-feature-name-plan(#id)`

2. **Setup**: Run `.specify/scripts/bash/setup-plan.sh --json` from repo root and parse JSON for FEATURE_SPEC, IMPL_PLAN, SPECS_DIR, BRANCH.

3. **Load context**: Read FEATURE_SPEC and `.specify/memory/constitution.md`. Load IMPL_PLAN template (already copied).

4. **Execute plan workflow**: Follow the structure in IMPL_PLAN template to:
   - Fill Technical Context (mark unknowns as "NEEDS CLARIFICATION")
   - Phase 1: Plan the project structure.
   - Phase 2: Plan the data model.
   - Phase 3: Plan the algorithm model.

## Phases

### Phase 1: Plan the project structure

1. **Review the current repository structure**:
   - Focus on source files
   - Analyze the tree view and control the depth

2. **Plan the feature position**:
   - Concentrate modifications to the least number of files
   - All modules must be considered

### Phase 2: Plan the data model

1. **Extract data model from feature spec**:
   - Extract all data models specialized for this feature (even a single variable)
   - For each data model, plan its structure, access, and update. Use a small example to illustrate.
   - . Avoid code implementation.

2. **Plan each data model**:
   - For each data model, plan its structure, access, and update. Fill the content in the plan template
   - Use simple examples to illustrate all operations.
   - Do not use complex code implementation. Use simple APIs instead.

### Phase 3: Plan the algorithm model

1. **Extract algorithm model from feature spec**:
   - Extract all algorithm models specialized for this feature that are not already provided by some packages.
   - Summarize the algorithm target.

2. **Plan each algorithm model**:
   - For each algorithm model, fill the detail of the algorithm in the plan template.
   - The structure is not fixed. But each algorithm should have clear results.
   - Focus on the algorithm description, not the implementation. Some small examples are required.

## Upload to GitHub

**Prerequisites:** Tell the user the following actions that these plan files will be push to the remote repository. Ask for permission to execute and wait for response.

   1. Commit generated files and push this `plan` branch to the remote.
   2. Create a new issue on the GitHub. This issue has the label `Plan`. The title is `[<N+1>-<short-name>][Plan] <description>`, e.g., `[001-add-frontend][Plan] generate plan for the frontend`. The body is about this plan.
   3. Make this `plan issue` as the sub-issue of the `feature issue`. The command is:
   ```
   gh api graphql -f query='mutation { addSubIssue(input: {issueId: "xxx", subIssueId: "xxx"}) { issue { id title } subIssue { id title } } }'
   ```
   4. Create a Pull Request that merge the current plan branch to main. The first line of the body should be "fix #[plan-issue-number]" to link to the `plan issue`.
   5. Update the `feature issue` that add the `plan issue` id in the body, e.g., `- [ ] Plan: #13`.
   6. List all contributors and ask users to choose the reviewers. Note the PR author (i.e., me) cannot be reviewers.

## Key rules

- Use absolute paths
- ERROR on gate failures or unresolved clarifications
