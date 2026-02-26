---
name: speckit.tasks
description: Generate an actionable, dependency-ordered tasks.md for the feature based on available design artifacts.
disable-model-invocation: true
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Locate Feature**:
   - Extract the feature name from the branch name or user input like `###-feature-name`, e.g., `001-feature-name`.
   - Find the most likely feature in the specs directory, i.e., `./specs/[###-feature-name]`. The feature id and name must be exact match.

2. **Pre-check**:
   - Make sure the feature directory exists, i.e., `./specs/[###-feature-name]`, otherwise ERROR "Feature directory not found: ./specs/[###-feature-name]".
   - Make sure the spec file exists, i.e., `./specs/[###-feature-name]/spec.md`, otherwise ERROR "Spec file not found: ./specs/[###-feature-name]/spec.md".
   - Make sure the plan file exists, i.e., `./specs/[###-feature-name]/plan.md`, otherwise ERROR "Plan file not found: ./specs/[###-feature-name]/plan.md".

3. **Execute task generation workflow**:
   - Load spec.md and extract modules and components with their priorities (P1, P2, P3, etc.)
   - Load plan.md and extract tech stack, libraries, project structure
   - Generate tasks organized by Module (see Task Generation Rules below)

4. **Generate metadata**:
   - Create a new tasks directory in the feature directory, i.e., `./specs/[###-feature-name]/tasks/`.
   - Copy the metadata template `templates/tasks-metadata-template.md` to the tasks directory as `specs/[###-feature-name]/tasks/metadata.md`
   - Feature name should match spec.md.
   - GitHub Feature Issue is the issue link of the feature issue on the GitHub.

5. **Generate modules**:
   - For each module, copy the module template `templates/tasks-module-template.md` to the module file as `specs/[###-feature-name]/tasks/module_i.md`.
   - The <i> is the module number, starting from 1.
   - The module ID and name must match the definition in the spec.md.
   - Each module includes: module goal, GitHub information (assignee, label, milestone, project), implementation tasks.
   - Each task includes: task title, description, details

6. **Report**: Generate a summary of the tasks:
   - Total task count
   - Task count per module

Context for task generation: $ARGUMENTS

Each task should be immediately executable - each task must be specific enough that an LLM can complete it without additional context.

## Task Generation Rules

**CRITICAL**: Tasks MUST be organized by module and follow the template `templates/tasks-metadata-template.md` and `templates/tasks-module-template.md`.

**Tests are OPTIONAL**: Only generate test tasks if explicitly requested in the feature specification or if user requests TDD approach.

### Metadata Rules

Metadata is saved in `tasks/metadata.md`. Metadata must follow the template including:
- Feature name
- Input
- Prerequisites
- GitHub Feature Issue
- List of modules

### Module Rules

Modules are saved in `tasks/module_i.md` (one file per module). Module must follow the template including:
- Module name
- Module goal
- GitHub settings (assignee, label, milestone, project)
- Details of each task

### Task Rules

Each module file includes multiple tasks. Task must follow the template including:
- [<FeatureID>-<TaskID>]
- Task Name
- Description
- Details

The <FeatureID> is the three-digit feature number at the beginning of the feature directory name, i.e., `001-feature-name`, e.g., F001, F002, F003...

The <TaskID> must restart for each module. For example:
* Module 1: T101, T102, T103...
* Module 2: T201, T202, T203...
...

The number of tasks is not fixed. You can add more tasks as needed but must match the spec.md.
