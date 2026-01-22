---
description: Generate an actionable, dependency-ordered tasks.md for the feature based on available design artifacts.
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Locate Feature**:
   - User must explicitly give the feature name, find the most likely feature in the specs directory, i.e., `./specs/001-feature-name(#id)`

2. **Setup**: Run `.specify/scripts/bash/check-prerequisites.sh --json` from repo root and parse FEATURE_DIR and AVAILABLE_DOCS list. All paths must be absolute.

3. **Load design documents**: Read from FEATURE_DIR:
   - **Required**: plan.md (tech stack, libraries, structure), spec.md (modules with priorities)
   - Note: Not all projects have all documents. Generate tasks based on what's available.

4. **Execute task generation workflow**:
   - Load spec.md and extract modules with their priorities (P1, P2, P3, etc.)
   - Load plan.md and extract tech stack, libraries, project structure
   - Generate tasks organized by Module (see Task Generation Rules below)

5. **Generate tasks/**: Use `.specify/templates/tasks-template.md` as structure:
   - All generated files are in the tasks/ directory.
   - Feature name should match spec.md. Feature information is saved in `tasks/metadata.md`.
   - Each module includes: module goal, GitHub information (assignee, label, milestone, project), implementation tasks. Each module is saved in `tasks/module_i.md`.
   - Each task includes: task title, description, details

6. **Report**: Output path to generated tasks.md and summary:
   - Total task count
   - Task count per module

Context for task generation: $ARGUMENTS

Each task should be immediately executable - each task must be specific enough that an LLM can complete it without additional context.

## Task Generation Rules

**CRITICAL**: Tasks MUST be organized by module and follow the template `.specify/templates/tasks-template.md`.

**Tests are OPTIONAL**: Only generate test tasks if explicitly requested in the feature specification or if user requests TDD approach.

### Metadata Rules

Metadata is saved in `tasks/metadata.md`. Metadata must follow the template including:
- Feature name
- Input
- Prerequisites
- Tests
- Organization
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

The <FeatureID> is the three-digit feature number at the beginning of the feature directory name, i.e., `001-feature-name(#id)`, e.g., F001, F002, F003...

The <TaskID> must restart for each module. For example:
* Module 1: T101, T102, T103...
* Module 2: T201, T202, T203...
...

The number of tasks is not fixed. You can add more tasks as needed but must match the spec.md.