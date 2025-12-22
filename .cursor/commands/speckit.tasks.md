---
description: Generate an actionable, dependency-ordered tasks.md for the feature based on available design artifacts.
handoffs: 
  - label: Analyze For Consistency
    agent: speckit.analyze
    prompt: Run a project analysis for consistency
    send: true
  - label: Implement Project
    agent: speckit.implement
    prompt: Start the implementation in phases
    send: true
---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Outline

1. **Locate Feature**:
   - User must explicitly given the feature name, find the most likely feature in the specs directory, i.e., `./specs/001-feature-name(#id)`
   - Checkout to a new branch with a `-task` suffix, e.g., `001-feature-name-task(#id)`

2. **Setup**: Run `.specify/scripts/bash/check-prerequisites.sh --json` from repo root and parse FEATURE_DIR and AVAILABLE_DOCS list. All paths must be absolute. For single quotes in args like "I'm Groot", use escape syntax: e.g 'I'\''m Groot' (or double-quote if possible: "I'm Groot").

3. **Load design documents**: Read from FEATURE_DIR:
   - **Required**: plan.md (tech stack, libraries, structure), spec.md (modules with priorities)
   - **Optional**: data-model.md (entities), contracts/ (API endpoints), research.md (decisions), quickstart.md (test scenarios)
   - Note: Not all projects have all documents. Generate tasks based on what's available.

4. **Execute task generation workflow**:
   - Load plan.md and extract tech stack, libraries, project structure
   - Load spec.md and extract modules with their priorities (P1, P2, P3, etc.)
   - If data-model.md exists: Extract entities and map to modules
   - If contracts/ exists: Map endpoints to modules
   - If research.md exists: Extract decisions for setup tasks
   - Generate tasks organized by Module (see Task Generation Rules below)
   - Generate dependency graph showing module completion order
   - Create parallel execution examples per module
   - Validate task completeness (each module has all needed tasks, independently testable)

5. **Generate tasks.md**: Use `.specify/templates/tasks-template.md` as structure, fill with:
   - Correct feature name from plan.md
   - Phase 1: Setup tasks (project initialization)
   - Phase 2: Foundational tasks (blocking prerequisites for all modules)
   - Phase 3+: One phase per module (in priority order from spec.md)
   - Each phase includes: module goal, independent test criteria, tests (if requested), implementation tasks
   - Final Phase: Polish & cross-cutting concerns
   - All tasks must follow the strict checklist format (see Task Generation Rules below)
   - Clear file paths for each task
   - Dependencies section showing module completion order
   - Parallel execution examples per module
   - Implementation strategy section (MVP first, incremental delivery)

6. **Upload to GitHub**: Tell the user the following actions that these tasks will be push to the remote repository. Ask for permission to execute and wait for response.
   - Commit generated files and push this `task` branch to the remote.
   - Create a new issue on the GitHub. This issue has the label `Task`. The title is `[<N+1>-<short-name>][Task] <description>`, e.g., `[001-add-frontend][Task] Generate tasks for the frontend`. The body is about this task.
   - Make this `task issue` as the sub-issue of the `feature issue`. The command is:
   ```
   gh api graphql -f query='mutation { addSubIssue(input: {issueId: "xxx", subIssueId: "xxx"}) { issue { id title } subIssue { id title } } }'
   ```
   - Create a Pull Request that merge the current task branch to main. The first line of the body should be "fix #[task-issue-number]" to link to the `task issue`.
   - Update the `feature issue` that add the `tasks issue` id in the body, e.g., `- [ ] Tasks: #15`.
   - List all contributors and ask users to choose the reviewers. Note the PR author (i.e., me) cannot be reviewers.

7. **Report**: Output path to generated tasks.md and summary:
   - Total task count
   - Task count per module
   - Parallel opportunities identified
   - Independent test criteria for each module
   - Suggested MVP scope (typically just Module 1)
   - Format validation: Confirm ALL tasks follow the checklist format (checkbox, ID, labels, file paths)

Context for task generation: $ARGUMENTS

The tasks.md should be immediately executable - each task must be specific enough that an LLM can complete it without additional context.

## Task Generation Rules

**CRITICAL**: Tasks MUST be organized by module to enable independent implementation and testing.

**Tests are OPTIONAL**: Only generate test tasks if explicitly requested in the feature specification or if user requests TDD approach.

### Checklist Format (REQUIRED)

Every task MUST strictly follow this format:

```text
- [ ] [TaskID] [P?] [Module?] Description with file path
```

**Format Components**:

1. **Checkbox**: ALWAYS start with `- [ ]` (markdown checkbox)
2. **Feature ID**: e.g., `F002` if the feature is `002-biagent-frontend`
2. **Task ID**: Sequential number (T001, T002, T003...) in execution order
3. **[P] marker**: Include ONLY if task is parallelizable (different files, no dependencies on incomplete tasks)
4. **[Module] label**: REQUIRED for module phase tasks only (also could be `Module`)
   - Format: [M1], [M2], [M3], etc. (maps to modules from spec.md)
   - Setup phase: NO module label
   - Foundational phase: NO module label  
   - Module phases: MUST have module label
   - Polish phase: NO module label
5. **Description**: Clear action with exact file path

**Examples**:

- ✅ CORRECT: `- [ ] F001-T001 Create project structure per implementation plan`
- ✅ CORRECT: `- [ ] F002-T005 [P] Implement authentication middleware in src/middleware/auth.py`
- ✅ CORRECT: `- [ ] F003-T012 [P] [M1] Create User model in src/models/user.py`
- ✅ CORRECT: `- [ ] F004-T014 [M1] Implement UserService in src/services/user_service.py`
- ❌ WRONG: `- [ ] Create User model` (missing ID and Module label)
- ❌ WRONG: `F001-T001 [M1] Create model` (missing checkbox)
- ❌ WRONG: `- [ ] F001 [M1] Create User model` (missing Task ID)
- ❌ WRONG: `- [ ] T001 [M1] Create User model` (missing Feature ID)
- ❌ WRONG: `- [ ] F001-T001 [M1] Create model` (missing file path)

### Task Organization

1. **From Modules (spec.md)** - PRIMARY ORGANIZATION:
   - Each module (P1, P2, P3...) gets its own phase
   - Map all related components to their module:
     - Models needed for that module
     - Services needed for that module
     - Endpoints/UI needed for that module
     - If tests requested: Tests specific to that module
   - Mark module dependencies (most stories should be independent)

2. **From Contracts**:
   - Map each contract/endpoint → to the module it serves
   - If tests requested: Each contract → contract test task [P] before implementation in that module's phase

3. **From Data Model**:
   - Map each entity to the modules that need it
   - If entity serves multiple stories: Put in earliest module or Setup phase
   - Relationships → service layer tasks in appropriate module phase

4. **From Setup/Infrastructure**:
   - Shared infrastructure → Setup phase (Phase 1)
   - Foundational/blocking tasks → Foundational phase (Phase 2)
   - Module-specific setup → within that module's phase

### Phase Structure

- **Phase 1**: Setup (project initialization)
- **Phase 2**: Foundational (blocking prerequisites - MUST complete before modules)
- **Phase 3+**: Modules in priority order (P1, P2, P3...)
  - Within each module: Tests (if requested) → Models → Services → Endpoints → Integration
  - Each phase should be a complete, independently testable increment
- **Final Phase**: Polish & Cross-Cutting Concerns
