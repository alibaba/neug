---

description: "Task list template for feature implementation"
---

# Tasks: [FEATURE NAME]

**Input**: Design documents from `/specs/[###-feature-name]/`
**Prerequisites**: plan.md (required), spec.md (required for modules), research.md, data-model.md, contracts/

**Tests**: The examples below include test tasks. Tests are OPTIONAL - only include them if explicitly requested in the feature specification.

**Organization**: Tasks are grouped by module to enable independent implementation and testing of each module.

## Format: `[ID] [P?] [Module] Description \n Detail`

- **[ID]**: <FeatureID>-<TaskID>, e.g., F002-T001. The Feature ID is the feature prefix number.
- **[P]**: Can run in parallel (different files, no dependencies)
- **[Module]**: Which module this task belongs to (e.g., M1, M2, M3)
- Include exact file paths in descriptions
- **Detail**: including detailed description, test cases, and error handling.

## Path Conventions

- **Single project**: `src/`, `tests/` at repository root
- **Web app**: `backend/src/`, `frontend/src/`
- **Mobile**: `api/src/`, `ios/src/` or `android/src/`
- Paths shown below assume single project - adjust based on plan.md structure

<!-- 
  ============================================================================
  IMPORTANT: The tasks below are SAMPLE TASKS for illustration purposes only.
  
  The /speckit.tasks command MUST replace these with actual tasks based on:
  - User stories from spec.md (with their priorities P1, P2, P3...)
  - Feature requirements from plan.md
  - Entities from data-model.md
  - Endpoints from contracts/
  
  Tasks MUST be organized by module so each module can be:
  - Implemented independently
  - Tested independently
  - Delivered as an MVP increment
  
  DO NOT keep these sample tasks in the generated tasks.md file.
  ============================================================================
-->

## Initialize: Setup & Shared Infrastructure

**Purpose**: Project initialization and basic structure

Examples of foundational tasks (adjust based on your project):

- [ ] F001-T001 Create project structure per implementation plan
- [ ] F001-T002 Initialize [language] project with [framework] dependencies
- [ ] F001-T003 [P] Configure linting and formatting tools
- [ ] F001-T004 Setup database schema and migrations framework
- [ ] F001-T005 [P] Implement authentication/authorization framework
- [ ] F001-T006 [P] Setup API routing and middleware structure
- [ ] F001-T007 Create base models/entities that all stories depend on
- [ ] F001-T008 Configure error handling and logging infrastructure
- [ ] F001-T009 Setup environment configuration management

---

## Module 1: [Module_Title] (Priority: P1) 🎯 MVP

**Goal**: [Brief description of what this module delivers]

**Independent Test**: [How to verify this module works on its own]

- [ ] F001-T012 [P] [M1] Create [Entity1] model in src/models/[entity1].py
- [ ] F001-T013 [P] [M1] Create [Entity2] model in src/models/[entity2].py
- [ ] F001-T014 [M1] Implement [Service] in src/services/[service].py (depends on T012, T013)
- [ ] F001-T015 [M1] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] F001-T016 [M1] Add validation and error handling
- [ ] F001-T017 [M1] Add logging for module 1 operations

---

## Module 2: [Module_Title] (Priority: P1) 🎯 MVP

**Goal**: [Brief description of what this module delivers]

**Independent Test**: [How to verify this module works on its own]

- [ ] F001-T020 [P] [M2] Create [Entity] model in src/models/[entity].py
- [ ] F001-T021 [M2] Implement [Service] in src/services/[service].py
- [ ] F001-T022 [M2] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] F001-T023 [M2] Integrate with User Story 1 components (if needed)

---

### Module 3: [Module_Title] (Priority: P2)

**Goal**: [Brief description of what this module delivers]

**Independent Test**: [How to verify this module works on its own]

- [ ] F001-T020 [P] [M2] Create [Entity] model in src/models/[entity].py
- [ ] F001-T021 [M2] Implement [Service] in src/services/[service].py
- [ ] F001-T022 [M2] Implement [endpoint/feature] in src/[location]/[file].py
- [ ] F001-T023 [M2] Integrate with Module 1 components (if needed)

---

### Module 4: [Module_Title] (Priority: P2)

**Goal**: [Brief description of what this module delivers]

**Independent Test**: [How to verify this module works on its own]

- [ ] F001-T026 [P] [M3] Create [Entity] model in src/models/[entity].py
- [ ] F001-T027 [M3] Implement [Service] in src/services/[service].py
- [ ] F001-T028 [M3] Implement [endpoint/feature] in src/[location]/[file].py

---

[Add more modules as needed, following the same pattern]

---

## Finalize: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple modules

- [ ] F001-TXXX [P] Documentation updates in docs/
- [ ] F001-TXXX Code cleanup and refactoring
- [ ] F001-TXXX Performance optimization across all stories
- [ ] F001-TXXX [P] Additional unit tests (if requested) in tests/unit/
- [ ] F001-TXXX Security hardening
- [ ] F001-TXXX Run quickstart.md validation

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Modules (Phase 2+)**: All depend on Foundational phase completion
  - Modules can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3)
- **Polish (Final Phase)**: Depends on all desired modules being complete

### Modules Dependencies

- **Modules 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **Modules 2 (P2)**: Can start after Foundational (Phase 2) - May integrate with M1 but should be independently testable
- **Modules 3 (P3)**: Can start after Foundational (Phase 2) - May integrate with M1/M2 but should be independently testable

### Within Each Module

- Tests (if included) MUST be written and FAIL before implementation
- Models before services
- Services before endpoints
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel
- Once Foundational phase completes, all modules can start in parallel (if team capacity allows)
- All tests for a module marked [P] can run in parallel
- Models within a module marked [P] can run in parallel
- Different modules can be worked on in parallel by different team members

---

## Parallel Example: Module 1

```bash
# Launch all tests for Module 1 together (if tests requested):
Task: "Contract test for [endpoint] in tests/contract/test_[name].py"
Task: "Integration test for [user journey] in tests/integration/test_[name].py"

# Launch all models for Module 1 together:
Task: "Create [Entity1] model in src/models/[entity1].py"
Task: "Create [Entity2] model in src/models/[entity2].py"
```

---

## Implementation Strategy

### MVP First (Phase 1 Only)

1. Complete Phase 0: Setup
3. Complete Phase 1
4. **STOP and VALIDATE**: Test Phase 1 independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup → Setup ready
2. Add Module 1 → Test independently → Deploy/Demo (MVP!)
3. Add Module 2 → Test independently → Deploy/Demo
4. Add Module 3 → Test independently → Deploy/Demo
5. Each module adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup together
2. Once Setup is done:
   - Developer A: Module 1
   - Developer B: Module 2
   - Developer C: Module 3
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Module] label maps task to specific module for traceability
- Each module should be independently completable and testable
- Verify tests fail before implementing
- Commit after each task or logical group
- Stop at any checkpoint to validate module independently
- Avoid: vague tasks, same file conflicts, cross-module dependencies that break independence
