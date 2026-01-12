---
directory: tasks/metadata.md
---

# Feature: [FEATURE NAME]

**Input**: Design documents from `/specs/[###-feature-name]/`
**Prerequisites**: plan.md (required), spec.md (required for modules), research.md, data-model.md, contracts/

# Modules

[list all modules]
    [for each module, list all tasks (only id and title)]

---
directory: tasks/module_i.md

Note:
1. The <i> is the module number, starting from 1.
2. The module ID and name must match the definition in the spec.md.
---

# Module <i>: [Module Name]

**Goal**: [Brief description of what this module delivers. E.g., support some features or provide some services.]

[ask users for default values]
**Assignee**: [assignee name]
**Label**: [label name]
**Milestone**: [milestone link]
**Project**: [project link]

## [<FeatureID>-<TaskID>] [Task Name]

**description**: [brief description of the task, e.g., what aspect of the module is implemented in this task and what is the expected result]

**details**: [implementation details]
* [e.g., required tools or libraries]
* [e.g., default values for parameters]
* [e.g., implementation process]
* [e.g., error handling and corner cases]

[add more tasks as needed]
