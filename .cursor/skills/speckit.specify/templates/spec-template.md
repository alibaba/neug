# Feature Specification: [FEATURE NAME]

**Feature Branch**: `[###-feature-name]`  
**Created**: [DATE]  
**Status**: Draft  
**Input**: User description: "$ARGUMENTS"

## Functional Modules *(mandatory)*

<!--
  IMPORTANT: Modules should be PRIORITIZED by importance and dependencies.
  Each module must be INDEPENDENTLY TESTABLE - meaning if you implement just ONE of them,
  you should still have a viable component that can be verified.
  
  Assign priorities (P1, P2, P3, etc.) to each module, where P1 is the most critical.
  Think of each module as a standalone functional unit that can be:
  - Developed independently
  - Tested independently
  - Deployed independently
  - Demonstrated to users independently
-->

### Module 1: [Module Name] (Priority: P1)

**Purpose**: [What this module does and why it's needed]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently - e.g., "Can be fully tested by [specific action] and delivers [specific value]"]

**Key Components**:

1. **[Component 1]**: [What it does, responsibilities, key attributes without implementation]
2. **[Component 2]**: [What it does, responsibilities, key attributes without implementation]

**Functional Requirements**:

1. **FR-001**: System MUST [specific capability]
2. **FR-002**: System MUST [specific capability]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]
2. **Given** [initial state], **When** [action], **Then** [expected outcome]

**Test Strategy**:

- **Unit Tests**: [Key scenarios to test in isolation]
- **Integration Tests**: [How to verify integration with other modules]

---

### Module 2: [Module Name] (Priority: P2)

**Purpose**: [What this module does and why it's needed]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently]

**Key Components**:

1. **[Component 1]**: [What it does, responsibilities, key attributes without implementation]

**Functional Requirements**:

1. **FR-003**: System MUST [specific capability]
2. **FR-004**: System MUST [specific capability]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]

**Test Strategy**:

- **Unit Tests**: [Key scenarios to test in isolation]
- **Integration Tests**: [How to verify integration with other modules]

---

### Module 3: [Module Name] (Priority: P3)

**Purpose**: [What this module does and why it's needed]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently]

**Key Components**:

1. **[Component 1]**: [What it does, responsibilities, key attributes without implementation]

**Functional Requirements**:

1. **FR-005**: System MUST [specific capability]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]

**Test Strategy**:

- **Unit Tests**: [Key scenarios to test in isolation]
- **Integration Tests**: [How to verify integration with other modules]

---

[Add more modules as needed, each with an assigned priority]

### Edge Cases

<!--
  ACTION REQUIRED: The content in this section represents placeholders.
  Fill them out with the right edge cases.
-->

- What happens when [boundary condition]?
- How does system handle [error scenario]?

## Success Criteria *(mandatory)*

<!--
  ACTION REQUIRED: Define measurable success criteria.
  These must be technology-agnostic and measurable.
-->

### Measurable Outcomes

- **SC-001**: [Measurable metric, e.g., "Module X processes requests in under 100ms"]
- **SC-002**: [Measurable metric, e.g., "System handles 1000 concurrent users without degradation"]
- **SC-003**: [Quality metric, e.g., "All modules have >80% test coverage"]
- **SC-004**: [Integration metric, e.g., "End-to-end flow completes successfully in under 2 seconds"]
