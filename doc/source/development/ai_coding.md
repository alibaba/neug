# AI-Assisted Development

NeuG provides AI-assisted commands to streamline your development workflow. These commands are integrated with your coding agent (Cursor, Qoder, Qwen Code, etc.) as slash commands.

## Quick Start

**1. Initialize commands for your coding agent:**

```bash
./scripts/init_commands.sh <agent>   # e.g., qoder, qwen
```

> **Note:** Cursor is the default agent and does not require initialization.

See [Supported Agents](#supported-agents) for full list.

**2. Use commands in your chat box with a `/` prefix:**

```
/create-issue I encountered a segfault when running this query...
```

## Common Commands

### `/create-issue` - Report Bugs or Request Features

Create GitHub issues directly from your IDE with full context.

**Example: Report a Bug**
```
/create-issue
Type: Bug
Assignee: @who
Parent Issue: #42

The query fails with segfault. Terminal output:

E20260112 10:23:45.123456 12345 executor.cc:271] Query execution failed
Segmentation fault (core dumped)
```

**Example: Request a Feature**
```
/create-issue
Type: Feature
Assignee: @me

I need support for OPTIONAL MATCH with multiple patterns.
Currently only single pattern is supported.
```

**Tips:**
- Copy-paste terminal error output directly - the agent will extract key information
- Specify `Type: Bug` or `Type: Feature` to skip auto-detection
- Use `Assignee: @me` or `Assignee: @username` to pre-assign
- Use `Parent Issue: #id` to link as sub-issue

### `/create-pr` - Submit Pull Requests

Create PRs with auto-generated summaries linked to related issues.

**Example**
```
/create-pr
Fixes: #42
Reviewers: @who

This fixes the segfault issue in query execution.
Added null pointer check before accessing result buffer.
```

**Tips:**
- Use `Fixes: #id` to auto-close the issue when PR merges
- Use `Reviewers: @user1, @user2` to request specific reviewers
- The agent auto-generates PR title and summary from your changes

### `/update-with-comments` - Address PR Feedback

Automatically fetch and apply PR review comments.

**Example**
```
/update-with-comments
```

Or specify a PR:
```
/update-with-comments #123
```

The agent will:
1. Fetch all review comments from the PR
2. Apply requested changes to your code
3. Show a summary of modifications
4. Commit and push after your review

## Spec-Driven Workflow

For larger features, we provide a structured workflow to plan before implementing.

### Step 1: `/speckit.specify` - Define the Feature

Create a feature specification from natural language.

**Example**
```
/speckit.specify
Add graph algorithm extension support for NeuG.
Users should be able to register custom algorithms and execute them via Cypher.
```

The agent will:
1. Create a feature branch (e.g., `001-graph-algo-extension-spec`)
2. Generate a specification document in `specs/001-graph-algo-extension/spec.md`
3. Create a GitHub issue to track the feature
4. Ask clarifying questions if needed (max 3)

### Step 2: `/speckit.plan` - Plan the Implementation

Generate a technical plan from the specification.

**Example**
```
/speckit.plan 001-graph-algo-extension
```

The agent will:
1. Analyze the spec and codebase structure
2. Plan project structure, data models, and algorithms
3. Generate `plan.md` with implementation details
4. Create a linked GitHub issue

### Step 3: `/speckit.tasks` - Break Down into Tasks

Generate actionable tasks from the plan.

**Example**
```
/speckit.tasks 001-graph-algo-extension
```

The agent will:
1. Create module-level breakdown
2. Generate task files for each module
3. Each task is sized for independent testing

### Sync to GitHub

After creating specs/plans/tasks, sync them to GitHub:

```
/sync-modules 001-graph-algo-extension    # Sync module issues
/sync-tasks 001-graph-algo-extension      # Sync task issues
```

## Setup

### Prerequisites

Commands are implemented as slash commands supported by most coding agents.

### Installing Commands

We provide commands in Cursor format at `.cursor/commands/`. To convert for your agent:

```bash
./scripts/init_commands.sh <agent-shortcut>
```

### Supported Agents

| Shortcut   | Agent               |
|------------|---------------------|
| cursor     | Cursor (default)    |
| amazonq    | Amazon Q            |
| claude     | Claude Code         |
| codebuddy  | CodeBuddy           |
| codex      | Codex               |
| gemini     | Gemini CLI          |
| kilocode   | Kilocode            |
| opencode   | OpenCode            |
| qoder      | Qoder               |
| qwen       | Qwen Code           |
| roo        | RooCode             |
| windsurf   | Windsurf            |

**Example:**
```bash
./scripts/init_commands.sh qoder
```

Or manually specify format and output:
```bash
./scripts/init_commands.sh --format=toml --output=.qwen/commands
```

## Command Reference

| Command | Purpose |
|---------|---------|
| `/create-issue` | Create bug reports or feature requests |
| `/create-pr` | Submit pull requests with auto-summary |
| `/update-with-comments` | Apply PR review feedback |
| `/speckit.specify` | Define feature specifications |
| `/speckit.plan` | Create implementation plans |
| `/speckit.tasks` | Break plans into tasks |
| `/sync-modules` | Sync module issues to GitHub |
| `/sync-tasks` | Sync task issues to GitHub |
| `/generate_testcase` | Generate test cases |
