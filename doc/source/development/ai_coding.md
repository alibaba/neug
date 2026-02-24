# AI-Assisted Development

We apply an AI-assisted workflow called **Spec-Driven** during the development of NeuG. Inspired by [GitHub Spec-Kit](https://github.com/github/spec-kit), the spec-driven workflow standardizes the development process by:
1. Propose a new feature with natural language description
2. Plan the implementation with a comprehensive investigation
3. Task disassembly and assignment
4. Implementation (same as traditional process)

Moreover, we also provide some useful tool-kits to simplify your developments:

1. Create issues (including Bug Report and Feature Request) using running contexts
2. Create pr with the current workspace
3. Update with github comments

## Coding Agent Requirements

Spec-driven workflow is installed as `Slash Commands` in the coding agent, which is supported by most coding agent such as Cursor and Qwen Code.

We provide cursor-format commands in [Commands](https://github.com/GraphScope/neug/tree/main/.cursor/commands). You can modify them to fit your own coding agent.

### Converting Commands to Other Coding Agents (Experimental Feature)

Since different coding agents have various temporary file names and formats, we provide a script to convert the commands to adapt your coding agent:
```bash
./scripts/init_commands.sh --format|-f=<markdown|toml> --output|-o=<output-path>
```

For example, to convert the commands to Qwen Code format, run:
```bash
./scripts/init_commands.sh --format=toml --output=.qwen/commands
```

We also provide a shortcut to convert the commands to the following common formats:

| Shortcut   | Coding Agent        | Format    | Output Path               |
|------------|---------------------|-----------|---------------------------|
| amazonq    | Amazon Q            | markdown  | .amazonq/prompts          |
| claude     | Claude Code         | markdown  | .claude/commands          |
| codebuddy  | CodeBuddy           | markdown  | .codebuddy/commands       |
| codex      | Codex               | markdown  | .codex/commands           |
| gemini     | Gemini CLI          | toml      | .gemini/commands          |
| kilocode   | Kilocode            | markdown  | .kilocode/rules           |
| opencode   | OpenCode            | markdown  | .opencode/command         |
| qoder      | Qoder               | markdown  | .qoder/commands           |
| qwen       | Qwen Code           | toml      | .qwen/commands            |
| roo        | RooCode             | markdown  | .roo/rules                |
| windsurf   | Windsurf            | markdown  | .windsurf/workflows       |

You can directly use the shortcut for convenience. For example, the command above can be simplified as:
```bash
./scripts/init_commands.sh qwen
```

## Command Usage

In the chat box of your coding agent, just type the command name with a slash prefix, e.g., `/speckit.specify`. You can add more context to describe your request:

```
/speckit.specify I want to add a new feature to NeuG that ...
```

## Spec-Driven Workflow

Here is our spec-driven workflow. Note we have made a lot of modifications based on the official version to adapt to our development styles.

### Specify

Use command `/speckit.specify` to specify a new feature. You can add some description and requirements about this feature. This command will:
1. Write a proposal about the feature
2. Decompose the full feature into multiple modules and components.
3. Create a spec file in the workspace (to persistently save the context)
4. Create a feature issue on the GitHub to track this feature (from proposal to implementation)

**Note**: In this step, we only consider the requirements and interactions from a high-level perspective. Do not involve too much details.

### Plan

Use command `/speckit.plan` to plan the implementation of the feature. This command will:
1. Clarify technical details and requirements
2. Plan the project structure
3. Define the data model that used for this feature
4. Describe the algorithm model in details

**Note**: In this step, we start to consider the core algorithms and choose a better implementation plan.

### Tasks

Use command `/speckit.tasks` to generate the tasks for the feature. This command will:
1. Create a metadata file to save all related information.
2. Create a module file for each module. Each module can independently support a specific ability.
3. Create multiple tasks for each module. Each task should have an appropriate workload (e.g., several days) and can be tested and verified independently.

## Synchronize to GitHub

GitHub is powerful to track the development with `Issue` and `Project`. Therefore, we will create multiple issues to related modules and tasks. Specifically, we will:
1. Create a feature issue to track the whole feature. (This feature will be created in the specify step)
2. Create module issues for each module. These module issues will be linked to the feature issue (by sub-issues). Managers can conveniently view the development status of the current feature.
3. Create smaller task issues for each module. These task issues help developers to decompose the large module into smaller tasks and remind them to add tests in time.

Therefore, we provide two task commands called `/sync-modules` and `/sync-tasks` to synchronize the tasks to GitHub. The reason we separate these two commands is that the modules are usually constant, but the tasks are frequently adjusted during the development. These two commands also support updating issues if you have any changes.

## Other Tool-kits

We also provide some other tool-kits to simplify your developments.

1. `/create-issue`
    This command will create a new issue from the template. Currently we provided two types: Bug Report and Feature Request. A common scenario is:
    - User uses NeuG to execute some queries.
    - Some problems or inconveniences are detected.
    - User uses this command attached with related files, codes, terminal logs, etc to create a new issue.
    - Coding Agent will analyze the user input and fill the template.
    - A temporary file is appeared for users to review and verify.
    - The issue is created after users' approval.

2. `/create-pr`
    This command will create a new PR from the current workspace. It will:
    - Commit all (or specified) changes to the local repository and push to the remote.
    - Create a new PR from the current branch.
    - Linked to related issues, i.e., `fix #<issue-id>`.
    - Automatically summarize the changes and request reviewers.

3. `/update-with-comments`
    This command will grab the comments from the current PR and update the code accordingly. It will:
    - Check if the current branch is on a PR.
    - Grab the comments from the PR.
    - Update the code accordingly.
    - Commit and push after users' review.