---
name: codegraph-qa
description: Use CodeScope to analyze any indexed codebase via its graph database (neug) and vector index (zvec). Supports Python, JavaScript/TypeScript, C, and Java (including Hadoop-scale repositories). Covers call graphs, dependency analysis, dead code detection, hotspots, module coupling, architectural layering, commit history, change attribution, semantic code search, impact analysis, full architecture reports, and bug root cause analysis from GitHub issues. Use this skill whenever the user asks about code structure, code dependencies, who calls what, why something changed, finding similar functions, generating architecture reports, understanding module boundaries, analyzing GitHub issues/bugs, finding bug root causes, understanding why a project has many bugs, tracing bugs to code, indexing Java projects, or any question that benefits from a code knowledge graph — even if they don't mention "CodeScope" by name. If a `.codegraph` or similar index directory exists in the workspace, this skill applies.
---

# CodeScope Q&A

CodeScope indexes source code into a two-layer knowledge graph — **structure** (functions, calls, imports, classes, modules) and **evolution** (commits, file changes, function modifications) — plus **semantic embeddings** for every function. Supports **Python, JavaScript/TypeScript, C, and Java** (including Hadoop-scale repositories with 8K+ files). This combination enables analyses that grep, LSP, or pure vector search cannot do alone. It can also **fetch GitHub issues and trace bugs to code** — mapping bug reports to root cause candidates using the graph + vector infrastructure.

## When to Use This Skill

- User asks about call chains, callers, callees, or dependencies
- User wants to find dead code, hotspots, or architectural layers
- User asks about code history, who changed what, or why something was modified
- User wants to find semantically similar functions across a codebase
- User wants a full architecture analysis or report
- User asks about module coupling, circular dependencies, or bridge functions
- User wants to index or analyze a Java project (Maven, Gradle, plain Java)
- User wants to analyze GitHub issues or bug reports to find root causes
- User asks "why does this project have so many bugs" or "what code is most buggy"
- User wants to trace a bug report to the most relevant code locations
- A `.codegraph` directory (or similar index) exists in the workspace

## Getting Started

### Installation

```bash
pip install codegraph-ai
```

### Environment Variables (optional)

```bash
# Create Python virtural environment
python -m venv .venv

source .venv/bin/activate

# Point to a pre-built database (skip indexing)
export CODESCOPE_DB_DIR="/path/to/.linux_db"

# Offline mode for HuggingFace models
export HF_HUB_OFFLINE="1"
```

### Check Index Status

```bash
codegraph status --db $CODESCOPE_DB_DIR
```

If no index exists, create one:

```bash
codegraph init --repo . --lang auto --commits 500
```

Supported languages: `python`, `c`, `javascript`, `typescript`, `java`, or `auto` (auto-detects from file extensions).

The `--commits` flag ingests git history (for evolution queries). Without it, only structural analysis is available. Add `--backfill-limit 200` to also compute function-level `MODIFIES` edges (slower but enables `change_attribution` and `co_change`).

## Two Interfaces: CLI vs Python

**Use the CLI** for status and reports:

```bash
codegraph status --db $CODESCOPE_DB_DIR
codegraph analyze --db $CODESCOPE_DB_DIR --output report.md
```

**Use the Python API** for queries and custom analyses:

```python
import os
os.environ['HF_HUB_OFFLINE'] = '1'  # required

from codegraph.core import CodeScope
cs = CodeScope(os.environ['CODESCOPE_DB_DIR'])

# Cypher query
rows = list(cs.conn.execute('''
    MATCH (caller:Function)-[:CALLS]->(f:Function {name: "free_irq"})
    RETURN caller.name, caller.file_path LIMIT 10
'''))
for r in rows:
    print(r)

cs.close()  # always close when done
```

The Python API is more powerful — it gives you raw Cypher access and lets you chain queries.

## Core Python API

### Raw Queries

These are the building blocks for any custom analysis:

| Method | What it does |
|--------|-------------|
| `cs.conn.execute(cypher)` | Run any Cypher query against the graph — returns list of tuples |
| `cs.vector_only_search(query, topk=10)` | Semantic search over all function embeddings — returns `[{id, score}]` |
| `cs.summary()` | Print a human-readable overview of the indexed codebase |

### Structural Analysis

| Method | What it does |
|--------|-------------|
| `cs.impact(func_name, change_desc, max_hops=3)` | Find callers up to N hops, ranked by semantic relevance to the change |
| `cs.hotspots(topk=10)` | Rank functions by structural risk (fan-in × fan-out) |
| `cs.dead_code()` | Find functions with zero callers (excluding entry points) |
| `cs.circular_deps()` | Detect circular import chains at file level |
| `cs.module_coupling(topk=10)` | Find cross-module coupling pairs with call counts |
| `cs.bridge_functions(topk=30)` | Find functions called from the most distinct modules |
| `cs.layer_discovery(topk=30)` | Auto-discover infrastructure / mid / consumer layers |
| `cs.stability_analysis(topk=50)` | Correlate fan-in with modification frequency |
| `cs.class_hierarchy(class_name=None)` | Return inheritance tree for a class (or all classes) |

### Semantic Search

| Method | What it does |
|--------|-------------|
| `cs.similar(function, scope, topk=10)` | Find functions similar to a given function within a module scope |
| `cs.cross_locate(query, topk=10)` | Find semantically related functions, then reveal call-chain connections |
| `cs.semantic_cross_pollination(query, topk=15)` | Find similar functions across distant subsystems |

### Evolution (requires `--commits` during init)

| Method | What it does |
|--------|-------------|
| `cs.change_attribution(func_name, file_path=None, limit=20)` | Which commits modified a function? (requires backfill) |
| `cs.co_change(func_name, file_path=None, min_commits=2, topk=10)` | Functions that are always modified together |
| `cs.intent_search(query, topk=10)` | Find commits matching a natural-language intent |
| `cs.commit_modularity(topk=20)` | Score commits by how many modules they touch |
| `cs.hot_cold_map(topk=30)` | Module modification density |

### Report Generation

```python
from codegraph.analyzer import generate_report
report = generate_report(cs)  # full architecture analysis as markdown
```

Or via CLI:

```bash
codegraph analyze --output reports/analysis.md
```

The report covers: overview stats, subsystem distribution, top modules, architectural layers (with Mermaid diagrams), bridge functions, fan-in/fan-out hotspots, cross-module coupling, evolution hotspots, and dead code density.

## Java Support

CodeScope includes a full Java adapter that handles enterprise-scale repositories like Apache Hadoop (~8K files, ~97K functions indexed in ~3.5 minutes).

### What Gets Indexed

| Element | Graph Node/Edge | Notes |
|---------|----------------|-------|
| Classes | `Class` node | Includes generics, annotations |
| Interfaces | `Class` node | `extends` → `INHERITS` edge |
| Enums | `Class` node | Enum methods extracted |
| Methods | `Function` node | Full generic signatures, JavaDoc |
| Constructors | `Function` node (name=`<init>`) | Including `super()` calls |
| Method calls | `CALLS` edge | Receiver context preserved (`obj.method()`) |
| `new` expressions | `CALLS` edge to `ClassName.<init>` | Constructor invocations |
| Imports | `IMPORTS` edge (file→file) | Single, wildcard, static |
| Inner classes | `Class` node (name=`Outer.Inner`) | Prefixed with outer class |
| Inheritance | `INHERITS` edge | `extends` + `implements` |

### Indexing a Java Project

```bash
codegraph init --repo /path/to/java-project --lang java --commits 500
```

Or with auto-detection (auto-detects `.java` files):

```bash
codegraph init --repo /path/to/java-project --lang auto
```

### Java-Specific Exclusions

By default, these directories are excluded when indexing Java projects: `target/`, `build/`, `.gradle/`, `.idea/`, `.settings/`, `bin/`, `out/`, `test/`, `tests/`, `src/test/`.

### Java Query Examples

```python
# Find all classes that extend a specific class
list(cs.conn.execute("""
    MATCH (c:Class)-[:INHERITS]->(p:Class {name: 'FileSystem'})
    RETURN c.name, c.file_path
"""))

# Find all methods in a specific class
list(cs.conn.execute("""
    MATCH (c:Class {name: 'DefaultParser'})-[:HAS_METHOD]->(f:Function)
    RETURN f.name, f.signature
"""))

# Find constructor call chains
list(cs.conn.execute("""
    MATCH (f:Function)-[:CALLS]->(init:Function {name: '<init>'})
    WHERE init.class_name = 'Configuration'
    RETURN f.name, f.file_path LIMIT 10
"""))
```

## Bug Root Cause Analysis

CodeScope can fetch GitHub issues and map them to code using the graph + vector infrastructure. This is the core workflow for answering questions like "why does this project have so many bugs?" or "where in the code does this bug come from?"

### Prerequisites

- A code graph must already be indexed for the target repository
- `gh` CLI must be installed and authenticated (`gh auth login`)

### Bug Analysis API

#### Single Issue Analysis

```python
# Analyze a specific GitHub issue against the indexed code graph
result = cs.analyze_issue("owner", "repo", 1234, topk=10)
print(result.format_report())
```

This:
1. Fetches the issue from GitHub (or loads from cache)
2. Parses file paths, function names, and stack traces from the issue body
3. Matches extracted paths to File nodes in the graph
4. Uses semantic search (`cross_locate`) to find related code
5. Traces callers of mentioned functions via `impact()`
6. Ranks and returns root cause candidates with explanation

#### Batch Bug Analysis

```python
# Analyze top-k bug issues and get aggregated hotspot data
results = cs.analyze_top_bugs("owner", "repo", k=10, label="bug")
for r in results:
    print(f"#{r.issue.number}: {r.issue.title}")
    for c in r.candidates[:3]:
        print(f"  {c.function_name} ({c.file_path}) score={c.score:.2f}")
```

#### CLI Commands

```bash
# Fetch and parse a single issue (no graph needed)
codegraph fetch-issue owner repo 1234

# Fetch top-k bugs from a repo
codegraph fetch-bugs owner repo --top 10 --label bug

# Analyze a single bug against the code graph
codegraph analyze-bug owner repo 1234 --db .codegraph --topk 10

# Batch analyze top bugs
codegraph analyze-bugs owner repo --db .codegraph --top 10 --label bug
```

#### Lower-Level Components

For custom analysis pipelines, the components can be used individually:

```python
from codegraph.issue_fetcher import fetch_and_parse_issue
from codegraph.bug_locator import (
    resolve_paths_to_files,
    find_semantic_matches,
    trace_callers,
    rank_root_causes,
    analyze_bug,
)

# Fetch and parse (with caching)
issue = fetch_and_parse_issue("owner", "repo", 1234)
print(issue.extracted_paths)   # file paths found in body
print(issue.extracted_funcs)   # function names from stack traces
print(issue.linked_commits)    # merge commit SHAs from linked PRs

# Match paths to graph nodes
path_matches = resolve_paths_to_files(cs, issue.extracted_paths)

# Semantic search using issue description
semantic_matches = find_semantic_matches(cs, f"{issue.title}\n{issue.body}")

# Trace callers of mentioned functions
caller_traces = trace_callers(cs, issue.extracted_funcs, max_hops=2)

# Combine into ranked candidates
candidates = rank_root_causes(path_matches, semantic_matches, caller_traces, issue.extracted_funcs)
```

### Scoring System

Root cause candidates are scored by combining multiple signals:

| Signal | Score | Description |
|--------|-------|-------------|
| Direct mention | +1.0 | Function name appears in issue body/stack trace |
| File path match | +0.8 | Function is in a file mentioned in the issue |
| Semantic match | +score | Raw cosine similarity (0.0-1.0) from `cross_locate` |
| Caller relationship | +0.5/hops | Function calls a mentioned function (decays with distance) |

### Issue Cache

Parsed issues are cached at `~/.codegraph/issue_cache/{owner}_{repo}_{number}.json`. Cache hits skip the GitHub API call entirely (sub-millisecond). To force a refresh, pass `use_cache=False` or use `--no-cache` on CLI.

```python
from codegraph.issue_cache import clear_cache
clear_cache(owner="openclaw", repo="openclaw")  # clear specific repo
clear_cache()  # clear all
```

### Stack Trace Parsing

The parser automatically extracts file paths and function names from stack traces in Python, C/C++, JavaScript/Node.js, Go, and Rust formats. It also extracts `func_name()` references in backticks and inline code.

## How to Route Questions

The key decision is: **does the user want an exact structural answer, a fuzzy semantic one, or a bug-to-code mapping?**

| User asks... | Best approach |
|-------------|---------------|
| "Who calls `free_irq`?" | Cypher: `MATCH (c:Function)-[:CALLS]->(f:Function {name: 'free_irq'}) RETURN c.name, c.file_path` |
| "Find functions related to memory allocation" | `cs.vector_only_search("memory allocation")` or `cs.cross_locate("memory allocation")` |
| "What's the most complex function?" | `cs.hotspots(topk=1)` |
| "Is there dead code in the networking stack?" | `cs.dead_code()` then filter by file path |
| "How has `schedule()` changed recently?" | `cs.change_attribution("schedule", "kernel/sched/core.c")` |
| "Which modules are tightly coupled?" | `cs.module_coupling(topk=20)` |
| "Generate a full architecture report" | `codegraph analyze` or `generate_report(cs)` |
| "What's the architectural role of `mm/`?" | `cs.layer_discovery()` then find `mm` entries |
| "Which functions act as API boundaries?" | `cs.bridge_functions(topk=30)` |
| "Find commits about fixing race conditions" | `cs.intent_search("fix race condition")` |
| "What functions are always changed together with `kmalloc`?" | `cs.co_change("kmalloc")` |
| "Why does this project have so many bugs?" | `cs.analyze_top_bugs("owner", "repo", k=10)` then aggregate hotspots |
| "Analyze issue #1234 from GitHub" | `cs.analyze_issue("owner", "repo", 1234)` |
| "What code is related to this bug?" | `cs.analyze_issue(...)` or manual `cross_locate(bug_description)` |
| "Find the root cause of the crash in issue #42" | `cs.analyze_issue("owner", "repo", 42)` |
| "Which modules have the most bugs?" | `cs.analyze_top_bugs(...)` then aggregate by file/module |
| "Index this Java project" | `codegraph init --repo . --lang java` |
| "What classes extend FileSystem in Hadoop?" | Cypher: `MATCH (c:Class)-[:INHERITS]->(p:Class {name: 'FileSystem'}) RETURN c.name, c.file_path` |
| "Find all constructors called in this module" | Cypher: `MATCH (f:Function)-[:CALLS]->(init:Function {name: '<init>'}) WHERE f.file_path CONTAINS 'module' RETURN ...` |

For **novel investigations** not covered by pre-built methods, compose raw Cypher queries. See [patterns.md](./patterns.md) for templates. For bug analysis patterns, see [bug-analysis.md](./bug-analysis.md).

## Important Filters for Cypher

When writing Cypher queries, these filters prevent misleading results:

- **`f.is_historical = 0`** — exclude deleted/renamed functions that are still in the graph as historical records
- **`f.is_external = 0`** (on File nodes) — exclude system headers/library files
- **`c.version_tag = 'bf'`** — only backfilled commits have `MODIFIES` edges; non-backfilled commits only have `TOUCHES` (file-level) edges
- **Always use `LIMIT`** — large codebases can return hundreds of thousands of rows

## Checking Data Availability

Before running evolution queries, check what's available:

```python
# How many commits are indexed?
list(cs.conn.execute("MATCH (c:Commit) RETURN count(c)"))

# How many have MODIFIES edges (backfilled)?
list(cs.conn.execute("MATCH (c:Commit) WHERE c.version_tag = 'bf' RETURN count(c)"))
```

If no commits exist, evolution methods will return empty results — guide the user to run `codegraph ingest` first. If commits exist but aren't backfilled, `TOUCHES` (file-level) queries still work but `MODIFIES` (function-level) queries won't.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Database locked` | Crashed process left neug lock | `rm <db>/graph.db/neugdb.lock` |
| `Can't open lock file` | zvec LOCK file deleted | `touch <db>/vectors/LOCK` |
| `Can't lock read-write collection` | Another process holds lock | Kill the other process |
| `recovery idmap failed` | Stale WAL files | Remove empty `.log` files from `<db>/vectors/idmap.0/` |

The CLI auto-cleans lock issues on startup when possible.

## References

- **[schema.md](./schema.md)** — Full graph schema: node types, edge types, properties, Cypher syntax notes
- **[patterns.md](./patterns.md)** — Ready-to-use Cypher query templates and composition strategies
- **[bug-analysis.md](./bug-analysis.md)** — Bug analysis workflows: single issue, batch analysis, hotspot aggregation, custom pipelines
