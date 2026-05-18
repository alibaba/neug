---
name: codegraph
description: Analyze indexed codebases via graph database (neug) and vector index (zvec). Covers call graphs, dependencies, dead code, hotspots, module coupling, architecture reports, semantic search, impact analysis, bug root cause from GitHub issues, class diagrams (UML), and PR review (risk scoring, conflict detection, auto-merge candidates, labeling). Also covers creating, inspecting, and repairing a CodeScope index. Use for: code structure, who calls what, why something changed, similar functions, module boundaries, bug tracing, class relationships, PR risk/conflicts, or any question benefiting from a code knowledge graph. Applies when a `.codegraph` index exists in the workspace, or when the user wants to create one.
---

# CodeScope Q&A

CodeScope indexes source code into a two-layer knowledge graph — **structure** (functions, calls, imports, classes, modules) and **evolution** (commits, file changes, function modifications) — plus **semantic embeddings** for every function. Supports **Python, JavaScript/TypeScript, C, and Java** (including Hadoop-scale repositories with 8K+ files). This combination enables analyses that grep, LSP, or pure vector search cannot do alone. It can also **fetch GitHub issues and trace bugs to code**, and **review open PRs** — scoring per-PR risk, detecting cross-PR conflicts, identifying auto-merge candidates, and applying GitHub labels.

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
- User asks about class relationships, ownership, composition, or wants a class diagram / UML
- User wants to understand which classes own or depend on other classes
- User wants to review PRs, assess PR risk, or prioritize PR reviews
- User asks about cross-PR conflicts or which PRs can be merged independently
- User wants to find auto-merge candidates or generate a PR review report
- User asks about the blast radius or impact scope of a PR
- User wants to apply labels to PRs from analysis results
- User wants to explore PR-specific follow-up questions for a given PR
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

> **Tip:** If the `all-MiniLM-L6-v2` model fails to download (network issues, firewall, etc.):
>
> - **HuggingFace mirror**: `export HF_ENDPOINT="https://hf-mirror.com"` then retry download
> - **ModelScope** (China-friendly): download from https://www.modelscope.cn/models/sentence-transformers/all-MiniLM-L6-v2 to a local path, then load by path instead of model name:
>   ```bash
>   pip install modelscope
>   modelscope download --model sentence-transformers/all-MiniLM-L6-v2 --local_dir /path/to/all-MiniLM-L6-v2
>   # Then use the local path in Python API:
>   # cs = CodeScope(db_dir, embedding_model="/path/to/all-MiniLM-L6-v2")
>   ```

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

To add git history to an existing index (without re-indexing structure):

```bash
codegraph ingest --repo . --db $CODESCOPE_DB_DIR --commits 500
codegraph ingest --repo . --db $CODESCOPE_DB_DIR --backfill-limit 200   # add MODIFIES edges only
```

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

### Class Dependency Relationships (UML-Style)

CodeScope extracts three UML relationship types from class fields and type annotations during indexing:

| Relationship | UML symbol | Meaning | How detected |
|-------------|-----------|---------|---------------|
| `COMPOSES` | `*--` filled diamond | Strong ownership — field always holds an instance | Non-optional field assigned a constructed object |
| `AGGREGATES` | `o--` open diamond | Optional/weak reference — may be `None` | `Optional[X]`, `X \| None`, or assigned `None` |
| `INHERITS` | `<\|--` hollow arrow | Subclass extends parent | `class A(B)` |

```python
# Get all composition relationships (A strongly owns B)
list(cs.conn.execute('MATCH (c1:Class)-[:COMPOSES]->(c2:Class) RETURN c1.name, c2.name'))

# Get all aggregation relationships (A optionally holds B)
list(cs.conn.execute('MATCH (c1:Class)-[:AGGREGATES]->(c2:Class) RETURN c1.name, c2.name'))

# How many objects does a class directly own?
list(cs.conn.execute(
    'MATCH (c:Class {name: "Llama"})-[:COMPOSES]->(t:Class) RETURN t.name'
))

# Full dependency graph for a class (composition + aggregation + inheritance)
list(cs.conn.execute(
    'MATCH (c:Class {name: "GPUModelRunner"})-[r:COMPOSES|AGGREGATES]->(t:Class) '
    'RETURN type(r), t.name'
))
```

**Generating a Mermaid class diagram:**

```python
inherits  = list(cs.conn.execute('MATCH (c1:Class)-[:INHERITS]->(c2:Class) RETURN c1.name, c2.name'))
composes  = list(cs.conn.execute('MATCH (c1:Class)-[:COMPOSES]->(c2:Class) RETURN c1.name, c2.name'))
aggregates = list(cs.conn.execute('MATCH (c1:Class)-[:AGGREGATES]->(c2:Class) RETURN c1.name, c2.name'))

print('classDiagram')
for src, tgt in inherits:   print(f'    {tgt} <|-- {src}')   # parent <|-- child
for src, tgt in composes:   print(f'    {src} *-- {tgt}')    # owner *-- owned
for src, tgt in aggregates: print(f'    {src} o-- {tgt}')    # holder o-- optional
```

**Scale reference:**

| Project | Classes | INHERITS | COMPOSES | AGGREGATES | Index time |
|---------|---------|----------|----------|------------|------------|
| llama-cpp-python | 128 | 18 | 8 | 4 | ~2s |
| vllm | 4,002 | 2,185 | 3,217 | 149 | ~50s |

### Semantic Search

| Method | What it does |
|--------|-------------|
| `cs.similar(function, scope, topk=10)` | Find functions similar to a given function within a module scope |
| `cs.cross_locate(query, topk=10)` | Find semantically related functions, then reveal call-chain connections. Returns `CrossLocateResult` (see below) |
| `cs.semantic_cross_pollination(query, topk=15)` | Find similar functions across distant subsystems |

**`cross_locate` return value** — a `CrossLocateResult` dataclass (not iterable directly):

```python
result = cs.cross_locate("memory allocation error handling", topk=10)

# result.seeds: list[dict] — semantically matched functions
for seed in result.seeds:
    print(f"  [{seed['score']:.3f}] {seed['name']} ({seed['file_path']})")

# result.connections: list[dict] — call-chain links between seeds
for conn in result.connections:
    print(f"  {conn['from']} -> {conn['to']} (distance={conn['distance']}, via={conn['via']})")

# result.clusters: list[list[str]] — connected groups of seed function IDs
for cluster in result.clusters:
    print(f"  Cluster: {cluster}")
```

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

## PR Review and Analysis

CodeScope can analyze open PRs against the indexed code graph to compute structural risk scores, detect cross-PR conflicts, and generate prioritized review reports.

### Prerequisites

- A code graph must already be indexed for the target repository
- `gh` CLI must be installed and authenticated (`gh auth login`)
- `GITHUB_TOKEN` environment variable recommended to avoid rate limiting

### Unified Pipeline (CLI)

Two subcommands: `prepare` (analyze + write to DB) and `label` (apply GitHub labels + comments).

```bash
# Phase 1: Analyze PRs, detect conflicts, write to graph DB (full rebuild)
# Pipeline: cross-PR analysis → single-PR risk scoring → report + labels
codegraph pr-review prepare --db .codegraph

# Filter by author during prepare:
codegraph pr-review prepare --db .codegraph --author someone

# Override auto-detected GitHub repo (owner/repo):
codegraph pr-review prepare --db .codegraph --repo owner/repo

# Skip per-PR risk scoring (conflict-only, faster):
codegraph pr-review prepare --db .codegraph --skip-single-pr

# Phase 2: Apply labels and post conflict comments from graph DB
codegraph pr-review label --db .codegraph

# Label with dry-run (preview without API calls):
codegraph pr-review label --db .codegraph --dry-run
```

Required arg: `--db`. Local repo path derived from `--db` parent. GitHub repo auto-detected from `git remote get-url origin` (or specified via `--repo`). Optional: `--author`, `--output`, `--skip-single-pr` (prepare); `--dry-run` (label).

### Python API (for agents / scripts)

For programmatic use within the same Python process, use `PRReview` — a
high-level wrapper that manages CodeScope lifecycle automatically.

```python
from codegraph.pr_api import PRReview

# Full pipeline in 2 lines
with PRReview(db=".codegraph") as pr:
    pr.prepare()                # fetch PRs → graph DB → scoring → report
    pr.label(dry_run=True)      # preview labels without API calls

# Query after prepare (works across sessions once DB has data)
with PRReview(db=".codegraph") as pr:
    # Conflicts
    pr.conflict_prs_of("100")           # → ["101", "102"]

    # Risk
    pr.risk("100")                      # → {"number": "100", "risk_level": "HIGH", ...}

    # Classification
    pr.auto_merge_candidates()          # → [{"number": "200", ...}, ...]
    pr.conflicting_groups()             # → [["100", "101"], ["103"]]

    # All PRs in DB
    pr.all_prs()                        # → [{"number": "100", ...}, ...]

    # Functions changed by a specific PR (added / modified / deleted)
    import json
    cs = pr._open_cs()
    rows = list(cs.conn.execute(
        f"MATCH (pr:PR {{id: {json.dumps('439')}}})-[c:CHANGES]->(f:Function) "
        f"RETURN c.info AS change_type, f.name, f.file_path "
        f"ORDER BY c.info, f.name"
    ))
    for change_type, name, path in rows:
        print(f"  [{change_type}] {name} ({path})")
    # change_type: 'hunk' (modified), 'new' (added), 'deleted', 'related' (newly calls)
```

All query methods return structured Python objects — no text parsing
required.  The CLI and Python API share the same underlying implementation
(`run_prepare` / `run_label` / graph DB), so you can ``prepare`` via CLI
and query via Python, or vice versa.

For lower-level components (PRScorer, CrossPRAnalyzer, etc.), see:

```python
from codegraph.pr_analysis import GitHubClient, GraphAnalyzer, PRScorer, CrossPRAnalyzer
gh = GitHubClient(repo='owner/repo')
scorer = PRScorer(GraphAnalyzer(cs, repo_dir), repo_dir, gh)
result = scorer.analyze(gh.pr_to_entry(pr), output_dir='/tmp')  # risk_score, risk_level, peak_blast...

cross = CrossPRAnalyzer(cs, repo_dir, gh)
cross.prepare(pr_ids)  # index PR nodes into graph
cross.connected_components()  # {root: [pr_ids]} — detects conflicts
cross.update_pr_labels(assignments)  # persist labels to graph DB

# Load PR results from graph DB (no GitHub API needed)
all_results, components = cross.load_from_graph()

# Build and apply labels from analysis results
from codegraph.pr_labeler import build_label_assignments, apply_labels
assignments = build_label_assignments(all_results, components)
apply_labels(assignments, repo='owner/repo', create_labels=True)
```

For detailed workflows, Cypher patterns, and CrossPRAnalyzer query dimensions, see [pr-analysis.md](./pr-analysis.md).

### Report Structure (3 sections)

1. **Auto-merge Candidates**: LOW risk, no interface/config changes, singleton component
2. **Independent Review**: Non-trivial PRs with no cross-PR conflict
3. **Conflicting PR Groups**: PRs sharing code/call paths via connected-components (DSU)

Risk levels: CRITICAL (≥12), HIGH (≥7), MEDIUM (≥3), LOW (<3), UNKNOWN (when `--skip-single-pr`). Key signals: blast_radius (3.0×), no_test_coverage (2.0×), interface_change (2.5×), dead_code (1.5×).

### Applying Labels and Conflict Comments

After running `codegraph pr-review prepare`, run `codegraph pr-review label` to apply category labels to GitHub PRs and post conflict comments:

```bash
# Apply labels and post conflict comments:
codegraph pr-review label --db .codegraph

# Preview without making API calls:
codegraph pr-review label --db .codegraph --dry-run
```

The `label` subcommand reads PR labels from the graph DB (`pr.label` column) — no re-analysis needed. For conflicting PRs (labelled `conflicting-group-N`), it also posts a comment on the GitHub PR listing shared functions and other conflicting PRs.

Labels are computed during `prepare` from the analysis results (connected components + risk scores) and persisted to PR nodes in the graph DB (`pr.label` column, semicolon-delimited).

Label scheme:

| Category | Label | Color |
|----------|-------|-------|
| Auto-merge Candidates (Part 1) | `auto-merge-candidate` | Green |
| Independent Review (Part 2) | `independent-review` | Yellow |
| Conflicting Group N (Part 3) | `conflicting-group-N` | Red/Orange/Blue |
| Any conflicting PR (Part 3) | `conflicting-pr` | Red |

### Follow-up Exploration

PR-specific follow-up questions are automatically included in `codegraph explore` when PR nodes exist in the graph DB (i.e., after `codegraph pr-review prepare`). PR exploration is a question template set integrated into `explore`. To query a specific PR's details (conflicts, changed functions), use the `PRReview` Python API.

```bash
# After pr-review prepare, explore includes PR questions automatically:
codegraph explore --db .codegraph --top 15

# Interactive exploration (including PR follow-up questions):
codegraph explore --db .codegraph

# Focus on PR-specific questions (use reviewer role):
codegraph explore --db .codegraph --role reviewer

# Filter to only architecture questions (exclude PR patterns):
codegraph explore --db .codegraph --type architecture

# Filter to only risk questions:
codegraph explore --db .codegraph --type risk

# Filter to only PR review questions:
codegraph explore --db .codegraph --type pr-review --role reviewer
```

The `--type` filter controls which question categories appear:
- `all` (default): all categories mixed together
- `architecture`: structural design questions (fan-in, coupling, cycles)
- `risk`: risk-focused questions (structural risk + PR risk)
- `evolution`: git history questions (change attribution, modification patterns)
- `hotspot`: frequently modified code questions
- `pr-review`: PR-specific questions (impact, conflicts, test coverage)

When `--type pr-review` is specified, only PR-related questions are shown.

## How to Route Questions

The key decision is: **does the user want an exact structural answer, a fuzzy semantic one, or a bug-to-code mapping?** Every entry below is tagged with the interface layer(s) that support it: **CLI**, **Python API**, **Cypher**, or **MCP**.

### Function Discovery & Location

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| Locate a function by name (file, lines, signature, docstring) | CLI: `codegraph query "where is func_name defined"` **or** Cypher: `MATCH (f:Function {name: 'func_name'}) WHERE f.is_historical = 0 RETURN f.file_path, f.start_line, f.end_line, f.signature, f.doc_comment LIMIT 5` | CLI, Cypher |
| Check if a function exists in the index | Cypher: `MATCH (f:Function {name: 'func_name'}) WHERE f.is_historical = 0 RETURN count(f)` | Cypher |
| Find functions by name prefix/pattern | Cypher: `MATCH (f:Function) WHERE f.name STARTS WITH 'prefix_' AND f.is_historical = 0 RETURN f.name, f.file_path LIMIT 30` | Cypher |
| Find functions containing a name fragment | Cypher: `MATCH (f:Function) WHERE f.name CONTAINS 'alloc' AND f.is_historical = 0 RETURN f.name, f.file_path LIMIT 30` | Cypher |
| Find functions in a specific module | Cypher: `MATCH (f:Function)<-[:DEFINES_FUNC]-(file:File)-[:BELONGS_TO]->(m:Module {path_prefix: 'module/path'}) WHERE f.is_historical = 0 RETURN f.name, f.file_path LIMIT 50` | Cypher |
| Find which module a function belongs to | CLI: `codegraph query "which module does func_name belong to"` **or** Cypher: `MATCH (f:Function {name: 'func_name'})<-[:DEFINES_FUNC]-(file:File)-[:BELONGS_TO]->(m:Module) RETURN m.path_prefix LIMIT 1` | CLI, Cypher |
| "Who calls `free_irq`?" | Cypher: `MATCH (caller:Function)-[:CALLS]->(f:Function {name: 'free_irq'}) WHERE f.is_historical = 0 RETURN caller.name, caller.file_path LIMIT 50` | Cypher |

### Call Graph Navigation (Callers & Callees)

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| Get **direct callers** of a function | CLI: `codegraph query "who calls func_name"` **or** Cypher: `MATCH (caller:Function)-[:CALLS]->(f:Function {name: 'func_name'}) WHERE f.is_historical = 0 RETURN caller.name, caller.file_path LIMIT 50` | CLI, Cypher |
| Get **transitive callers** (up to N hops) | `cs.impact(func_name, change_desc, max_hops=N)` **or** Cypher: `MATCH (caller:Function)-[:CALLS*1..N]->(f:Function {name: 'func_name'}) WHERE f.is_historical = 0 RETURN DISTINCT caller.name, caller.file_path LIMIT 50` | Python API, Cypher |
| Get **direct callees** of a function | CLI: `codegraph query "what does func_name call"` **or** Cypher: `MATCH (f:Function {name: 'func_name'})-[:CALLS]->(callee:Function) WHERE f.is_historical = 0 RETURN callee.name, callee.file_path LIMIT 50` | CLI, Cypher |
| Get **transitive callees** (call tree) | Cypher: `MATCH (f:Function {name: 'func_name'})-[:CALLS*1..3]->(callee:Function) WHERE f.is_historical = 0 RETURN DISTINCT callee.name, callee.file_path LIMIT 50` | Cypher |
| Get impact scope of modifying a function | MCP: `codegraph_impact(function_name, change_description)` **or** `cs.impact(func_name, "refactor", max_hops=3)` | MCP, Python API |

> **Note:** `cs.impact()` is caller-only and ranks by semantic relevance. For pure structural caller/callee queries without ranking, use the Cypher patterns above.

### Class & Object Navigation

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| Find all methods in a class | Cypher: `MATCH (c:Class {name: 'ClassName'})-[:HAS_METHOD]->(f:Function) RETURN f.name, f.signature, f.file_path, f.start_line, f.end_line` | Cypher |
| Find which class a method belongs to | Cypher: `MATCH (c:Class)-[:HAS_METHOD]->(f:Function {name: 'method_name'}) RETURN c.name, c.file_path LIMIT 5` | Cypher |
| Get class inheritance chain | `cs.class_hierarchy(class_name)` **or** Cypher: `MATCH (c:Class {name: 'ClassName'})-[:INHERITS*1..10]->(base:Class) RETURN c.name, base.name` | Python API, Cypher |
| Find all subclasses of a class | Cypher: `MATCH (c:Class)-[:INHERITS]->(p:Class {name: 'ParentClass'}) RETURN c.name, c.file_path` | Cypher |
| Find composition/aggregation neighbors | Cypher: `MATCH (c:Class {name: 'ClassName'})-[r:COMPOSES|AGGREGATES]->(t:Class) RETURN type(r), t.name` | Cypher |
| "What classes extend FileSystem in Hadoop?" | Cypher: `MATCH (c:Class)-[:INHERITS]->(p:Class {name: 'FileSystem'}) RETURN c.name, c.file_path` | Cypher |
| "Find all constructors called in this module" | Cypher: `MATCH (f:Function)-[:CALLS]->(init:Function {name: '<init>'}) WHERE f.file_path CONTAINS 'module' RETURN init.class_name, f.name, f.file_path LIMIT 20` | Cypher |
| "Draw a class diagram / show class UML" | Query `COMPOSES`, `AGGREGATES`, `INHERITS` edges and render as Mermaid `classDiagram` (see Core Python API section) | Cypher |
| "What does `Llama` own / compose?" | Cypher: `MATCH (c:Class {name:'Llama'})-[:COMPOSES]->(t:Class) RETURN t.name` | Cypher |
| "Which class holds a reference to `KVCacheManager`?" | Cypher: `MATCH (c:Class)-[:COMPOSES|AGGREGATES]->(t:Class {name:'KVCacheManager'}) RETURN c.name` | Cypher |
| "Show all optional dependencies of `GPUModelRunner`" | Cypher: `MATCH (c:Class {name:'GPUModelRunner'})-[:AGGREGATES]->(t:Class) RETURN t.name` | Cypher |

### Semantic & Similarity Search

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| Find functions semantically similar to a given function | `cs.similar(func_name, scope, topk=10)` | Python API |
| "Find functions related to memory allocation" | MCP: `codegraph_vector_search("memory allocation")` **or** `cs.vector_only_search("memory allocation", topk=10)` **or** `cs.cross_locate("memory allocation", topk=10)` | MCP, Python API |
| Find functions similar across distant subsystems | MCP: `codegraph_cross_pollination("memory allocation")` **or** `cs.semantic_cross_pollination("memory allocation", topk=15)` | MCP, Python API |

### Code Quality & Risk Signals

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| "What's the most complex / risky function?" | MCP: `codegraph_hotspots(topk=10)` **or** `cs.hotspots(topk=10)` | MCP, Python API |
| "Is there dead code in the networking stack?" | MCP: `codegraph_dead_code()` **or** `cs.dead_code()` then filter by file path | MCP, Python API |
| Find circular dependencies | `cs.circular_deps()` | Python API |
| "Which modules are tightly coupled?" | MCP: `codegraph_coupling(topk=10)` **or** `cs.module_coupling(topk=10)` | MCP, Python API |
| "Which functions act as API boundaries?" | MCP: `codegraph_bridges(topk=30)` **or** `cs.bridge_functions(topk=30)` | MCP, Python API |

### Evolution & Change History

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| "How has `schedule()` changed recently?" | MCP: `codegraph_history("schedule", "kernel/sched/core.c")` **or** `cs.change_attribution("schedule", "kernel/sched/core.c", limit=20)` | MCP, Python API |
| "What functions are always changed together with `kmalloc`?" | MCP: `codegraph_cochange("kmalloc")` **or** `cs.co_change("kmalloc", min_commits=2, topk=10)` | MCP, Python API |
| "Find commits about fixing race conditions" | MCP: `codegraph_intent("fix race condition")` **or** `cs.intent_search("fix race condition", topk=10)` | MCP, Python API |
| Score commits by how many modules they touch | MCP: `codegraph_commit_modularity(topk=20)` **or** `cs.commit_modularity(topk=20)` | MCP, Python API |
| Map module modification density | MCP: `codegraph_hot_cold(topk=30)` **or** `cs.hot_cold_map(topk=30)` | MCP, Python API |
| Prove Stable Dependencies Principle | MCP: `codegraph_stability(topk=50)` **or** `cs.stability_analysis(topk=50)` | MCP, Python API |

### Bug & Issue Analysis

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| "Analyze issue #1234 from GitHub" | `cs.analyze_issue("owner", "repo", 1234)` **or** CLI: `codegraph analyze-bug owner repo 1234 --db .codegraph` | Python API, CLI |
| "Find the root cause of the crash in issue #42" | `cs.analyze_issue("owner", "repo", 42)` | Python API |
| "What code is related to this bug?" | `cs.analyze_issue(...)` or manual `cs.cross_locate(bug_description)` | Python API |
| "Why does this project have so many bugs?" | `cs.analyze_top_bugs("owner", "repo", k=10)` then aggregate hotspots | Python API |
| "Which modules have the most bugs?" | `cs.analyze_top_bugs(...)` then aggregate by file/module | Python API |
| Fetch and parse a single issue (no graph needed) | CLI: `codegraph fetch-issue owner repo 1234` | CLI |
| Fetch top-k bug issues (no graph needed) | CLI: `codegraph fetch-bugs owner repo --top 10 --label bug` | CLI |
| Batch analyze top bugs against the graph | CLI: `codegraph analyze-bugs owner repo --db .codegraph --top 10` | CLI |

### PR Review & Analysis

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| "Review all open PRs and generate report" | CLI: `codegraph pr-review prepare --db .codegraph` | CLI |
| "Which PRs can be auto-merged?" | `PRReview.auto_merge_candidates()` (after `prepare`) | Python API |
| "Are there conflicting PRs?" | `PRReview.conflicting_groups()` or `CrossPRAnalyzer.connected_components()` | Python API |
| "What's the risk of PR #42?" | `PRReview.risk("42")` | Python API |
| "What's the blast radius of this PR?" | `PRScorer.analyze(entry)` → `result.peak_blast` | Python API |
| "Which PRs modify the same function?" | `CrossPRAnalyzer.connected_components()` | Python API |
| "Label PRs with their review category" | CLI: `codegraph pr-review label --db .codegraph` | CLI |
| "Preview labels/comments without applying" | CLI: `codegraph pr-review label --db .codegraph --dry-run` | CLI |
| "Query a specific PR's conflicts" | `PRReview.conflict_prs_of("42")` | Python API |
| "Query a specific PR's changed functions" | Cypher: `MATCH (pr:PR {id: '42'})-[c:CHANGES]->(f:Function) RETURN c.info, f.name, f.file_path` | Cypher |
| "Compare two PRs for overlap" | Cypher: `MATCH (pr1:PR {id: '42'})-[c1:CHANGES]->(f:Function)<-[c2:CHANGES]-(pr2:PR {id: '43'}) RETURN f.name, f.file_path` | Cypher |
| "Explore PR follow-up questions interactively" | CLI: `codegraph explore --db .codegraph` (auto-includes PR patterns if `prepare` was run) | CLI |
| "Show only architecture questions" | CLI: `codegraph explore --db .codegraph --type architecture` | CLI |
| "Show only PR review questions" | CLI: `codegraph explore --db .codegraph --type pr-review --role reviewer` | CLI |
| "Show top PR risk questions" | CLI: `codegraph explore --db .codegraph --top 15 --role reviewer` | CLI |
| "Full PR review pipeline: analyze, label, explore" | 1) `codegraph pr-review prepare` 2) `codegraph pr-review label` 3) `codegraph explore --db .codegraph` | CLI |

### Architecture & Reports

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| "Generate a full architecture report" | CLI: `codegraph analyze --output report.md` **or** `generate_report(cs)` | CLI, Python API |
| Print human-readable index summary | `cs.summary()` **or** MCP: `codegraph_stats()` **or** CLI: `codegraph open` | Python API, MCP, CLI |
| "What's the architectural role of `mm/`?" | MCP: `codegraph_layers(topk=30)` **or** `cs.layer_discovery(topk=30)` then find `mm` entries | MCP, Python API |
| Ask a natural-language question against the index | CLI: `codegraph query "who calls free_irq?"` **or** MCP: `codegraph_query("who calls free_irq?")` | CLI, MCP |

### Indexing & Setup

| Question / Need | Best approach | Layer |
|----------------|---------------|-------|
| "Index this Java project" | CLI: `codegraph init --repo . --lang java --commits 500` | CLI |
| Create index for current repo | CLI: `codegraph init --repo . --lang auto --commits 500` | CLI |
| Add git history to existing index | CLI: `codegraph ingest --repo . --db .codegraph --commits 500` | CLI |
| Backfill function-level MODIFIES edges | CLI: `codegraph ingest --repo . --db .codegraph --backfill-limit 200` | CLI |
| Check index health and counts | CLI: `codegraph status --db .codegraph` | CLI |
| Open index and print summary | CLI: `codegraph open --db .codegraph` | CLI |
| Start MCP server | CLI: `codegraph server --db .codegraph` | CLI |

For **novel investigations** not covered by pre-built methods, compose raw Cypher queries. See [patterns.md](./patterns.md) for templates. For bug analysis patterns, see [bug-analysis.md](./bug-analysis.md). For PR analysis patterns, see [pr-analysis.md](./pr-analysis.md).

### Cypher Cheat-Sheet

These patterns cover 90% of structural navigation needs:

```cypher
-- 1. Function location + metadata
MATCH (f:Function {name: 'func_name'})
WHERE f.is_historical = 0
RETURN f.file_path, f.start_line, f.end_line, f.signature, f.doc_comment
LIMIT 5

-- 2. Direct callers
MATCH (caller:Function)-[:CALLS]->(f:Function {name: 'func_name'})
WHERE f.is_historical = 0
RETURN caller.name, caller.file_path
LIMIT 50

-- 3. Direct callees
MATCH (f:Function {name: 'func_name'})-[:CALLS]->(callee:Function)
WHERE f.is_historical = 0
RETURN callee.name, callee.file_path
LIMIT 50

-- 4. Transitive callers (2 hops)
MATCH (caller:Function)-[:CALLS*1..2]->(f:Function {name: 'func_name'})
WHERE f.is_historical = 0
RETURN DISTINCT caller.name, caller.file_path
LIMIT 50

-- 5. Module membership
MATCH (f:Function {name: 'func_name'})<-[:DEFINES_FUNC]-(file:File)-[:BELONGS_TO]->(m:Module)
RETURN m.path_prefix
LIMIT 1

-- 6. Class methods
MATCH (c:Class {name: 'ClassName'})-[:HAS_METHOD]->(f:Function)
RETURN f.name, f.signature, f.file_path, f.start_line

-- 7. Fan-in / fan-out for a specific function
MATCH (caller:Function)-[:CALLS]->(f:Function {name: 'func_name'})-[:CALLS]->(callee:Function)
WHERE f.is_historical = 0
RETURN count(DISTINCT caller) AS fan_in, count(DISTINCT callee) AS fan_out

-- 8. Find functions by semantic + structural combo
-- Step A: vector search for seed functions
-- Step B: graph query to find which seeds call each other
```

### Important Filters for Cypher

When writing Cypher queries, these filters prevent misleading results:

- **`f.is_historical = 0`** — exclude deleted/renamed functions that are still in the graph as historical records
- **`f.is_external = 0`** (on File nodes) — exclude system headers/library files
- **`c.version_tag = 'bf'`** — only backfilled commits have `MODIFIES` edges; non-backfilled commits only have `TOUCHES` (file-level) edges
- **Always use `LIMIT`** — large codebases can return hundreds of thousands of rows
- Use **`file_path` disambiguation** when function names are overloaded across files

### When to Use Python API vs. Raw Cypher vs. MCP

| Situation | Use |
|-----------|-----|
| Pure structural navigation (callers, callees, module, class) | **Cypher** — fastest, most precise |
| Semantic ranking needed (impact analysis, similarity) | **Python API** (`cs.impact()`, `cs.similar()`, `cs.cross_locate()`) |
| Combining vector + graph (cross-pollination, intent search) | **Python API** |
| One-off quick checks in a script | **Cypher** via `cs.conn.execute()` |
| External agent / LLM tool calling | **MCP tools** (`codegraph_query`, `codegraph_impact`, `codegraph_cypher`, `codegraph_hotspots`, etc.) |
| Shell automation, CI/CD, or human operators | **CLI** (`codegraph analyze`, `codegraph pr-review`, `codegraph explore`) |

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
| HuggingFace model download fails | Network/firewall blocks huggingface.co | Use `HF_ENDPOINT="https://hf-mirror.com"` or ModelScope (see Getting Started tip) |

The CLI auto-cleans lock issues on startup when possible.

## References

- **[schema.md](./schema.md)** — Full graph schema: node types, edge types, properties, Cypher syntax notes
- **[patterns.md](./patterns.md)** — Ready-to-use Cypher query templates and composition strategies
- **[bug-analysis.md](./bug-analysis.md)** — Bug analysis workflows: single issue, batch analysis, hotspot aggregation, custom pipelines
- **[pr-analysis.md](./pr-analysis.md)** — PR analysis workflows: per-PR scoring, cross-PR conflict detection, Cypher patterns, CrossPRAnalyzer usage
