---
name: neug-codescope
description: Vulnerability audit and dataflow analysis built on the neug CPG engine. file_audit (default, recommended): first builds a function-level summary graph (tier1) over the entire project, then incrementally builds detailed CPGs (tier2) on demand for chosen functions; both tiers coexist in a single neug database (unified schema, cross-layer refines edges; C/Java/Python/Rust). All capabilities are exposed as CLI tool subcommands (audit/overview/build/query/methods/paths), so an agent can drive the audit step by step via shell; the audit subcommand can also run the built-in LLM loop. The same tooling also supports a dataflow analysis mode (Workflow C): no vulnerability hunting, only variable-level dataflow tracing for functions of interest, producing dataflow_analysis.json. repo_audit: multi-round LLM/Cypher audit over a full-repo CPG. Use when the user asks to audit a codebase/project for vulnerabilities, scan a project with CPG/neug, run repo_audit/file_audit, find source-to-sink taint paths, or analyze dataflow inside a function. Supports C, Java, Python, and Rust.
---

# Vulnerability Audit (CPG-based interactive scanning)

## Overview

Two entry points; choose by scenario:

- **`file_audit` (default, recommended; two-stage, single neug database)**: tier1 uses
  lightweight per-language parsers
  (C=libclang, Java/Rust=tree-sitter, Python=stdlib ast) to build a
  **function-level summary graph** (Repo/SourceFile/Symbol + contains/declares/calls) over the whole project;
  tier2 **purely incrementally appends** a detailed full CPG (including all overlays up to dataflow)
  on demand (function granularity `func:<relpath>:<name>`, built per containing file) onto **the same
  neug database**,
  and stitches the summary Symbol to the detailed Method with the cross-layer edge `refines_Symbol_Method`.
  Both tiers share one unified schema (`init_audit_schema` in `codegraph/cpg/schema.py`).
  tier2 builds are idempotent and safe to repeat (watermark + append, never drops the database).
- **`repo_audit` (whole-repo mode)**: builds one big CPG for an entire directory, with multi-round LLM/Cypher interaction.
  Expensive; use only when you truly need the whole repo graphed in one shot.

## tier1 and tier2 in Detail

The core of file_audit is the design of **two graph tiers coexisting in a single neug database**. Understanding the division of labor,
capabilities, and costs of the two tiers is the prerequisite for using tool mode well.

### Design Motivation

Building a detailed CPG (AST+CFG+dataflow) for a large project in full is expensive and noisy; yet an audit must
first survey the whole picture before deciding where to dig. So it is split into two tiers:

- **tier1 is cheap, complete, and always present** — it answers "what functions does the project have, and who calls whom";
- **tier2 is expensive, on demand, appended file by file** — it answers "how does data flow inside this function".

The agent's typical path: tier1 overview to shortlist suspicious functions -> tier2 detailed graphs built only for the files containing those functions
-> source-to-sink verification on tier2.

### tier1: Function-Level Summary Graph (whole-project coverage)

- **How it is built**: the `audit` command extracts file by file with lightweight parsers — C=libclang,
  Java/Rust=tree-sitter, Python=stdlib ast. Only function/method-level information is extracted:
  name, signature, start/end lines, inter-function calls (callee names). Statements inside function bodies are not parsed.
- **Nodes**:
  - `Repo` (`repo:<name>`, repository root)
  - `SourceFile` (`file:<relpath>`, with `loc`)
  - `Symbol` (`func:<relpath>:<name>`, `symbol_kind='function'`;
    properties include `path`/`start_line`/`end_line`/`signature`; `is_external=1`
    marks a library-function stub that is called but has no definition in the project, e.g. `memcpy`/`printf`)
- **Edges**: `contains_Repo_SourceFile`, `declares_SourceFile_Symbol`,
  `calls_Symbol_Symbol` (resolved by name; both internal->internal and internal->external-stub calls are linked).
- **Nested-function attribution**: nested methods / anonymous-class methods are also extracted as independent Symbols; calls made inside them
  are attributed to the innermost function, not mistakenly recorded under the outer function.
- **Capability boundary**: only a call graph, no dataflow at all; it cannot answer "where does this variable come from".
- **Lifecycle**: every `audit` run rebuilds the unified database (tier1 reloaded); after code changes you must
  re-run `audit`.

### tier2: Detailed CPG (on demand, incremental, file granularity)

- **How it is built**: triggered by the `build` subcommand. The target is a function node_id (`func:<rel>:<name>`)
  or a file path, but **the build granularity is the file** — the complete detailed CPG of the file containing the target function
  is ingested in one go (all methods of that file, including the `<global>` top-level node).
- **Content**: identical to repo_audit's full CPG — AST, CFG, call sites, identifiers, local
  variables, type relations, and all overlays up to dataflow (the `REACHING_DEF_*` physical edge tables).
- **Nodes**: `Method`/`Call`/`Identifier`/`Local`/`TypeDecl` etc., with properties including
  `id`, `filename`, `line_number`, `code`.
- **Edges**: `AST`/`CFG`/`CONTAINS`/`INVOKES`/`REF` plus the pair-partitioned
  `REACHING_DEF_<Src>_<Dst>` (neug has no `type()`; query the physical table names directly).
- **Cross-layer edge**: `refines_Symbol_Method` (Symbol -> Method) stitches the two tiers together —
  this is the edge you traverse when entering a tier2 detailed graph from a suspicious tier1 function.
- **Incremental semantics (important)**:
  - Builds are **pure appends**: the current maximum node id of the database is taken as the watermark wm; after a new file's AST nodes
    are appended, all overlays (reaching-def, callgraph, variable scoping, etc.) are computed only over
    new nodes with `id > wm`, never touching old data.
  - `build` is **idempotent across processes**: a file already built (judged by `Method.filename`)
    returns `already_built` directly and is not re-ingested.
  - Known trade-offs: a new file does not back-fill `INVOKES` edges for calls already resolved in old files; `build`
    does not re-parse already-built files — if file contents changed, you must rebuild the whole database (re-run `audit`).
- **Capability boundary**: only built files have tier2 data; unbuilt files are blind spots at the dataflow
  level (recorded in coverage as blind_spots).

### How the Two Tiers Cooperate in an Audit

1. `overview`/tier1 queries -> inspect the call graph, shortlist suspicious functions (callers of dangerous APIs,
   external-input entry points).
2. `build` -> pay the detailed-graph cost only for the relevant files.
3. tier2 `query` -> find the node ids of sources (input points/parameters) and sinks (dangerous calls).
4. `paths` -> rebuild the evidence chain along REACHING_DEF.
5. For cross-layer verification, use `refines_Symbol_Method` to confirm that a detailed Method really corresponds to
   the Symbol shortlisted in tier1, avoiding confusion between same-named functions.

## Dataflow-Provable Bug Taxonomy (B1–B8, audit hints for the built-in LLM loop)

The built-in audit loop's system prompt injects an empirically validated bug taxonomy
(`DATAFLOW_BUG_TAXONOMY` in `repo_audit.py`, derived from the neug C++ malloc-family audit); an agent driving tool mode should investigate and eliminate false positives against the same taxonomy:

| Type | source->sink pattern | Graph evidence |
|---|---|---|
| B1 untrusted size -> allocation argument | Field or local assigned from deserialization/external input -> size argument of malloc/realloc/calloc/aligned_alloc/new[] | `(c:Call)-[:ARGUMENT]->(a), (d)-[:REACHING_DEF*1..4]->(a)`; the definition point's code is the field on the deserialize line |
| B2 inconsistent sizes from two fields | Two fields loaded from the same input: field A feeds the allocation size, field B feeds the read/memcpy length, with no `size<=capacity` check between them | Two fieldAccess definition points reaching different sinks; no lessThan+ControlStructure between the two lines |
| B3 size arithmetic wraparound | Untrusted size `*sizeof(T)`/`+K` then into the allocator; 64-bit wraparound -> small block, large access | `<operator>.multiplication`/`.add` on the REACHING_DEF chain |
| B4 unchecked return value -> write/dereference | alloc -> cast -> assignment/member -> sink (read buffer, placement-new, indexed write loop, memcpy dst), with no null check in between | No `<operator>.equals/notEquals` Call between the alloc line and the use line |
| B5 realloc-failure leak | `p = realloc(p, n)` overwrites the old pointer; failure means a leak (a destructor `if(!p) return` actually skips the free) | Direct assignment chain; the guarded form (temp variable + null check + old pointer preserved) does not count |
| B6 indexed write loop over a suspicious pointer | Downstream of a B1/B4 sink, `p[i]=v` with `i<n` exactly equal to the requested size | List in-method Calls via CONTAINS edges, look for lessThan+assignment+indirectIndexAccess |
| B7 unchecked placement-new on aligned_alloc | aligned_alloc -> cast -> member -> `new (&p[i])` loop | Same direct-chain shape as B4 |
| B8 integer truncation in a size chain | `int len = v.size()` feeding resize/memcpy arithmetic | Memory-unsafe only if the truncated value and the original value reach different operations; otherwise just data truncation |

**False-positive elimination checklist** (run on every candidate finding before reporting):

- Same-arithmetic resize-then-write is safe: a container `resize(size+K+len)` followed by a memcpy with **exactly the same arithmetic** — resize throws first, so the write never happens.
- A null-comparison Call exists between alloc and use -> rule out B4.
- Loop bound and allocation size share the same source and the same expression -> no out-of-bounds.
- The aligned_alloc size is guaranteed by an explicit padding field to be a multiple of the alignment value -> no alignment defect.
- The containing function/type has zero inbound calls repo-wide (no source in tier1 `calls_Symbol_Symbol`, no inbound INVOKES) -> dead code; downgrade to low/latent and state this in the description.
- Allocation failure only causes a crash (no attacker-controlled-size write) -> an availability issue; cap confidence at medium.

**path_queries discipline**: anchor endpoints with structural properties (method name/file/line/code) and have the
query return `src.id`/`sink.id` — node ids are the **output** of the query and must not be assumed from
earlier rounds; allocator and memcpy arguments are anchored via ARGUMENT edges (macro expansion duplicates Call nodes).

## Prerequisites

- Install the `neug-codescope` package from PyPI (the importable package name is `codegraph`):

  ```bash
  pip install "neug-codescope[all]"           # Full: all language frontends + LLM dependencies
  pip install neug-codescope                  # Minimal: neug + networkx + requests
  ```

  Extras reference: `c`=libclang (set the `LIBCLANG_PATH` environment variable if it cannot be found automatically),
  `java`=tree-sitter + tree-sitter-java, `rust`=tree-sitter-rust,
  `llm`=openai (only needed by the built-in audit loop); the Python frontend uses the standard library ast, no extra dependencies.
  After installation, `python -m codegraph.llmxcpg_neug.file_audit` (and the console script
  `codescope-audit`) works from any directory — but file_audit's tool subcommands must still be run from
  the **project root directory** (the directory passed via `audit -i`) (see the examples below).
- LLM endpoint and key (only needed by the built-in audit loop): the `LLM_API_KEY` environment variable, or `--api-key`.

## Workflow A: file_audit (tool mode, agent-driven step by step)

All of file_audit's capabilities are 6 CLI subcommands; tool commands print structured results to stdout
(JSON/text) and logs to stderr, directly parseable by an agent.

### Subcommand Overview

| Subcommand | Purpose | Output |
|---|---|---|
| `audit` | One-shot flow: build tier1 (optionally pre-warm tier2, optionally run the built-in LLM loop) | Report JSON |
| `overview` | Print the tier1 overview: files, function node_ids, inter-function calls | Text |
| `build` | Incrementally build the tier2 detailed CPG (idempotent, safe to repeat) | JSON: build result + method listing |
| `query` | Run arbitrary Cypher against the unified audit database (tier1/tier2 share one DB) | JSON: rows |
| `methods` | List the tier2 methods of built files | JSON |
| `paths` | Rebuild REACHING_DEF dataflow paths by (source_id, sink_id) | JSON: path node chains |

### Steps

1. **Build the tier1 summary graph** (`--build-only --skip-cpg` builds only the graph):

```bash
python -m codegraph.llmxcpg_neug.file_audit audit \
    -i <project dir or files...> --out <output dir> --build-only --skip-cpg
```

2. **Run all subsequent tool commands from the project root directory (the directory passed via `-i`)**; file path arguments must
   match the `path` values in tier1:

```bash
cd <project dir>
FA="python -m codegraph.llmxcpg_neug.file_audit"
DB="--db-path <output dir>/audit.neug"
```

3. **View the overview** to get function node_ids (`func:<relpath>:<name>`) and call relations:

```bash
$FA overview $DB
```

4. **Build tier2 for suspicious functions** (incrementally appended per containing file, idempotent):

```bash
$FA build "func:<relpath>:<name>" $DB      # A file path also works directly
```

5. **Cypher queries** (single database, no routing; query tier1/tier2/cross-layer freely):

```bash
$FA query "MATCH (s:Symbol)-[:refines_Symbol_Method]->(m:Method) \
    -[:CONTAINS]->(c:Call) RETURN s.node_id, c.name" $DB
```

6. **Path reconstruction**: first use query to find suspected source/sink node ids, then:

```bash
$FA paths --pair <source_id>:<sink_id> $DB   # --pair may be repeated
```

### Locating the File by Function/Interface Name (when the user did not give a file name)

Users often give only a function or interface name (e.g. "analyze nextString", "look at the UserDao interface").
In that case **do not guess the file path** — drive the agent to resolve the name into
`func:<relpath>:<name>` via tier1 graph queries before `build`:

```bash
# 1) Exact name match (there may be multiple files / same-name disambiguation suffixes @line)
$FA query "MATCH (s:Symbol) WHERE s.name = '<name>' AND s.is_external = 0 \
    RETURN s.node_id, s.path, s.start_line, s.end_line LIMIT 20" $DB

# 2) Fuzzy match when the name is only partially remembered
$FA query "MATCH (s:Symbol) WHERE s.name CONTAINS '<fragment>' AND s.is_external = 0 \
    RETURN s.node_id, s.path LIMIT 20" $DB
```

Disambiguation (when multiple candidates exist, the driving agent judges from context):
- **Use call context**: when the user mentions "the places that call X", query
  `MATCH (a:Symbol)-[:calls_Symbol_Symbol]->(b:Symbol) WHERE b.name='<name>'
  RETURN a.node_id, a.path` to see which files the callers live in and pick the semantically matching candidate.
- **Interfaces/abstract methods** (Java): an interface declaration and its implementing class methods share the same name and are all Symbols;
  distinguish them with `qualified_name` (includes file and signature); usually the implementing class is the analysis target.
- **node_ids with an `@line` suffix** (e.g. `getPath@1739`) are anonymous-class/nested same-named methods;
  for `build`, just pass the **file** (`build <path>` or any `func:` id from the same file).
- **Only `is_external=1` matched**: the name is a library-function stub with no implementation in the project —
  analyze its **callers** instead: look up inbound `calls` edges.
- If multiple candidates remain ambiguous, list them for the user (file + line) and ask for confirmation; do not select all on your own.

Once the node_id is located, proceed with the normal flow: `build` -> `query`/`paths`.

### Graph Schema Cheat Sheet (for writing Cypher)

- tier1 nodes: `Repo`, `SourceFile`, `Symbol` (functions have `symbol_kind='function'`;
  `is_external=1` marks a library stub); node_id formats: `repo:<name>`, `file:<relpath>`,
  `func:<relpath>:<name>`.
- tier1 edges: `contains_Repo_SourceFile`, `declares_SourceFile_Symbol`,
  `calls_Symbol_Symbol`.
- tier2 nodes: `Method`/`Call`/`Identifier`/`Local`/`TypeDecl`/... (with properties such as `id`,
  `filename`, `line_number`, `code`); only built files have data.
- tier2 edges: `AST`/`CFG`/`CONTAINS`/`INVOKES`/`REF`/`REACHING_DEF_*`
  (physically partitioned tables, e.g. `REACHING_DEF_Method_Call`; probe with `query` which tables hold data).
- Cross-layer: `refines_Symbol_Method` (Symbol -> Method); cross-layer analysis enters through this edge.

See [schema.md](./schema.md) for the full table reference and [patterns.md](./patterns.md) for a verified Cypher template library.

### Key Parameters of the audit Subcommand

| Parameter | Purpose |
|---|---|
| `-i` | Project directories or source files (multiple allowed); at least one of this and `--dataset` is required |
| `--dataset` | Test-set JSON; samples are extracted to disk and included in the audit |
| `--sample-limit` | Take only the first N samples of the dataset (0=all) |
| `--language` | `c`/`java`/`python`/`rust`; defaults to extension-based detection |
| `--out` | Output directory (default `file_audit_out`) |
| `--db-path` | Unified audit database path (default `<out>/audit.neug`) |
| `--detail-files` | Files for which to pre-build the tier2 detailed CPG |
| `--skip-cpg` | Keep only the tier1 summary graph |
| `--build-only` | Build the graph only, do not enter the LLM loop |
| `--dump-csv <dir>` | Export files/symbols/calls CSVs after tier1 is built |
| `--max-rounds` / `--max-rows` | Built-in LLM loop parameters |
| `--llm-model-type/--llm-model-name/...` | Model configuration for the built-in LLM loop |

Without `--build-only`, `audit` runs the built-in multi-round LLM loop (each round the LLM returns a single
JSON: `queries`/`build_cpg`/`path_queries`/`findings`/`done`), suitable for unattended
batch audits; for agent-driven step-by-step auditing, use tool mode.

## Workflow B: repo_audit (whole repo)

1. Confirm the target directory and language. `--language` may be omitted (auto-detected by file extension); pass it explicitly when ambiguous. Supports `c` (libclang), `java` (tree-sitter), `python` (stdlib ast), and `rust` (tree-sitter-rust).
2. Build + audit in one step:

```bash
export LLM_API_KEY=sk-...
python -m codegraph.llmxcpg_neug.repo_audit \
    -i <code dir> --language c \
    --cpg-path <output dir>/cpg -o <output dir>/report.json \
    --llm-model-type DeepSeek --llm-model-name deepseek-chat
```

For a self-hosted vLLM endpoint: `--llm-model-type vLLM --llm-port 9001` (or `--llm-base-url`).

3. When re-auditing the same code (changing prompt/model/parameters for experiments), add `--skip-cpg` to reuse the already-built CPG. Without it, `--cpg-path` is deleted and rebuilt.

## Key Parameters (repo_audit)

| Parameter | Purpose |
|---|---|
| `-i` | Source files or directory (required) |
| `--language` | `c`, `java`, `python`, or `rust` |
| `--cpg-path` | CPG database directory (required) |
| `--skip-cpg` | Reuse an existing CPG |
| `--max-rounds` | Number of LLM interaction rounds, default 10 (use 3 for smoke tests) |
| `--max-rows` | Maximum rows per query returned to the LLM, default 30 |
| `-o` | Report JSON path, default `repo_audit_report.json` |

## Reading the Report (audit built-in loop)

The report JSON contains:
- `findings`: `[{vulnerability_type, file, line, function, description, confidence, round}]` — presented to the user sorted by confidence; every item must be verified against the source code before being asserted.
- `paths`: BFS-rebuilt REACHING_DEF paths (`{source_id, sink_id, nodes:[{id, filename, line_number, code}]}`) — the evidence chain for each finding.
- `transcript`: the complete LLM/Cypher conversation log, used to investigate false negatives.
- Also `focus_functions` (node_ids of the shortlisted suspicious functions), `detail_files` (list of files with
  tier2 built), `detail_functions` (node_ids of built functions), and
  `db_path` (unified audit database path).

In tool mode there is no engine-written report file — the audit conclusions are produced by the driving agent itself;
see the next section for format requirements.

## Final Output Requirements (tool mode)

After completing the audit, the driving agent must produce two JSON files, saved in the audit output directory
(`<out>/`, i.e. the value of the `audit` command's `--out` parameter):

| File | Content |
|---|---|
| `<out>/audit_findings.json` | Structured findings array (see below) |
| `<out>/audit_coverage.json` | Audit coverage summary (see below) |

### 1. audit_findings.json (required)

The findings array; each entry's fields are compatible with the built-in loop report and **must carry an evidence chain**:

```json
[
  {
    "vulnerability_type": "Stack Buffer Overflow",
    "file": "src/auth.c",
    "line": 87,
    "function": "parse_token",
    "confidence": "high|medium|low",
    "description": "User input is passed to strcpy without a length check",
    "evidence": {
      "source": {"id": 142, "code": "read(0, buf, 512)", "line": 61},
      "sink":   {"id": 287, "code": "strcpy(dst, buf)", "line": 87},
      "path": [ ...node chain returned by the paths subcommand, per-hop id/code/line_number/filename... ]
    }
  }
]
```

- Attach the output of `paths --pair <source_id>:<sink_id>` verbatim as the evidence chain — this is the core of reproducibility.
- Confidence grading: `high` = path verified reachable against the source code with no sanitization;
  `medium` = a dataflow path exists but depends on unverified runtime conditions;
  `low` = suspicious only at the pattern-matching level (e.g. a call exists on the graph but no complete chain was seen).
- Verified and unverified findings must be presented separately; "a path exists on the graph" does not equal "a vulnerability exists".
- When there are no findings, write an empty array `[]`; never omit the file.

### 2. audit_coverage.json (required)

Lets the reader assess the risk of false negatives:

```json
{
  "scope": {"files": 42, "language": "c", "total_functions": 310},
  "tier2_built": {"files": ["src/auth.c"], "functions": ["func:src/auth.c:parse_token"]},
  "investigated": ["input-handling functions", "strcpy/system callers"],
  "ruled_out": ["utils.c: no external-input entry point"],
  "blind_spots": ["unbuilt files have no dataflow coverage", "cross-function propagation is best-effort"]
}
```

- The `scope` numbers come from `overview`; `tier2_built` comes from the return values of each `build`.
- `investigated`/`ruled_out` are recorded honestly by the driving agent as the investigation proceeds.

### Presentation

After both JSON files are written to disk, the interactive session should still output a markdown summary to the user (findings list +
coverage summary) for direct reading; the JSON files are for archiving and later aggregation.

## Workflow C: Dataflow Analysis Mode (not vulnerability hunting)

When the user's goal is not to find vulnerabilities but to understand **how data flows inside a function of interest** (for example:
which transformations a parameter goes through, where a variable's definition comes from, whom a value is finally passed to), use this mode. The flow is the same as
tool mode, but no vulnerability assertions are made and the deliverable becomes a dataflow report.

### Steps

1. Locate the target function in tier1 (`overview` or name-based `query`), obtaining its node_id and line numbers.
2. `build` the tier2 of the file containing that function.
3. Use `query` to map out the function's internals:
   - Entry data: the `Method`'s parameters and the `Identifier`/`Local` nodes at the entry;
   - Reaching definitions: the `REACHING_DEF_*` tables (reverse-lookup of each use's definition sources by id);
   - References and uses: `REF`/`CONTAINS` to find in which expressions/call arguments a variable appears;
   - Cross-function exits: `Call`/`INVOKES` to see which callees the data is passed to.
4. For interesting (definition, use) pairs, rebuild the node chain with `paths --pair`.
5. Write `<out>/dataflow_analysis.json` in the format below, and give a markdown summary in the session.

### Output Format: `<out>/dataflow_analysis.json`

```json
{
  "subject": {
    "function": "func:src/auth.c:parse_token",
    "file": "src/auth.c",
    "lines": [45, 120],
    "question": "Where does the parameter buf flow after entering the function"
  },
  "inputs": [
    {"id": 96, "name": "buf", "kind": "parameter", "line": 45}
  ],
  "flows": [
    {
      "variable": "buf",
      "definition": {"id": 96, "code": "char *buf", "line": 45},
      "uses": [
        {"id": 142, "code": "read(0, buf, 512)", "line": 61, "role": "read destination"},
        {"id": 287, "code": "strcpy(dst, buf)", "line": 87, "role": "strcpy source argument"}
      ],
      "path": [ ...node chain returned by the paths subcommand, attached verbatim... ],
      "summary": "buf is first filled by read, then passed as the source into strcpy"
    }
  ],
  "cross_function": [
    {"callee": "validate", "call_line": 80, "arg_ids": [142],
     "note": "A derived value of buf leaves this file via validate(); tier2 does not cover its internals"}
  ],
  "unresolved": [
    "Propagation through the function pointer ctx->get_glyph_index cannot be tracked statically"
  ],
  "coverage": {
    "tier2_built": ["src/auth.c"],
    "blind_spots": ["Propagation inside unbuilt callee files is invisible"]
  }
}
```

Field conventions:
- `subject.question`: record in one sentence the specific question the user/agent is analyzing this time; the report is organized around it.
- `flows`: one entry per tracked variable; in `uses`, `role` describes the use in natural language
  (assignment target, call argument, condition check, return, etc.).
- `path`: the evidence chain must likewise be attached verbatim from `paths` output.
- `cross_function`/`unresolved`: honestly record where data flows out of this file, or links that cannot be tracked
  statically (function pointers, macros, external libraries) — this is the dataflow report's "boundary statement".
- This mode **outputs no findings and assigns no confidence ratings**; if an incidental suspicious point is noticed, you may
  note "consider re-checking in audit mode" in the markdown summary, but do not write it into this report.

## Troubleshooting

- `No C source files found` — wrong directory or language mismatch; check `-i` and `--language`.
- `audit DB not found` — tool commands require tier1 first: run `audit --build-only --skip-cpg` first.
- `build` reports `file not found` — tool commands must be run from the project root directory (the directory passed via `-i`),
  and paths must match the `path` values in `overview`.
- LLM connection errors (built-in loop only) — confirm the endpoint is online (`--llm-port`/`--llm-base-url`) and `LLM_API_KEY` is set.
- libclang fails to load — point `LIBCLANG_PATH` at the libclang dynamic library.
- Zero findings but the code looks suspicious — add more `build` targets, broaden `query` exploration, or rerun the built-in loop with a stronger model.
- repo_audit has no incremental update — after code changes you must rebuild from scratch (drop `--skip-cpg`).
- file_audit rebuilds the unified database on every `audit` (tier1 reloaded); tier2 tool commands
  (`build`) are persistently incremental across invocations.

## Boundary Notes

- Each run builds a single-language CPG; cross-language analysis is not supported.
- file_audit's summary-graph extraction supports C (libclang), Java (tree-sitter), Python (stdlib ast), and Rust (tree-sitter-rust); files in other languages only get a SourceFile node registered. In mixed projects, detailed CPG construction is dispatched by file extension.
- Detection quality depends on REACHING_DEF coverage: intra-function dataflow is reliable; cross-function coverage is best-effort.
- Known trade-offs of incremental builds: a new file does not back-fill INVOKES edges for calls already resolved in old files
  (existing call edges are not recomputed); `build` only processes new files and does not re-parse already-built ones.
- Findings are "hypotheses + graph-path corroboration" — always confirm against the source code before reporting to the user.
