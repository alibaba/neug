# Verified Cypher Template Library

All templates are executed through file_audit's `query` subcommand (tier1/tier2 share one database, no routing needed):

```bash
FA="python -m codegraph.llmxcpg_neug.file_audit"
DB="--db-path <output-dir>/audit.neug"
$FA query "<Cypher>" $DB
```

Build the database first (`audit --build-only --skip-cpg`) or there is nothing to query; tier2 templates only return data for files that have been `build`-ed. For table structures and dialect pitfalls, see [schema.md](schema.md).

## tier1: Locating Symbols and the Call Graph

```cypher
-- Resolve a node_id by function name (exact / fuzzy; the right-hand side of CONTAINS must be a literal)
MATCH (s:Symbol) WHERE s.name = 'parse_token' AND s.is_external = 0
RETURN s.node_id, s.path, s.start_line, s.end_line LIMIT 20
MATCH (s:Symbol) WHERE s.name CONTAINS 'parse' AND s.is_external = 0
RETURN s.node_id, s.path LIMIT 20

-- Direct callers / callees (edge table name = calls_Symbol_Symbol, pair-partitioned naming)
MATCH (a:Symbol)-[:calls_Symbol_Symbol]->(b:Symbol {node_id: 'func:src/auth.c:parse_token'})
RETURN DISTINCT a.node_id, a.path
MATCH (a:Symbol {node_id: 'func:src/auth.c:parse_token'})-[:calls_Symbol_Symbol]->(b)
RETURN DISTINCT b.node_id, b.is_external

-- Multi-hop call chains (variable-length paths)
MATCH (a:Symbol {node_id: 'func:src/main.c:main'})-[:calls_Symbol_Symbol*1..3]->(b)
RETURN DISTINCT b.node_id

-- Callers of dangerous APIs (inbound edges of external stubs)
MATCH (a:Symbol)-[:calls_Symbol_Symbol]->(b:Symbol)
WHERE b.is_external = 1 AND b.name IN ['strcpy', 'sprintf', 'system']
RETURN a.node_id, b.name LIMIT 30

-- File -> function listing
MATCH (f:SourceFile {node_id: 'file:src/auth.c'})-[:declares_SourceFile_Symbol]->(s)
RETURN s.node_id, s.name, s.start_line, s.end_line
```

## Cross-Layer: tier1 -> tier2

```cypher
-- Enter the detailed Method from a summary Symbol (refines is the only cross-layer edge)
MATCH (s:Symbol {node_id: 'func:src/auth.c:parse_token'})
  -[:refines_Symbol_Method]->(m:Method)
RETURN m.id, m.full_name, m.line_number, m.line_number_end

-- Reverse check: which summary function does a detailed Method belong to (for disambiguating same-named functions)
MATCH (s:Symbol)-[:refines_Symbol_Method]->(m:Method)
WHERE m.filename = 'src/auth.c' RETURN s.node_id, m.id, m.name
```

## tier2: AST and Method Bodies

```cypher
-- All call sites inside a method body
MATCH (m:Method {id: 12})-[:CONTAINS]->(c:Call)
RETURN c.id, c.name, c.method_full_name, c.line_number, c.code

-- Arguments of a call (ARGUMENT edges, argument_index starts at 0)
MATCH (c:Call {id: 287})-[a:ARGUMENT]->(x)
RETURN a, x.id, x.code, x.argument_index   -- Note: ARGUMENT has no property columns; return the edge directly
MATCH (c:Call {id: 287})-[:ARGUMENT]->(x)
RETURN x.id, x.code, x.argument_index ORDER BY x.argument_index

-- Identifiers/locals inside a method (dataflow endpoint candidates)
MATCH (m:Method {id: 12})-[:CONTAINS]->(i:Identifier)
RETURN i.id, i.name, i.line_number, i.code
MATCH (m:Method {id: 12})-[:CONTAINS]->(l:Local)
RETURN l.id, l.name, l.type_full_name

-- Control structures (branches/loops)
MATCH (m:Method {id: 12})-[:CONTAINS]->(cs:ControlStructure)
RETURN cs.id, cs.control_structure_type, cs.line_number, cs.code
```

## tier2: Call Graph / Control Flow / Dataflow

```cypher
-- Which Method a call site resolved to (INVOKES; there is no CALL table, never write [:CALL])
MATCH (c:Call {id: 287})-[:INVOKES]->(t:Method)
RETURN t.id, t.full_name, t.filename

-- CFG successors (single hop) and variable-length reachability
MATCH (n {id: 142})-[:CFG]->(s) RETURN s.id, s.code
MATCH (n {id: 142})-[:CFG*1..8]->(s) RETURN DISTINCT s.id

-- Dominance (n dominates m => n lies on every path from the entry to m)
MATCH (d)-[:DOMINATE]->(m {id: 287}) RETURN d.id, d.code
MATCH (m {id: 287})-[:POST_DOMINATE]->(p) RETURN p.id, p.code

-- Reaching definitions: definition sources of a use node (variable property = propagated variable name)
MATCH (def)-[r:REACHING_DEF]->(use {id: 287})
RETURN def.id, def.code, r.variable, use.code

-- Which uses a definition flows to
MATCH (def {id: 96})-[r:REACHING_DEF]->(use)
RETURN use.id, use.code, r.variable
```

## source-to-sink (the Main Audit Path)

```cypher
-- 1. Find sinks: dangerous call sites
MATCH (m:Method)-[:CONTAINS]->(c:Call)
WHERE c.name IN ['strcpy', 'sprintf', 'gets']
RETURN m.filename, c.id, c.name, c.line_number, c.code

-- 2. Find sources: external-input call sites (read/recv/fgets/parameter entries)
MATCH (m:Method)-[:CONTAINS]->(c:Call)
WHERE c.name IN ['read', 'recv', 'fgets']
RETURN m.filename, c.id, c.name, c.line_number

-- 3. Verify reachability: does the source define (over several hops) into a sink argument
MATCH (src {id: 142})-[r:REACHING_DEF*1..6]->(sink_arg {id: 286})
RETURN r   -- Any row means reachable; rebuild the full node chain with file_audit paths --pair 142:286
```

Multi-hop REACHING_DEF via variable-length paths is fine for existence checks; **always rebuild the complete evidence chain via
`file_audit paths --pair <src>:<sink>`** (BFS reconstruction with per-hop code/line numbers).
Do not hand-assemble paths in Cypher.

## Dialect Pitfalls (read before writing queries)

- The right-hand side of `CONTAINS` must be a literal (bound parameters raise an error); `IN` must be a literal list (bound array parameters segfault);
  `LIMIT` only accepts literal numbers.
- neug has no `type()` function: filter by edge type by writing the table name directly (tier2 table names are the edge types themselves;
  tier1 uses `<type>_<From>_<To>`).
- Node properties are columns of their table: tier2 node primary key is `id` (INT64), tier1 is `node_id`
  (VARCHAR(512)) — do not mix the two notations.
- Mixed-database trap: `MATCH (n) RETURN max(n.id)` silently returns
  NULL because summary tables have no id column; continuation numbering must query per CPG label (`max_cpg_node_id()` already handles this).
- **`file_audit query` defaults to `--max-cell-len 120`, which silently truncates cells**: long
  `func:` node_ids and long code get cut, and feeding the truncated text back into build/query returns nothing. For programmatic use,
  always add `--max-cell-len 65535`.
- Read-only usage: the query subcommand only runs queries; any edge-creation/table-drop operation is subject to the cross-session corruption constraints
  (see the "Dialect" section of schema.md); make changes via the build/audit subcommands.

## Composition Strategies (field-tested patterns)

Audit locating: tier1 `overview`/name-based query to shortlist suspicious functions -> `build` their files ->
tier2 query to find source/sink node ids -> `paths --pair` to rebuild the evidence chain ->
`refines_Symbol_Method` back to tier1 to confirm you did not pick the wrong same-named function.

Dataflow tracing: `build` the target file -> `CONTAINS` to enumerate Identifiers/Locals inside the method ->
bidirectional `REACHING_DEF` lookups of definitions and uses -> `INVOKES`/`calls` to check whether data leaves the file
(if it leaves, that is a blind spot — record it honestly in the coverage summary).
