# CodeScope Graph Schema

## Nodes

| Node | Key Properties | Notes |
|------|---------------|-------|
| `File` | id, path, language, loc, is_external | `is_external=1` for system headers / library stubs |
| `Function` | id, name, qualified_name, signature, file_path, start_line, end_line, doc_comment, class_name, is_historical | `is_historical=1` for deleted/renamed functions |
| `Class` | id, name, qualified_name, file_path | |
| `Module` | id, name, path_prefix | Auto-discovered from directories (e.g. `kernel/sched`) |
| `Commit` | id, hash, message, author, timestamp, version_tag | `version_tag='bf'` means MODIFIES edges computed |
| `Metadata` | id, value | Pipeline state (e.g. `oldest_commit`) |

## Edges

| Edge | From → To | Meaning |
|------|-----------|---------|
| `CALLS` | Function → Function | Static call graph (resolved from AST) |
| `DEFINES_FUNC` | File → Function | File defines this function |
| `DEFINES_CLASS` | File → Class | File defines this class |
| `HAS_METHOD` | Class → Function | Class contains this method |
| `IMPORTS` | File → File | Include / import dependency |
| `BELONGS_TO` | File → Module | File belongs to this module |
| `INHERITS` | Class → Class | Class inheritance |
| `MODIFIES` | Commit → Function | Commit changed this function (requires backfill) |
| `TOUCHES` | Commit → File | Commit changed this file (always present) |

## Backfill State

Not all commits have MODIFIES edges — only those with `version_tag = 'bf'`. TOUCHES edges are always present for all ingested commits.

```cypher
MATCH (c:Commit) WHERE c.version_tag = 'bf' RETURN count(c) AS backfilled
```

```cypher
MATCH (c:Commit) RETURN count(c) AS total_commits
```

## Neug Cypher Reference

**Supported syntax:**
- `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `LIMIT`, `WITH`
- Aggregations: `count()`, `count(DISTINCT x)`
- Inline property filters: `{name: 'foo'}`
- Variable-length paths: `[*1..3]`
- String predicates: `STARTS WITH`, `CONTAINS`, `ENDS WITH`
- Comparisons: `=`, `<>`, `<`, `>`, `<=`, `>=`
- Boolean: `AND`, `OR`, `NOT`

**Limitations:**
- `OPTIONAL MATCH` is not available in the pip package
- Chained `MATCH` after `WITH` may be limited — prefer single `MATCH` clauses with multiple patterns separated by commas
- No `CREATE`, `SET`, `DELETE` via Cypher — graph mutations go through the Python API
