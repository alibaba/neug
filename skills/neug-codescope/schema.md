# NeuG-codescope CPG Schema

Two graph tiers coexist in a single neug database (`init_audit_schema` creates everything in one pass, fully idempotent):
the tier1 summary graph (Repo/SourceFile/Symbol at function level) + the tier2 detailed CPG (Joern-aligned
44 node tables / 34 unified edge tables) + the cross-layer edge `refines_Symbol_Method`.
Single source of truth for the DDL: `cpg/schema.py`; the summary-layer column sets `NODE_COLS`/`REL_PROPS` are also defined there.

## tier1 Summary Layer (3 node tables + 3 edge tables)

| table | id prefix | description |
|---|---|---|
| Repo | `repo:<name>` | Repository root |
| SourceFile | `file:<relpath>` | Source file (with loc). **Not called File** — the CPG layer already occupies the File table name |
| Symbol | `func:<relpath>:<name>` | Function/method, symbol_kind='function'; `is_external=1` marks a library-function stub (e.g. memcpy); same-named nested methods carry an `@line` suffix |

**All nodes share 29 columns (NODE_COLS)**: node_id (VARCHAR(512) primary key), node_type, tier,
name, qualified_name, repo, module_id, parent_id, path, language, symbol_kind,
config_kind, module_kind, protocol_kind, is_build_unit, start_line, end_line, loc,
in_hot_subgraph, stability_score, is_test, is_example, is_tooling, is_generated,
is_deprecated, is_external, role_tag, last_modified, last_commit.

**Edge tables are physically partitioned by pair**, named `<edge_type>_<From>_<To>`, and share 14 edge property columns (REL_PROPS:
edge_id, edge_type, subtype, tier, evidence_kind, confidence, is_conditional,
is_cross_repo, weight, resolver, evidence, lines, from_repo, to_repo):

| table | meaning |
|---|---|
| contains_Repo_SourceFile | Repo contains file |
| declares_SourceFile_Symbol | File declares function |
| calls_Symbol_Symbol | Inter-function calls (resolved by name, including internal -> external stub) |
| refines_Symbol_Method | **Cross-layer edge**: summary Symbol -> detailed CPG Method |

## tier2 CPG Layer (44 node tables + 34 unified edge tables)

Node table names are the Joern labels; the primary key is always `id INT64` (incremental builds
continue numbering per label from the watermark via `max_cpg_node_id()` — summary tables have no id column, so a global max silently returns None).

- **Structure**: MetaData, File, Namespace, NamespaceBlock, TypeDecl, Type,
  TypeParameter, TypeArgument, Member, Binding, Dependency, Import, Comment,
  ConfigFile, KeyValuePair, Location, Tag, TagNodePair, Finding, ClosureBinding
- **Methods**: Method (name/full_name/signature/filename/code/line_number...),
  MethodParameterIn/Out, MethodReturn, Modifier
- **Expressions** (CFG nodes): Call, Identifier, Literal, Local, Block, Return,
  ControlStructure, FieldIdentifier, MethodRef, TypeRef, JumpTarget, JumpLabel,
  Unknown, Annotation, AnnotationLiteral, AnnotationParameter,
  AnnotationParameterAssign, ArrayInitializer, TemplateDom

**Edge tables = the edge types themselves** (unified tables, each covering multiple `FROM x TO y` pairs, 34 in total; queries write
`[:EDGE_TYPE]` directly — **not** tier1's `<type>_<From>_<To>` partition naming):

| group | edge types |
|---|---|
| Syntax tree | AST, ARGUMENT, RECEIVER, CONDITION, JUMP_ARGUMENT |
| Control-structure bodies | TRUE_BODY, FALSE_BODY, DO_BODY, TRY_BODY, CATCH_BODY, FINALLY_BODY, FOR_INIT, FOR_UPDATE, FOR_BODY |
| Containment/structural shortcuts | CONTAINS, SOURCE_FILE, PARAMETER_LINK |
| Control flow | CFG, DOMINATE (dominator tree), POST_DOMINATE, CDG (control dependence) |
| Types | EVAL_TYPE, INHERITS_FROM, ALIAS_OF, BINDS, BINDS_TO |
| References/closures | REF, CAPTURE, CAPTURED_BY |
| Call graph | **INVOKES** (Call/MethodRef -> Method) |
| Dataflow | REACHING_DEF (the only table with an edge property, `variable STRING`) |
| Other | TAGGED_BY, IMPORTS, IS_CALL_FOR_IMPORT |

**There is no CALL edge table**: neug is case-insensitive, so CALL would clash with the Call node table; call edges always use
INVOKES (see the comment in schema.py).

The five overlay layers produce the edges above in dependency order: Base (File/Namespace/CONTAINS) -> ControlFlow
(CFG + DOMINATE/POST_DOMINATE + CDG) -> TypeRelations (EVAL_TYPE etc.) ->
CallGraph (INVOKES, static/dynamic dispatch + three MethodRef linkers) -> OssDataFlow
(REACHING_DEF, reaching-definition Gen/Kill). During incremental builds, each layer only computes over new
nodes with `id > watermark` (`PassScheduler(conn, db_dir, min_node_id).apply_layers()`).

## neug Cypher Dialect (verified against 0.1.3)

Supported: MATCH/WHERE/RETURN/ORDER BY/LIMIT/WITH, count()/count(DISTINCT),
variable-length paths `*1..N`, STARTS WITH/CONTAINS/ENDS WITH, `{prop: $param}` bound parameters.
neug does not support the `type()` function — to filter by edge type, write the physical table name directly (in the tier2 layer the table name is the edge type itself).

Limitations (each hit in practice; see the header comment of cpg/query_lib.py):

- The right operand of CONTAINS only accepts literals (bound parameters raise ERR_COMPILATION) -> inline and escape
- `IN $list` with a bound array parameter segfaults outright -> use a literal list
- LIMIT/SKIP only accept literals; no INSERT INTO, no UNWIND
- In DDL, STRING silently maps to VARCHAR(256); long-text columns must be declared as explicit VARCHAR(n),
  n<65536 (choosing exactly 65536 silently clears values)
- **The VARCHAR limit only applies to the write path within "the DB-open session that created the table"**: writes after reopening the database
  are silently truncated at 256B -> create the database / backfill data in a single session whenever possible
- **Cross-session Cypher CREATE of edges corrupts the next CHECKPOINT** (when the endpoints were previously
  checkpointed); cross-session edge backfill must go through CSV COPY; DROP/DETACH DELETE on an already
  checkpointed table corrupts it too -> the only way to delete data is to rebuild the database
- COPY interprets backslash escapes inside quoted fields (\n \t \\) -> long text containing literal backslashes
  must be written via parameterized CREATE
