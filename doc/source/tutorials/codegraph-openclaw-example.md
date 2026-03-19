# CodeGraph: Code Analysis with Knowledge Graph

## Introduction

CodeGraph is a code analysis Skill built on **NeuG** (graph database) and **zvec** (vector database). It indexes source code into a knowledge graph containing nodes (File, Function, Class, Module, Commit) and edges (CALLS, IMPORTS, INHERITS, MODIFIES, etc.), plus semantic embeddings for each function.

This combination enables analyses that grep, LSP, or pure vector search cannot accomplish alone.

### Core Capabilities


| Capability              | Description                                             |
| ----------------------- | ------------------------------------------------------- |
| Call Chain Analysis     | Find callers, callees, N-hop impact analysis            |
| Architecture Analysis   | Auto-discover layers, bridge functions, module coupling |
| Dead Code Detection     | Identify functions with zero callers                    |
| Semantic Search         | Find functions by natural language description          |
| Hotspot Analysis        | Identify high-risk functions (fan-in × fan-out)        |
| Evolution Analysis      | Track commit history, function modification records     |
| Bug Root Cause Analysis | Map GitHub issues to code locations                     |

## Example: Analyzing OpenClaw Codebase

This section demonstrates CodeGraph usage with the OpenClaw codebase as an example.

### Prerequisites

CodeGraph requires Python 3.10+ and PyTorch 2.4+.

```bash
# Create virtual environment
cd /path/to/your/project
python3 -m venv .venv

# Activate and install codegraph-ai
source .venv/bin/activate
pip install codegraph-ai
```

### Environment Setup

```bash
# Point to the database directory
export CODESCOPE_DB_DIR="/path/to/your/project/.codegraph"

# If you have offline mode for HuggingFace, you can use the offline mode
export HF_HUB_OFFLINE="1"
```

### Indexing the Codebase

```bash
# Create index (first time)
codegraph init --repo /path/to/your/project --lang auto --commits 100

# Check index status
codegraph status --db $CODESCOPE_DB_DIR
```

**Actual output from OpenClaw:**

```
============================================================
CodeScope Index Status: /path/to/openclaw/.codegraph
============================================================

Graph:
  File        :     12,857
  Function    :     24,173
  Class       :        255
  Module      :        380
  Commit      :        100

Edges:
  CALLS       :     41,269
  TOUCHES     :        605
  MODIFIES    :          0

Vectors: 24,173 function embeddings
============================================================
```

---

## CLI Usage Examples

### 1. Check Status

```bash
codegraph status --db $CODESCOPE_DB_DIR 2>/dev/null
```

### 2. Natural Language Query

```bash
codegraph query "Who calls runHeartbeatOnce?" --db $CODESCOPE_DB_DIR 2>/dev/null
```

**Actual output:**

```
Question type: structural
Retrieved 6 evidence items in 74ms:

[1] (caller) startGatewayServer (src/gateway/server.impl.ts) — hop=2
[2] (caller) createGatewayReloadHandlers (src/gateway/server-reload-handlers.ts) — hop=2
[3] (caller) executeJob (src/cron/service/timer.ts) — hop=2
[4] (caller) executeJobCore (src/cron/service/timer.ts) — hop=1
[5] (caller) buildGatewayCronService (src/gateway/server-cron.ts) — hop=1
[6] (caller) executeJobCoreWithTimeout (src/cron/service/timer.ts) — hop=2
```

### 3. Generate Architecture Report

```bash
codegraph analyze --db $CODESCOPE_DB_DIR --output architecture-report.md 2>/dev/null
```

**Report includes:**

- Codebase overview (files, functions, call edges, classes, modules)
- Subsystem distribution
- Architectural layers (with Mermaid diagrams)
- Bridge functions
- Hotspots
- Module coupling
- Dead code density

---

## Python API Examples

For complex queries, use the Python API:

### Setup

```python
import os
os.environ['HF_HUB_OFFLINE'] = '1'

from codegraph.core import CodeScope
cs = CodeScope(os.environ['CODESCOPE_DB_DIR'])
```

### Find Callers of a Function

```python
rows = list(cs.conn.execute('''
    MATCH (caller:Function)-[:CALLS]->(f:Function {name: "runHeartbeatOnce"})
    RETURN caller.name, caller.file_path
'''))
for r in rows:
    print(f"{r[0]} @ {r[1]}")
```

**Actual output:**

```
executeJobCore @ src/cron/service/timer.ts
buildGatewayCronService @ src/gateway/server-cron.ts
```

### Find Functions Called by a Function

```python
rows = list(cs.conn.execute('''
    MATCH (f:Function {name: "runHeartbeatOnce"})-[:CALLS]->(callee:Function)
    RETURN callee.name
    LIMIT 20
'''))
for r in rows:
    print(f"-> {r[0]}")
```

**Actual output:**

```
-> parseAgentSessionKey
-> resolveDefaultAgentId
-> resolveHeartbeatConfig
-> areHeartbeatsEnabled
-> isHeartbeatEnabledForAgent
-> resolveHeartbeatIntervalMs
-> nowMs
-> now
-> isWithinActiveHours
-> resolveHeartbeatPreflight
-> emitHeartbeatEvent
-> resolveCronSession
-> saveSessionStore
-> resolveHeartbeatDeliveryTarget
-> resolveHeartbeatVisibility
-> resolveHeartbeatSenderContext
-> resolveEffectiveMessagesConfig
-> resolveAgentWorkspaceDir
-> resolveHeartbeatRunPrompt
-> appendCronStyleCurrentTimeLine
```

### Impact Analysis (N-hop Callers)

```python
rows = list(cs.conn.execute('''
    MATCH (caller:Function)-[:CALLS*1..2]->(f:Function {name: "runHeartbeatOnce"})
    RETURN DISTINCT caller.name, caller.file_path
    LIMIT 20
'''))
for r in rows:
    print(f"{r[0]} @ {r[1]}")
```

**Actual output:**

```
executeJobCore @ src/cron/service/timer.ts
buildGatewayCronService @ src/gateway/server-cron.ts
executeJobCoreWithTimeout @ src/cron/service/timer.ts
executeJob @ src/cron/service/timer.ts
createGatewayReloadHandlers @ src/gateway/server-reload-handlers.ts
startGatewayServer @ src/gateway/server.impl.ts
```

---

## Built-in Analysis Methods

### Hotspots

High-risk functions ranked by fan-in × fan-out:

```python
for h in cs.hotspots(topk=10):
    print(f"{h.name} @ {h.file_path}")
    print(f"  fan_in={h.fan_in}, fan_out={h.fan_out}")
```

**Actual output:**

```
push @ ui/src/ui/chat/input-history.ts
  fan_in=1747, fan_out=0
createConfigIO @ src/config/io.ts
  fan_in=18, fan_out=57
fn @ extensions/diffs/assets/viewer-runtime.js
  fan_in=533, fan_out=1
runEmbeddedPiAgent @ src/agents/pi-embedded-runner/run.ts
  fan_in=14, fan_out=65
startGatewayServer @ src/gateway/server.impl.ts
  fan_in=10, fan_out=88
now @ src/auto-reply/reply/export-html/template.security.test.ts
  fan_in=857, fan_out=0
loadOpenClawPlugins @ src/plugins/loader.ts
  fan_in=21, fan_out=36
runCronIsolatedAgentTurn @ src/cron/isolated-agent/run.ts
  fan_in=11, fan_out=56
loadSessionStore @ src/config/sessions/store.ts
  fan_in=60, fan_out=8
getReplyFromConfig @ src/auto-reply/reply/get-reply.ts
  fan_in=20, fan_out=24
```

### Bridge Functions

Functions called from many distinct modules (cross-subsystem connectors):

```python
for b in cs.bridge_functions(topk=10):
    print(f"{b.name} @ {b.file_path}")
    print(f"  modules={b.module_count}")
```

**Actual output:**

```
push @ ui/src/ui/chat/input-history.ts
  modules=167
now @ src/auto-reply/reply/export-html/template.security.test.ts
  modules=135
fn @ extensions/diffs/assets/viewer-runtime.js
  modules=103
error @ src/plugins/config-schema.ts
  modules=102
toString @ extensions/discord/src/send.types.ts
  modules=95
next @ src/wizard/session.ts
  modules=36
shouldLogVerbose @ src/globals.ts
  modules=33
release @ src/browser/cdp-proxy-bypass.ts
  modules=32
formatCliCommand @ src/cli/command-format.ts
  modules=31
isDirectory @ src/infra/path-env.ts
  modules=28
```

### Dead Code Detection

Functions with zero callers:

```python
for d in cs.dead_code()[:10]:
    print(f"{d.name} @ {d.file_path}")
```

**Actual output:**

```
promptUrlWidgetExtension @ .pi/extensions/prompt-url-widget.ts
showPagedSelectList @ .pi/extensions/ui/paged-select.ts
copyToClipboard @ .venv/lib/python3.10/site-packages/sklearn/utils/_repr_html/estimator.js
CodeSection @ .venv/lib/python3.10/site-packages/torch/utils/model_dump/code.js
ExtraJsonSection @ .venv/lib/python3.10/site-packages/torch/utils/model_dump/code.js
...
```

> **Note**: Dead code detection may include external dependencies. Filter by `is_external = 0` for project-specific results.

### Semantic Search

Find functions by natural language description:

```python
results = cs.vector_only_search('heartbeat periodic wake agent schedule', topk=5)
for r in results:
    print(f"id={r['id'][:20]}... score={r['score']:.3f}")
```

**Actual output:**

```
id=59744ec14e23575012c1... score=0.514
id=0b27570192377b7077cd... score=0.481
id=11fad68a6ba0d7fa0228... score=0.478
id=b33f6f3241c0a61d7118... score=0.477
id=8221fa3eb46b7e06e561... score=0.473
```

---

## Cypher Query Templates

The following templates serve as reference queries for analyzing codebases. Replace `FUNC_NAME`, `PATH`, `MODULE` with your specific values:

| Analysis              | Cypher Query                                                                                                  |
| --------------------- | ------------------------------------------------------------------------------------------------------------- |
| Find callers          | `MATCH (c:Function)-[:CALLS]->(f:Function {name: "FUNC_NAME"}) RETURN c.name, c.file_path`                    |
| Find callees          | `MATCH (f:Function {name: "FUNC_NAME"})-[:CALLS]->(c:Function) RETURN c.name`                                 |
| Impact (N hops)       | `MATCH (c:Function)-[:CALLS*1..N]->(f:Function {name: "FUNC_NAME"}) RETURN DISTINCT c.name`                   |
| Functions in file     | `MATCH (file:File)-[:DEFINES_FUNC]->(f:Function) WHERE file.path CONTAINS "PATH" RETURN f.name`               |
| Files in module       | `MATCH (f:File)-[:BELONGS_TO]->(m:Module) WHERE m.name = "MODULE" RETURN f.path`                              |
| Class hierarchy       | `MATCH (c:Class)-[:INHERITS]->(p:Class) RETURN c.name, p.name`                                                |
| Most-called functions | `MATCH (f:Function)<-[:CALLS]-(c:Function) RETURN f.name, count(c) as callers ORDER BY callers DESC LIMIT 10` |
