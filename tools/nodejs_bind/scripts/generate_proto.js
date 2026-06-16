#!/usr/bin/env node
/**
 * Generate lib/proto/error_pb.js from proto/error.proto.
 *
 * This script parses the `enum Code { ... }` block from the .proto source
 * and emits a standalone JS module — no protobuf compiler or npm packages
 * required (only Node.js built-in `fs` and `path`).
 *
 * Usage:
 *   node scripts/generate_proto.js            # from tools/nodejs_bind/
 *   node tools/nodejs_bind/scripts/generate_proto.js   # from repo root
 *
 * Or simply:
 *   make proto
 */

'use strict';

const fs = require('fs');
const path = require('path');

// Resolve paths relative to this script's location (tools/nodejs_bind/scripts/)
const BIND_DIR = path.resolve(__dirname, '..');
const REPO_ROOT = path.resolve(BIND_DIR, '..', '..');
const PROTO_FILE = path.join(REPO_ROOT, 'proto', 'error.proto');
const OUTPUT_FILE = path.join(BIND_DIR, 'lib', 'proto', 'error_pb.js');

// ── 1. Read and parse proto/error.proto ────────────────────────────────────

const src = fs.readFileSync(PROTO_FILE, 'utf8');

// Extract the first `enum <Name> { ... }` block.
const enumRe = /enum\s+(\w+)\s*\{([^}]*)\}/s;
const match = enumRe.exec(src);
if (!match) {
  console.error('ERROR: No enum block found in', PROTO_FILE);
  process.exit(1);
}

const enumName = match[1]; // "Code"
const body = match[2];

// Parse entries: "  ERR_FOO = 1234;  // optional comment"
const entryRe = /^\s*(\w+)\s*=\s*(\d+)\s*;/gm;
const entries = [];
let m;
while ((m = entryRe.exec(body)) !== null) {
  entries.push({ name: m[1], value: parseInt(m[2], 10) });
}

if (entries.length === 0) {
  console.error('ERROR: No enum entries parsed from', PROTO_FILE);
  process.exit(1);
}

console.log(`Parsed ${entries.length} entries from enum ${enumName} in ${PROTO_FILE}`);

// ── 2. Generate lib/proto/error_pb.js ──────────────────────────────────────

// Compute the max name length for aligned output
const maxLen = Math.max(...entries.map(e => e.name.length));

const lines = [
  '/**',
  ` * Generated from \`${path.relative(REPO_ROOT, PROTO_FILE)}\` — DO NOT EDIT MANUALLY.`,
  ' *',
  ' * This file is checked into the repository so end users never need to run',
  ' * protoc or install any extra tooling.',
  ' *',
  ' * ── Regeneration (developer only) ────────────────────────────────────────',
  ' *',
  ' *   make proto          # from tools/nodejs_bind/',
  ' *   # or equivalently:',
  ' *   node scripts/generate_proto.js',
  ' *',
  ' * ──────────────────────────────────────────────────────────────────────────',
  ' */',
  '',
  "'use strict';",
  '',
  `// ${enumName} enum — parsed from ${path.relative(REPO_ROOT, PROTO_FILE)}.`,
  '// Forward lookup:  Code.ERR_INVALID_ARGUMENT === 1009',
  '// Reverse lookup:  Code[1009] === "ERR_INVALID_ARGUMENT"  (via prototype chain)',
  `const ${enumName} = Object.create(`,
  '  // reverse map: number → name (on prototype)',
  '  Object.fromEntries([',
];

for (const { name, value } of entries) {
  lines.push(`    [${String(value).padStart(4)}, '${name}'],`);
}

lines.push('  ])');
lines.push(');');
lines.push('');
lines.push('// Forward map: name → number (own properties)');

for (const { name, value } of entries) {
  lines.push(`${enumName}.${name.padEnd(maxLen)} = ${value};`);
}

lines.push('');
lines.push(`module.exports = { ${enumName} };`);
lines.push('');

const output = lines.join('\n');

// Ensure output directory exists
fs.mkdirSync(path.dirname(OUTPUT_FILE), { recursive: true });
fs.writeFileSync(OUTPUT_FILE, output, 'utf8');

console.log(`Generated: ${path.relative(REPO_ROOT, OUTPUT_FILE)} (${entries.length} codes)`);
