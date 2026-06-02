/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

/**
 * Native addon loader for NeuG Node.js bindings.
 *
 * Looks for the compiled .node file in:
 *   1. build/Release/neug_node_bind.node           (standard build)
 *   2. build/Debug/neug_node_bind.node              (debug build)
 *   3. prebuilds/<platform>/neug_node_bind.node     (npm distributed package)
 *
 * When installed from npm, libneug.so (or libneug.dylib on macOS) and
 * libmimalloc are bundled alongside the .node file in the prebuilds
 * directory. The .node binary is built with RPATH ($ORIGIN on Linux,
 * @loader_path on macOS) so the dynamic linker resolves them automatically.
 */

const path = require('path');
const fs = require('fs');

const MODULE_NAME = 'neug_node_bind.node';
const ROOT = path.join(__dirname, '..');

/**
 * Map Node.js process.platform + process.arch to the prebuilds directory name.
 */
function getPrebuildPlatform() {
  const { platform, arch } = process;
  if (platform === 'linux' && arch === 'x64') return 'linux-x64';
  if (platform === 'linux' && arch === 'arm64') return 'linux-arm64';
  if (platform === 'darwin' && arch === 'x64') return 'darwin-x64';
  if (platform === 'darwin' && arch === 'arm64') return 'darwin-arm64';
  return null;
}

const candidates = [
  path.join(ROOT, 'build', 'Release', MODULE_NAME),
  path.join(ROOT, 'build', 'Debug', MODULE_NAME),
];

const prebuildPlatform = getPrebuildPlatform();
if (prebuildPlatform) {
  candidates.push(path.join(ROOT, 'prebuilds', prebuildPlatform, MODULE_NAME));
}

let nativeBinding;
for (const candidate of candidates) {
  if (fs.existsSync(candidate)) {
    nativeBinding = require(candidate);
    break;
  }
}

if (!nativeBinding) {
  throw new Error(
    `NeuG native binding not found. Run \`make build\` first. ` +
    `Searched: ${candidates.join(', ')}`
  );
}

module.exports = nativeBinding;
