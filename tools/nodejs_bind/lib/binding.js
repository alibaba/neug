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

// Native artifacts live in build/<Release|Debug>/<platform>/.
// Platform tags: linux_x86_64, linux_arm64, osx_x86_64, osx_arm64
// Prefer Debug if present (dev intent); otherwise Release (the only
// flavor shipped in npm tarballs).

const path = require('path');
const fs = require('fs');

const OS_MAP = { linux: 'linux', darwin: 'osx' };
const ARCH_MAP = { x64: 'x86_64', arm64: 'arm64' };

const os = OS_MAP[process.platform];
const arch = ARCH_MAP[process.arch];
if (!os || !arch) {
  throw new Error(`Unsupported platform: ${process.platform}-${process.arch}`);
}
const platform = `${os}_${arch}`;

const buildDir = path.join(__dirname, '..', 'build');
const cfg = fs.existsSync(path.join(buildDir, 'Debug', platform)) ? 'Debug' : 'Release';

module.exports = require(path.join(buildDir, cfg, platform, 'neug_node_bind.node'));
