/**
 * Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const fs = require('fs');
const path = require('path');

/**
 * Read the version from NEUG_VERSION file or package.json.
 * @returns {string} The version string.
 */
function loadVersion() {
  // Try reading from NEUG_VERSION file at repo root
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const versionFile = path.join(repoRoot, 'NEUG_VERSION');
  try {
    if (fs.existsSync(versionFile)) {
      return fs.readFileSync(versionFile, 'utf-8').trim();
    }
  } catch {
    // ignore
  }

  // Try reading from package.json
  try {
    const pkgPath = path.join(__dirname, '..', 'package.json');
    if (fs.existsSync(pkgPath)) {
      const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf-8'));
      if (pkg.version) return pkg.version;
    }
  } catch {
    // ignore
  }

  return '0.0.0';
}

const version = loadVersion();

module.exports = { version };
