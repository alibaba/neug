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
 * Convert a mode string to a human-readable string.
 * @param {string} mode - The mode string.
 * @returns {string} The readable mode string.
 */
function readable(mode) {
  if (['r', 'read', 'read-only', 'read_only'].includes(mode)) {
    return 'read-only';
  } else if (['w', 'rw', 'write', 'readwrite', 'read-write', 'read_write'].includes(mode)) {
    return 'read-write';
  }
  throw new Error(
    `Invalid mode: ${mode}. Must be one of: r, read, w, rw, write, readwrite.`
  );
}

/** Valid access modes for query execution. */
const validAccessModes = ['read', 'r', 'insert', 'i', 'update', 'u', 'schema', 's'];

/**
 * Check if the given access mode string is valid.
 * @param {string} mode - The access mode to check.
 * @returns {boolean} True if valid.
 */
function isAccessModeValid(mode) {
  return validAccessModes.includes(mode);
}

module.exports = { readable, validAccessModes, isAccessModeValid };
