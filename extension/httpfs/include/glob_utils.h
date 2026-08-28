/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#pragma once

#include <fnmatch.h>
#include <algorithm>
#include <string>

namespace neug {
namespace extension {
namespace s3 {

/**
 * @brief Match a file path against a glob pattern
 *
 * Uses POSIX fnmatch() for robust glob pattern matching.
 * Supports: * (matches any chars including /), ? (matches single char),
 *           [abc] (character classes), [!abc] (negated character classes)
 * Does NOT support: ** (recursive directory matching), {a,b} (alternatives)
 *
 * @param text The file path to test
 * @param pattern The glob pattern
 * @return true if the path matches the pattern
 */
inline bool MatchGlobPattern(const std::string& text,
                             const std::string& pattern) {
  // flags=0: '*' matches any character including '/', '?' matches any single
  // char
  return fnmatch(pattern.c_str(), text.c_str(), 0) == 0;
}

/**
 * @brief Extract the longest wildcard-free prefix of a glob pattern.
 *
 * Used as the `prefix` argument of S3 ListObjectsV2 so the server only
 * scans the relevant part of the bucket.
 * e.g. "data/2026/*.parquet" -> "data/2026/"
 *      "data/*.parquet"      -> "data/"
 *      "*.parquet"           -> ""
 */
inline std::string LongestGlobPrefix(const std::string& pattern) {
  size_t wildcard_pos =
      std::min({pattern.find('*'), pattern.find('?'), pattern.find('[')});
  if (wildcard_pos == std::string::npos) {
    return pattern;
  }
  size_t last_slash = pattern.rfind('/', wildcard_pos);
  if (last_slash == std::string::npos) {
    return "";
  }
  // Include the trailing '/' so the prefix is a directory boundary.
  return pattern.substr(0, last_slash + 1);
}

/**
 * @brief Whether the path contains any glob wildcard.
 */
inline bool HasGlobWildcard(const std::string& path) {
  return path.find('*') != std::string::npos ||
         path.find('?') != std::string::npos ||
         path.find('[') != std::string::npos;
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
