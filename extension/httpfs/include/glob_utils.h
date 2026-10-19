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

#ifdef _WIN32
#include <algorithm>
#include <string>
#else
#include <fnmatch.h>
#include <algorithm>
#include <string>
#endif

namespace neug {
namespace extension {
namespace s3 {

#ifdef _WIN32
/**
 * @brief Inline reimplementation of POSIX fnmatch(pattern, text, 0) for MSVC,
 *        which has no <fnmatch.h>.
 *
 * Semantics mirror fnmatch(3) with flags=0: '*' matches any sequence
 * (including '/'), '?' matches any single character (including '/'),
 * and "[...]" classes support ranges ("a-z") and '!' negation. An
 * unterminated '[' matches nothing (BSD/musl behaviour; glibc treats
 * it as a literal -- an implementation-defined corner that malformed
 * S3 patterns should never rely on).
 */
inline bool Fnmatch(const char* pattern, const char* text) {
  while (*pattern != '\0') {
    switch (*pattern) {
    case '*': {
      // Collapse consecutive '*' and try every suffix of text.
      while (*pattern == '*') {
        ++pattern;
      }
      if (*pattern == '\0') {
        return true;
      }
      for (const char* t = text;; ++t) {
        if (Fnmatch(pattern, t)) {
          return true;
        }
        if (*t == '\0') {
          return false;
        }
      }
    }
    case '?': {
      if (*text == '\0') {
        return false;
      }
      ++pattern;
      ++text;
      break;
    }
    case '[': {
      if (*text == '\0') {
        return false;
      }
      const char* start = pattern + 1;
      bool negated = (*start == '!');
      if (negated) {
        ++start;
      }
      bool matched = false;
      bool first = true;
      const char* p = start;
      while (*p != '\0' && (*p != ']' || first)) {
        char lo = *p;
        char hi = lo;
        if (p[1] == '-' && p[2] != ']' && p[2] != '\0') {
          hi = p[2];
          p += 3;
        } else {
          ++p;
        }
        if (*text >= lo && *text <= hi) {
          matched = true;
        }
        first = false;
      }
      if (*p == '\0') {
        // Unterminated '[' matches nothing (see doc comment above).
        return false;
      }
      if (matched == negated) {
        return false;
      }
      pattern = p + 1;  // past the ']'
      ++text;
      break;
    }
    default: {
      if (*text != *pattern) {
        return false;
      }
      ++pattern;
      ++text;
      break;
    }
    }
  }
  return *text == '\0';
}
#endif  // _WIN32

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
#ifdef _WIN32
  return Fnmatch(pattern.c_str(), text.c_str());
#else
  return fnmatch(pattern.c_str(), text.c_str(), 0) == 0;
#endif
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
