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
namespace detail {

// Match a single character `c` against the bracket expression that begins at
// `*pattern` (which must point at '['). On return `*pattern` is advanced just
// past the closing ']'. Supports ranges ("a-z"), leading '!' negation, and
// ']'/'-' as literals when they appear first in the class. An unterminated '['
// consumes the remainder of the pattern and matches nothing. Comparisons use
// unsigned char so bytes >= 0x80 (non-ASCII UTF-8 S3 keys) order the way
// glibc's fnmatch does, instead of going negative under MSVC's signed char.
inline bool MatchBracketClass(const char*& pattern, unsigned char c) {
  const char* p = pattern + 1;  // step over '['
  bool negated = (*p == '!');
  if (negated) {
    ++p;
  }
  bool matched = false;
  bool first = true;
  while (*p != '\0' && (*p != ']' || first)) {
    first = false;
    unsigned char lo = static_cast<unsigned char>(*p);
    unsigned char hi = lo;
    if (p[1] == '-' && p[2] != ']' && p[2] != '\0') {
      hi = static_cast<unsigned char>(p[2]);
      p += 3;
    } else {
      ++p;
    }
    if (c >= lo && c <= hi) {
      matched = true;
    }
  }
  if (*p == '\0') {
    pattern = p;  // unterminated '[': consumed everything, matches nothing
    return false;
  }
  pattern = p + 1;  // past the closing ']'
  return matched != negated;
}

}  // namespace detail

/**
 * @brief Inline reimplementation of POSIX fnmatch(pattern, text, 0) for MSVC,
 *        which has no <fnmatch.h>.
 *
 * Semantics mirror fnmatch(3) with flags=0: '*' matches any sequence
 * (including '/'), '?' matches any single character (including '/'),
 * "[...]" classes support ranges ("a-z") and '!' negation, and a backslash
 * escapes the following character (so "\*" matches a literal '*'). An
 * unterminated '[' matches nothing (BSD/musl behaviour; glibc treats it as a
 * literal -- an implementation-defined corner that malformed S3 patterns
 * should never rely on).
 *
 * Matching is the classic iterative greedy scan with a single backtrack point
 * at the most recent '*': worst case O(len(pattern) * len(text)), no
 * recursion. A naive "retry every suffix at each '*'" recursion is
 * exponential on multi-wildcard patterns that fail to match (e.g. sixteen
 * "*a" segments plus "b" against sixteen 'a's needs 2^17-1 calls), which
 * would let one long non-matching S3 glob stall a COPY/LOAD query.
 */
inline bool Fnmatch(const char* pattern, const char* text) {
  const char* p = pattern;
  const char* t = text;
  const char* star_p = nullptr;  // pattern just past the most recent '*'
  const char* star_t = nullptr;  // text to resume from when backtracking

  while (*t != '\0') {
    if (*p == '*') {
      while (*p == '*') {
        ++p;
      }
      star_p = p;
      star_t = t;
      continue;  // first try '*' matching zero characters
    }

    bool matched = false;
    if (*p == '\0') {
      matched = false;  // pattern exhausted before text
    } else if (*p == '?') {
      ++p;
      ++t;
      matched = true;
    } else if (*p == '[') {
      matched = detail::MatchBracketClass(p, static_cast<unsigned char>(*t));
      if (matched) {
        ++t;
      }
    } else if (*p == '\\' && p[1] != '\0') {
      // Backslash escapes the next character (fnmatch without FNM_NOESCAPE).
      matched =
          static_cast<unsigned char>(p[1]) == static_cast<unsigned char>(*t);
      p += 2;
      if (matched) {
        ++t;
      }
    } else {
      matched =
          static_cast<unsigned char>(*p) == static_cast<unsigned char>(*t);
      if (matched) {
        ++p;
        ++t;
      }
    }

    if (matched) {
      continue;
    }
    if (star_p != nullptr) {
      // Backtrack: let the most recent '*' absorb one more character.
      p = star_p;
      ++star_t;
      t = star_t;
      continue;
    }
    return false;
  }

  // Text exhausted; any trailing '*'s still match the empty remainder.
  while (*p == '*') {
    ++p;
  }
  return *p == '\0';
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
