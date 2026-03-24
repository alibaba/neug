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

#include <arrow/filesystem/filesystem.h>
#include <memory>
#include <string>
#include <vector>
#include "neug/utils/exception/exception.h"

namespace neug {
namespace extension {
namespace s3 {

/**
 * @brief Match a file path against a glob pattern
 *
 * Simple glob pattern matcher for filesystem paths.
 * Supports: * (matches any chars), ? (matches single char)
 * This is a lightweight implementation preferred for performance-critical
 * path filtering (e.g., S3 object listing).
 *
 * @param text The file path to test
 * @param pattern The glob pattern
 * @return true if the path matches the pattern
 */
inline bool MatchGlobPattern(const std::string& text, const std::string& pattern) {
  size_t text_idx = 0;
  size_t pattern_idx = 0;
  size_t text_backup = std::string::npos;
  size_t pattern_backup = std::string::npos;

  while (text_idx < text.length()) {
    if (pattern_idx < pattern.length()) {
      if (pattern[pattern_idx] == '*') {
        // Save position for backtracking
        pattern_backup = pattern_idx++;
        text_backup = text_idx;
        continue;
      } else if (pattern[pattern_idx] == '?' || pattern[pattern_idx] == text[text_idx]) {
        // Match single char or exact match
        pattern_idx++;
        text_idx++;
        continue;
      }
    }

    // Mismatch - try backtracking if we saw a '*'
    if (pattern_backup != std::string::npos) {
      pattern_idx = pattern_backup + 1;
      text_idx = ++text_backup;
      continue;
    }

    return false;
  }

  // Consume remaining '*' in pattern
  while (pattern_idx < pattern.length() && pattern[pattern_idx] == '*') {
    pattern_idx++;
  }

  return pattern_idx == pattern.length();
}

/**
 * @brief Resolve glob patterns on any Arrow FileSystem
 * 
 * This helper function works with any Arrow FileSystem (S3, local, etc.) by
 * providing a root prefix (e.g., bucket name for S3, empty for local)
 * and a pattern relative to that root. The helper lists files via Arrow's
 * FileSelector and filters them using MatchGlobPattern on the relative path.
 *
 * @param fs Arrow FileSystem instance (S3FileSystem, LocalFileSystem, etc.)
 * @param root Root prefix (e.g., "my-bucket" for S3, "" for local paths)
 * @param pattern Glob pattern relative to root (e.g., "data/test*.parquet")
 * @param out_paths Output vector to append matched paths (in "root/relative" format)
 * @param original_path_for_error Original user path for error messages
 */
inline void ResolvePathsWithGlobOnFs(
    const std::shared_ptr<arrow::fs::FileSystem>& fs,
    const std::string& root,
    const std::string& pattern,
    std::vector<std::string>& out_paths,
    const std::string& original_path_for_error) {
  // Extract base directory (part before first wildcard)
  std::string base_dir = pattern;
  size_t wildcard_pos = std::min({
      base_dir.find('*'),
      base_dir.find('?'),
      base_dir.find('[')});

  if (wildcard_pos != std::string::npos) {
    // Find last '/' before wildcard
    size_t last_slash = base_dir.rfind('/', wildcard_pos);
    if (last_slash != std::string::npos) {
      base_dir = base_dir.substr(0, last_slash);
    } else {
      base_dir = "";
    }
  }

  // Use Arrow FileSelector to list objects from root/base_dir
  arrow::fs::FileSelector selector;
  selector.base_dir = root + (base_dir.empty() ? "" : "/" + base_dir);
  selector.recursive = (pattern.find("**") != std::string::npos);

  auto file_infos_result = fs->GetFileInfo(selector);
  if (!file_infos_result.ok()) {
    THROW_IO_EXCEPTION("Failed to list objects: " +
                       file_infos_result.status().ToString());
  }

  auto file_infos = *file_infos_result;

  int match_count = 0;
  for (const auto& file_info : file_infos) {
    if (!file_info.IsFile()) {
      continue;
    }

    // Arrow returns paths in backend-specific format, but for S3 and
    // most filesystems we can treat them as "root/relative" when
    // root is a non-empty prefix.
    std::string file_path = file_info.path();
    std::string relative_path = file_path;

    if (!root.empty()) {
      std::string prefix = root + "/";
      if (file_path.rfind(prefix, 0) == 0) {
        relative_path = file_path.substr(prefix.size());
      }
    }

    if (MatchGlobPattern(relative_path, pattern)) {
      // Store normalized path as "root/relative" if root is present
      if (!root.empty()) {
        out_paths.push_back(root + "/" + relative_path);
      } else {
        out_paths.push_back(relative_path);
      }
      match_count++;
    }
  }

  if (match_count == 0) {
    THROW_IO_EXCEPTION("No files matched glob pattern: " +
                       original_path_for_error);
  }
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
