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

#pragma once

#include <string>

namespace neug {

void ensure_directory_exists(const std::string& dir_path);

bool read_string_from_file(const std::string& file_path, std::string& content);

bool write_string_to_file(const std::string& content,
                          const std::string& file_path);

/**
 * Copy file from src to dst.
 * @param src source file path
 * @param dst destination file path
 * @param overwrite whether to clean up the dst file if it already exists
 * @param recursive whether to copy directories recursively
 */
void copy_directory(const std::string& src, const std::string& dst,
                    bool overwrite = false, bool recursive = true);

void remove_directory(const std::string& dir_path);

void write_file(const std::string& filename, const void* buffer, size_t size,
                size_t num);

void read_file(const std::string& filename, void* buffer, size_t size,
               size_t num);

void write_statistic_file(const std::string& file_path, size_t capacity,
                          size_t size);

void read_statistic_file(const std::string& file_path, size_t& capacity,
                         size_t& size);

}  // namespace neug
