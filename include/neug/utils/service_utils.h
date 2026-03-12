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

#include <rapidjson/encodings.h>
#include <stdint.h>

// Disable class-memaccess warning to facilitate compilation with gcc>7
// https://github.com/Tencent/rapidjson/issues/1700
#pragma GCC diagnostic push
#if defined(__GNUC__) && __GNUC__ >= 8
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
#include <rapidjson/rapidjson.h>

#pragma GCC diagnostic pop

#include <signal.h>

#include <glog/logging.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>
#include <utility>

#include "neug/utils/yaml_utils.h"

// Disable class-memaccess warning to facilitate compilation with gcc>7
// https://github.com/Tencent/rapidjson/issues/1700
#pragma GCC diagnostic push
#if defined(__GNUC__) && __GNUC__ >= 8
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
#include <rapidjson/document.h>

#pragma GCC diagnostic pop
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"

namespace neug {

/// Util functions.

inline void blockSignal(int sig) {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, sig);
  if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0) {
    perror("pthread_sigmask");
  }
}

inline int64_t GetCurrentTimeStamp() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline std::string rapidjson_stringify(const rapidjson::Value& value,
                                       int indent = -1) {
  rapidjson::StringBuffer buffer;
  if (indent == -1) {
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return buffer.GetString();
  } else {
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    writer.SetIndent(' ', indent);
    value.Accept(writer);
    return buffer.GetString();
  }
}

inline std::string toUpper(const std::string str) {
  std::string upper_str = str;
  std::transform(upper_str.begin(), upper_str.end(), upper_str.begin(),
                 ::toupper);
  return upper_str;
}

inline std::string jsonToString(const rapidjson::Value& json) {
  if (json.IsString()) {
    return json.GetString();
  } else {
    return rapidjson_stringify(json);
  }
}

// Get the directory of the current executable
std::string get_current_dir();

std::pair<uint64_t, uint64_t> get_total_physical_memory_usage();

std::string memory_to_mb_str(uint64_t mem_bytes);

size_t human_readable_to_bytes(const std::string& human_readable);

}  // namespace neug
