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

#include "neug/transaction/wal/wal.h"

#include <glog/logging.h>
#include <memory>
#include <regex>
#include <sstream>
#include <utility>

#include "neug/storages/file_names.h"
#include "neug/transaction/wal/dummy_wal_writer.h"

namespace gs {

std::string parse_wal_uri(std::string wal_uri, const std::string& work_dir) {
  if (wal_uri.empty()) {
    VLOG(1) << "wal_uri is not set, use default wal_uri";
    wal_uri = wal_dir(work_dir);
  } else if (wal_uri.find("{GRAPH_DATA_DIR}") != std::string::npos) {
    VLOG(1) << "Template {GRAPH_DATA_DIR} found in wal_uri, replace it with "
               "data_dir";
    wal_uri = std::regex_replace(wal_uri, std::regex("\\{GRAPH_DATA_DIR\\}"),
                                 work_dir);
  }
  return wal_uri;
}

std::string get_wal_uri_scheme(const std::string& uri) {
  std::string scheme;
  auto pos = uri.find("://");
  if (pos != std::string::npos) {
    scheme = uri.substr(0, pos);
  }
  if (scheme.empty()) {
    VLOG(20) << "No scheme found in wal uri: " << uri
             << ", using default scheme: file";
    scheme = "file";
  }
  return scheme;
}

std::string get_wal_uri_path(const std::string& uri) {
  std::string path;
  auto pos = uri.find("://");
  if (pos != std::string::npos) {
    path = uri.substr(pos + 3);
  } else {
    path = uri;
  }
  return path;
}

void WalWriterFactory::Init() {}

void WalWriterFactory::Finalize() {}

std::unique_ptr<IWalWriter> WalWriterFactory::CreateWalWriter(
    const std::string& wal_uri, int32_t thread_id) {
  auto& known_writers_ = getKnownWalWriters();
  auto scheme = get_wal_uri_scheme(wal_uri);
  auto iter = known_writers_.find(scheme);
  if (iter != known_writers_.end()) {
    return iter->second(wal_uri, thread_id);
  } else {
    std::stringstream ss;
    for (const auto& writer : known_writers_) {
      ss << "[" << writer.first << "] ";
    }
    LOG(FATAL) << "Unknown wal writer: " << scheme << " for uri: " << wal_uri
               << ", supported writers are: " << ss.str();
    return nullptr;  // to suppress warning
  }
}

std::unique_ptr<IWalWriter> WalWriterFactory::CreateDummyWalWriter() {
  return std::make_unique<DummyWalWriter>();
}

bool WalWriterFactory::RegisterWalWriter(
    const std::string& wal_writer_type,
    WalWriterFactory::wal_writer_initializer_t initializer) {
  auto& known_writers_ = getKnownWalWriters();
  known_writers_.emplace(wal_writer_type, initializer);
  return true;
}

std::unordered_map<std::string, WalWriterFactory::wal_writer_initializer_t>&
WalWriterFactory::getKnownWalWriters() {
  static std::unordered_map<
      std::string, WalWriterFactory::wal_writer_initializer_t>* known_writers_ =
      new std::unordered_map<std::string, wal_writer_initializer_t>();
  return *known_writers_;
}

////////////////////////// WalParserFactory //////////////////////////

void WalParserFactory::Init() {}

void WalParserFactory::Finalize() {}

std::unique_ptr<IWalParser> WalParserFactory::CreateWalParser(
    const std::string& wal_uri) {
  auto& know_parsers_ = getKnownWalParsers();
  auto scheme = get_wal_uri_scheme(wal_uri);
  auto iter = know_parsers_.find(scheme);
  if (iter != know_parsers_.end()) {
    return iter->second(wal_uri);
  } else {
    std::stringstream ss;
    for (const auto& parser : know_parsers_) {
      ss << "[" << parser.first << "] ";
    }
    LOG(FATAL) << "Unknown wal parser: " << scheme << " for uri: " << wal_uri
               << ", supported parsers are: " << ss.str();
    return nullptr;  // to suppress warning
  }
}

bool WalParserFactory::RegisterWalParser(
    const std::string& wal_writer_type,
    WalParserFactory::wal_parser_initializer_t initializer) {
  auto& known_parsers_ = getKnownWalParsers();
  known_parsers_.emplace(wal_writer_type, initializer);
  return true;
}

std::unordered_map<std::string, WalParserFactory::wal_parser_initializer_t>&
WalParserFactory::getKnownWalParsers() {
  static std::unordered_map<
      std::string, WalParserFactory::wal_parser_initializer_t>* known_parsers_ =
      new std::unordered_map<std::string, wal_parser_initializer_t>();
  return *known_parsers_;
}

}  // namespace gs
