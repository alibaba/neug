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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/utils/result.h"

namespace neug {
class StorageReadInterface;
namespace execution {
class IContextColumn;

class IExportWriter {
 public:
  virtual ~IExportWriter() = default;
  virtual Status Write(
      const std::vector<std::shared_ptr<IContextColumn>>& columns_map,
      const StorageReadInterface& graph) = 0;
};

class ExportWriterFactory {
 public:
  using writer_initializer_t = std::shared_ptr<IExportWriter> (*)(
      const std::string& file_path,
      const std::vector<std::pair<int, std::string>>& header,
      const std::unordered_map<std::string, std::string>& write_config);

  static std::shared_ptr<IExportWriter> CreateExportWriter(
      const std::string& name, const std::string& file_path,
      const std::vector<std::pair<int, std::string>>& header,
      const std::unordered_map<std::string, std::string>& write_config);

  static bool Register(const std::string& name,
                       writer_initializer_t initializer);

 private:
  static std::unordered_map<std::string, writer_initializer_t>&
  getKnownWriters();
};
}  // namespace execution
}  // namespace neug
