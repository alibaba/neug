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

#ifndef INCLUDE_NEUG_EXECUTION_EXECUTE_WRITER_EXPORT_WRITER_FACTORY_H_
#define INCLUDE_NEUG_EXECUTION_EXECUTE_WRITER_EXPORT_WRITER_FACTORY_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {

class IExportWriter {
 public:
  virtual ~IExportWriter() = default;
  virtual Status Write(
      const std::vector<std::shared_ptr<IContextColumn>>& columns_map,
      const gs::runtime::GraphReadInterface& graph) = 0;
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
}  // namespace runtime
}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_EXECUTE_WRITER_EXPORT_WRITER_FACTORY_H_
