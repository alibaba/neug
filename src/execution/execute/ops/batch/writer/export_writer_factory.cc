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

#include <unordered_map>

#include "neug/execution/execute/ops/batch/writer/export_writer_factory.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {

std::shared_ptr<IExportWriter> ExportWriterFactory::CreateExportWriter(
    const std::string& name, const std::string& file_path,
    const std::vector<std::pair<int, std::string>>& header,
    const std::unordered_map<std::string, std::string>& write_config) {
  auto& known_writers_ = getKnownWriters();
  auto iter = known_writers_.find(name);
  if (iter != known_writers_.end()) {
    return iter->second(file_path, header, write_config);
  } else {
    LOG(FATAL) << "Unsupported format: " << name;
  }
  return nullptr;
}

bool ExportWriterFactory::Register(
    const std::string& name,
    ExportWriterFactory::writer_initializer_t initializer) {
  auto& known_writers_ = getKnownWriters();
  known_writers_.emplace(name, initializer);
  return true;
}

std::unordered_map<std::string, ExportWriterFactory::writer_initializer_t>&
ExportWriterFactory::getKnownWriters() {
  static std::unordered_map<
      std::string, ExportWriterFactory::writer_initializer_t>* known_writers_ =
      new std::unordered_map<std::string,
                             ExportWriterFactory::writer_initializer_t>();
  return *known_writers_;
}
}  // namespace execution
}  // namespace neug
