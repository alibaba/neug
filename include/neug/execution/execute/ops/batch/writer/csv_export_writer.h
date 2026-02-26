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

#include "neug/execution/execute/ops/batch/writer/export_writer_factory.h"

namespace neug {
namespace execution {
class CsvExportWriter : public IExportWriter {
 public:
  CsvExportWriter(
      const std::string& file_path,
      const std::vector<std::pair<int, std::string>>& header,
      const std::unordered_map<std::string, std::string>& write_config);

  ~CsvExportWriter() {}

  static std::shared_ptr<IExportWriter> Make(
      const std::string& file_path,
      const std::vector<std::pair<int, std::string>>& header,
      const std::unordered_map<std::string, std::string>& write_config);

  Status Write(const std::vector<std::shared_ptr<IContextColumn>>& columns_map,
               const StorageReadInterface& graph) override;

 private:
  void parse_csv_options(
      const std::unordered_map<std::string, std::string>& csv_options);

  std::string file_path_;
  std::vector<std::pair<int, std::string>> header_;

  bool write_header_;
  char delimeter_;
  static const bool registered_;
};

}  // namespace execution
}  // namespace neug
