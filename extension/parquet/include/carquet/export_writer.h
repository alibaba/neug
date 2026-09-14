/** Copyright 2020 Alibaba Group Holding Limited.
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

#include <functional>
#include <memory>
#include <utility>

#include "neug/utils/io/write/writer.h"

namespace neug::parquet {

/**
 * Carquet implementation of NeuG's existing query export contract.
 *
 * The writer receives the QueryResponse produced by QueryExportWriter and
 * writes only the Parquet representation. The injected opener keeps stream
 * selection in the shared export layer and is invoked only after the response,
 * options, and schema have been validated.
 */
class CarquetExportWriter final : public writer::QueryExportWriter {
 public:
  explicit CarquetExportWriter(
      const reader::FileSchema& schema,
      std::shared_ptr<reader::EntrySchema> entrySchema = nullptr)
      : QueryExportWriter(schema, std::move(entrySchema)) {}

  void setStreamOpener(
      std::function<std::unique_ptr<io::OutputStream>()> opener) {
    streamOpener_ = std::move(opener);
  }

  Status writeTable(const QueryResponse* table) override;

 private:
  std::function<std::unique_ptr<io::OutputStream>()> streamOpener_;
};

}  // namespace neug::parquet
