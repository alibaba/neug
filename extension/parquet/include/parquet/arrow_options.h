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

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/scanner.h>
#include <memory>

#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/schema.h"

namespace neug {
namespace reader {

std::shared_ptr<arrow::Schema> createSchema(const EntrySchema& entrySchema);

struct ArrowOptions {
  std::shared_ptr<arrow::dataset::ScanOptions> scanOptions;
  std::shared_ptr<arrow::dataset::FileFormat> fileFormat;
};

class ArrowOptionsBuilder : public OptionsBuilder<ArrowOptions> {
 public:
  explicit ArrowOptionsBuilder(std::shared_ptr<ReadSharedState> state)
      : OptionsBuilder<ArrowOptions>(state) {}
  ~ArrowOptionsBuilder() override = default;

  ArrowOptions build() const override = 0;
  bool projectColumns(ArrowOptions& options) override;
  bool skipRows(ArrowOptions& options) override;
};

}  // namespace reader
}  // namespace neug
