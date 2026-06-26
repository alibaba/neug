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

#include "parquet/arrow_sniffer.h"

#include <arrow/type.h>

#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/schema.h"
#include "parquet/arrow_type_converter.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {

result<std::shared_ptr<EntrySchema>> ArrowSniffer::sniff() {
  if (!reader_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "ArrowReader is null");
  }

  auto arrowSchema = reader_->inferSchema();
  if (!arrowSchema.ok()) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_IO_ERROR,
                        "Failed to infer schema from ArrowReader: " +
                            arrowSchema.status().ToString());
  }

  return convertArrowSchemaToEntrySchema(arrowSchema.ValueOrDie());
}

result<std::shared_ptr<EntrySchema>>
ArrowSniffer::convertArrowSchemaToEntrySchema(
    const std::shared_ptr<arrow::Schema>& arrowSchema) {
  if (!arrowSchema) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Arrow schema is null");
  }

  auto entrySchema = std::make_shared<TableEntrySchema>();
  ArrowTypeConverter converter;
  int numFields = arrowSchema->num_fields();

  entrySchema->columnNames.reserve(numFields);
  entrySchema->columnTypes.reserve(numFields);

  for (int i = 0; i < numFields; ++i) {
    const auto& field = arrowSchema->field(i);
    const std::string& columnName = field->name();

    entrySchema->columnNames.push_back(columnName);

    auto commonType = converter.convert(*field->type());
    if (!commonType) {
      RETURN_STATUS_ERROR(
          neug::StatusCode::ERR_TYPE_CONVERSION,
          "Failed to convert Arrow type for column: " + columnName);
    }
    entrySchema->columnTypes.push_back(std::move(commonType));
  }

  return entrySchema;
}

}  // namespace reader
}  // namespace neug
