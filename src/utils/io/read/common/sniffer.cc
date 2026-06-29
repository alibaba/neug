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

#include "neug/utils/io/read/common/sniffer.h"

#include "neug/utils/result.h"

namespace neug {
namespace reader {

result<std::shared_ptr<EntrySchema>> CsvSniffer::sniff() {
  if (!reader_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "CsvReader is null");
  }
  return reader_->inferSchema();
}

result<std::shared_ptr<EntrySchema>> JsonSniffer::sniff() {
  if (!reader_) {
    RETURN_STATUS_ERROR(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "JsonReader is null");
  }
  return reader_->inferSchema();
}

}  // namespace reader
}  // namespace neug
