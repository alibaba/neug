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
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/result.h"

namespace neug {
namespace reader {

class Sniffer {
 public:
  virtual ~Sniffer() = default;
  virtual result<std::shared_ptr<EntrySchema>> sniff() = 0;
};

class CsvSniffer : public Sniffer {
 public:
  explicit CsvSniffer(std::shared_ptr<CsvReader> reader)
      : reader_(std::move(reader)) {
    if (!reader_) {
      THROW_RUNTIME_ERROR("CsvReader cannot be null");
    }
  }

  result<std::shared_ptr<EntrySchema>> sniff() override;

 private:
  std::shared_ptr<CsvReader> reader_;
};

class JsonSniffer : public Sniffer {
 public:
  explicit JsonSniffer(std::shared_ptr<JsonReader> reader)
      : reader_(std::move(reader)) {
    if (!reader_) {
      THROW_RUNTIME_ERROR("JsonReader cannot be null");
    }
  }

  result<std::shared_ptr<EntrySchema>> sniff() override;

 private:
  std::shared_ptr<JsonReader> reader_;
};

}  // namespace reader
}  // namespace neug
