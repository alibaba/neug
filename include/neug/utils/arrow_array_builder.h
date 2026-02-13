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

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/types/value.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

#include <arrow/api.h>

namespace neug {

class IArrowArrayBuilder {
 public:
  virtual ~IArrowArrayBuilder() = default;
  virtual std::shared_ptr<arrow::Array> Build() = 0;
};

////////////////// C++ Arrow appender functions ////////////////////////

std::shared_ptr<IArrowArrayBuilder> create_wrap_array_builder(DataType type);

std::unique_ptr<arrow::ArrayBuilder> create_arrow_array_builder(
    const StorageReadInterface& graph, DataType type,
    std::shared_ptr<execution::IContextColumn> col = nullptr);

void append_property_to_builder(const Property& prop, DataTypeId type_id,
                                arrow::ArrayBuilder* builder);

template <typename T, typename Enable = void>
class ArrowArrayWrapBuilder : public IArrowArrayBuilder {
 public:
  ArrowArrayWrapBuilder() {}
  ~ArrowArrayWrapBuilder() = default;

  std::shared_ptr<arrow::Array> Build() override {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "ArrowArrayWrapBuilder not support for this type");
  }

 private:
};

std::shared_ptr<arrow::Buffer> convert_vector_bool_to_bitmap(
    const std::vector<bool>& bitmap, size_t& null_count);
/**
 * For wrapping a vector as an Arrow array.
 */
template <typename T>
class ArrowArrayWrapBuilder<
    T, typename std::enable_if<is_arrow_wrappable<T>::value>::type>
    : public IArrowArrayBuilder {
 public:
  ArrowArrayWrapBuilder() : data_(nullptr), size_(0), bitmap_(nullptr) {}
  ~ArrowArrayWrapBuilder() = default;

  void Wrap(const T* data, size_t size,
            const std::vector<bool>* bitmap = nullptr) {
    data_ = data;
    size_ = size;
    if (bitmap != nullptr) {
      bitmap_ = bitmap;
    }
  }

  std::shared_ptr<arrow::Array> Build() override {
    auto buffer = arrow::Buffer::Wrap(data_, size_);
    std::shared_ptr<arrow::Buffer> validity_buffer = nullptr;
    size_t null_count = 0;
    if (bitmap_ != nullptr && !bitmap_->empty()) {
      validity_buffer = convert_vector_bool_to_bitmap(*bitmap_, null_count);
    }
    auto array_data =
        arrow::ArrayData::Make(TypeConverter<T>::ArrowTypeValue(), size_,
                               {validity_buffer, buffer}, null_count);
    return arrow::MakeArray(std::move(array_data));
  }

 private:
  const T* data_;
  size_t size_;
  const std::vector<bool>* bitmap_;
};

/**
 * Build an Arrow Table from columns.
 */
class ArrowTableBuilder {
 public:
  ArrowTableBuilder() = default;
  ~ArrowTableBuilder() = default;

  // TODO(zhanglei): Support nullable column
  void AddColumn(const std::string& name, std::shared_ptr<arrow::Array> array);

  std::shared_ptr<arrow::Table> Build();

 private:
  std::vector<std::shared_ptr<arrow::Field>> fields_;
  std::vector<std::shared_ptr<arrow::Array>> columns_;
};

}  // namespace neug