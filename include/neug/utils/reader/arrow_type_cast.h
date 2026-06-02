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

#include <arrow/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>

#include <functional>
#include <map>
#include <memory>
#include <utility>
#include <vector>

namespace neug {
namespace reader {

/**
 * @brief Casts Arrow columns from one type to another using registered
 *        conversion functions.
 *
 * Primary use case: CSV files cannot represent Arrow list types natively.
 * List columns are read as strings (e.g. "[1,2,3]") and then converted to
 * the target list type by this caster.
 */
class ArrowTypeCaster {
 public:
  using CastFunc = std::function<std::shared_ptr<arrow::Array>(
      const std::shared_ptr<arrow::Array>&,
      const std::shared_ptr<arrow::DataType>&)>;

  void registerCast(arrow::Type::type from, arrow::Type::type to,
                    CastFunc func);

  std::shared_ptr<arrow::Table> castTable(
      const std::shared_ptr<arrow::Table>& table,
      const std::shared_ptr<arrow::Schema>& expectedSchema) const;

  std::shared_ptr<arrow::RecordBatch> castBatch(
      const std::shared_ptr<arrow::RecordBatch>& batch,
      const std::shared_ptr<arrow::Schema>& expectedSchema) const;

  std::shared_ptr<arrow::Array> castArray(
      const std::shared_ptr<arrow::Array>& array,
      const std::shared_ptr<arrow::DataType>& targetType) const;

  static std::shared_ptr<ArrowTypeCaster> createDefault();

 private:
  std::shared_ptr<arrow::ChunkedArray> castChunkedArray(
      const std::shared_ptr<arrow::ChunkedArray>& array,
      const std::shared_ptr<arrow::DataType>& targetType) const;

  std::map<std::pair<arrow::Type::type, arrow::Type::type>, CastFunc> casters_;
};

/**
 * @brief A RecordBatch subclass that lazily converts columns on first access.
 *
 * Instead of eagerly converting all columns when the batch is obtained,
 * this class defers type casting until column(i) is actually called.
 * This preserves the lazy evaluation design of batch_read where data
 * construction is delayed until the consumer requests it.
 */
class LazyTypeCastRecordBatch : public arrow::RecordBatch {
 public:
  LazyTypeCastRecordBatch(std::shared_ptr<arrow::RecordBatch> inner,
                          std::shared_ptr<ArrowTypeCaster> caster,
                          std::shared_ptr<arrow::Schema> expectedSchema);

  std::shared_ptr<arrow::Array> column(int i) const override;
  const std::vector<std::shared_ptr<arrow::Array>>& columns() const override;
  std::shared_ptr<arrow::ArrayData> column_data(int i) const override;
  const arrow::ArrayDataVector& column_data() const override;

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> AddColumn(
      int i, const std::shared_ptr<arrow::Field>& field,
      const std::shared_ptr<arrow::Array>& column) const override;
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> SetColumn(
      int i, const std::shared_ptr<arrow::Field>& field,
      const std::shared_ptr<arrow::Array>& column) const override;
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> RemoveColumn(
      int i) const override;
  std::shared_ptr<arrow::RecordBatch> ReplaceSchemaMetadata(
      const std::shared_ptr<const arrow::KeyValueMetadata>& metadata)
      const override;
  std::shared_ptr<arrow::RecordBatch> Slice(int64_t offset,
                                            int64_t length) const override;
  const std::shared_ptr<arrow::Device::SyncEvent>& GetSyncEvent()
      const override;
  arrow::DeviceAllocationType device_type() const override;

 private:
  void materializeColumn(int i) const;
  void materializeAll() const;

  std::shared_ptr<arrow::RecordBatch> inner_;
  std::shared_ptr<ArrowTypeCaster> caster_;
  std::shared_ptr<arrow::Schema> expectedSchema_;
  mutable std::vector<std::shared_ptr<arrow::Array>> cachedColumns_;
  mutable arrow::ArrayDataVector cachedColumnData_;
  mutable std::vector<bool> materialized_;
  mutable bool allMaterialized_ = false;
};

}  // namespace reader
}  // namespace neug
