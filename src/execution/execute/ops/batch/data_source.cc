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

#include <arrow/record_batch.h>
#include <glog/logging.h>
#include <stdint.h>

#include <ostream>
#include <utility>

#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/batch/data_source.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/result.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace gs {
class Schema;

namespace runtime {
class GraphUpdateInterface;
class OprTimer;

namespace ops {

// CSVDataSourceOpr read from csv file and load the arrow table into memory.
//  Insert the columns to the context.

/**
 * @brief DataSourceOpr is used to load data from a CSV file.
 */
class CSVDataSourceOpr : public IUpdateOperator {
 public:
  static constexpr bool batch_reader_default = true;
  CSVDataSourceOpr(
      const std::vector<std::shared_ptr<IRecordBatchSupplier>>& suppliers,
      bool batch_reader)
      : suppliers_(suppliers), batch_reader_(batch_reader) {}

  ~CSVDataSourceOpr() = default;

  std::string get_operator_name() const override { return "CSVDataSourceOpr"; }

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  gs::result<Context> eval_table_reader(Context&& ctx);
  gs::result<Context> eval_batch_reader(Context&& ctx);

  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers_;
  bool batch_reader_;  // With batch reader, we will read the file in batches.
};

gs::result<Context> CSVDataSourceOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer* timer) {
  if (ctx.row_num() != 0) {
    LOG(ERROR) << "Expect a empty context, but got " << ctx.row_num();
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                            "Expect a empty context"));
  }

  if (batch_reader_) {
    LOG(INFO) << "Using batch reader for CSV data source";
    return eval_batch_reader(std::move(ctx));
  } else {
    LOG(INFO) << "Using table reader for CSV data source";
    return eval_table_reader(std::move(ctx));
  }
}

gs::result<Context> CSVDataSourceOpr::eval_batch_reader(Context&& ctx) {
  if (ctx.col_num() != 0) {
    LOG(ERROR) << "Expect a empty context, but got " << ctx.col_num();
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                            "Expect a empty context"));
  }
  // Try to get the first batch from the suppliers.
  std::shared_ptr<arrow::RecordBatch> first_batch;
  for (const auto& supplier : suppliers_) {
    // Copy the supplier to get the first batch.
    // This is because the supplier may be a streaming supplier, and we need to
    // get the first batch without consuming the supplier.
    if (!supplier) {
      LOG(ERROR) << "Supplier is null";
      RETURN_ERROR(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT, "Supplier is null"));
    }
    first_batch = supplier->GetNextBatch();
    if (first_batch) {
      LOG(INFO) << "Got first batch with " << first_batch->num_rows()
                << " rows and " << first_batch->num_columns() << " columns";
      break;
    }
  }
  if (!first_batch) {
    LOG(ERROR) << "No batch found from the suppliers";
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                            "No batch found from the suppliers"));
  }
  auto num_columns = first_batch->num_columns();
  LOG(INFO) << "Got first batch with " << num_columns << " columns";
  auto supplier_with_first_batch =
      std::make_shared<SupplierWrapperWithFirstBatch>(suppliers_, first_batch);
  for (int i = 0; i < num_columns; i++) {
    ArrowStreamContextColumnBuilder column_builder({supplier_with_first_batch});
    ctx.set(i, column_builder.finish());
  }
  return gs::result<Context>(std::move(ctx));
}

gs::result<Context> CSVDataSourceOpr::eval_table_reader(Context&& ctx) {
  // M X N, where M is the number of batches and N is the number of columns.
  std::vector<std::vector<std::shared_ptr<arrow::Array>>> arrow_columns;
  int32_t num_batch = 0, num_columns = 0;
  for (const auto& supplier : suppliers_) {
    auto batch = supplier->GetNextBatch();
    if (!batch) {
      continue;
    }
    num_batch++;
    if (num_columns == 0) {
      num_columns = batch->num_columns();
    } else if (num_columns != batch->num_columns()) {
      LOG(ERROR) << "Expect the same number of columns, but got " << num_columns
                 << " and " << batch->num_columns();
      RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                              "Expect the same number of columns"));
    }
    arrow_columns.push_back(batch->columns());
  }
  LOG(INFO) << "Got " << num_batch << " batches with " << num_columns
            << " columns";
  // Expect there are no aliases in the context.
  if (ctx.col_num() != 0) {
    LOG(ERROR) << "Expect a empty context, but got " << ctx.col_num();
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                            "Expect a empty context"));
  }
  for (int i = 0; i < num_columns; i++) {
    ArrowArrayContextColumnBuilder column_builder;
    for (int j = 0; j < num_batch; j++) {
      column_builder.push_back(arrow_columns[j][i]);
    }
    ctx.set(i, column_builder.finish());
  }
  return gs::result<Context>(std::move(ctx));
}

std::unique_ptr<IUpdateOperator> DataSourceOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  if (!plan.query_plan().plan(op_idx).opr().has_source()) {
    LOG(ERROR) << "Data source operator is not found in the plan.";
  }
  auto source_opr = plan.query_plan().plan(op_idx).opr().source();
  auto extension_name = source_opr.extension_name();
  auto file_path = source_opr.file_path();
  std::unordered_map<std::string, std::string> options;
  for (const auto& entry : source_opr.options()) {
    std::string upper_str;
    std::transform(entry.first.begin(), entry.first.end(),
                   std::back_inserter(upper_str), ::toupper);
    options.insert({upper_str, entry.second});
  }
  const auto& metadatas = plan.query_plan().plan(op_idx).meta_data();
  std::vector<PropertyType> column_types;
  for (const auto& metadata : metadatas) {
    PropertyType type;
    if (!gs::data_type_to_property_type(metadata.type().data_type(), type)) {
      THROW_RUNTIME_ERROR("Unrecognized data type: " +
                          metadata.type().DebugString());
    }
    column_types.push_back(type);
  }

  if (extension_name == "csv") {
    auto csv_record_suppliers =
        create_csv_record_suppliers(file_path, column_types, options);
    return std::make_unique<CSVDataSourceOpr>(csv_record_suppliers, true);
  } else {
    LOG(FATAL) << "Unsupported csv data source, got: "
               << source_opr.ShortDebugString();
  }
  return nullptr;  // to suppress compiler warning
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs
