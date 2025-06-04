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

#include "src/engines/graph_db/runtime/execute/ops/insert/data_source.h"
#include "src/engines/graph_db/runtime/common/columns/arrow_context_column.h"

namespace gs {
namespace runtime {
namespace ops {

// CSVDataSourceOpr read from csv file and load the arrow table into memory.
//  Insert the columns to the context.

bl::result<Context> CSVDataSourceOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer& timer) {
  if (ctx.row_num() != 0) {
    LOG(ERROR) << "Expect a empty context, but got " << ctx.row_num();
    return bl::new_error(gs::StatusCode::ERR_INVALID_ARGUMENT,
                         "Expect a empty context");
  }

  if (batch_reader_) {
    LOG(INFO) << "Using batch reader for CSV data source";
    return eval_batch_reader(std::move(ctx));
  } else {
    LOG(INFO) << "Using table reader for CSV data source";
    return eval_table_reader(std::move(ctx));
  }
}

bl::result<Context> CSVDataSourceOpr::eval_batch_reader(Context&& ctx) {
  if (ctx.col_num() != 0) {
    LOG(ERROR) << "Expect a empty context, but got " << ctx.col_num();
    return bl::new_error(gs::StatusCode::ERR_INVALID_ARGUMENT,
                         "Expect a empty context");
  }
  // Try to get the first batch from the suppliers.
  std::shared_ptr<arrow::RecordBatch> first_batch;
  for (const auto& supplier : suppliers_) {
    // Copy the supplier to get the first batch.
    // This is because the supplier may be a streaming supplier, and we need to
    // get the first batch without consuming the supplier.
    if (!supplier) {
      LOG(ERROR) << "Supplier is null";
      return bl::new_error(gs::StatusCode::ERR_INVALID_ARGUMENT,
                           "Supplier is null");
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
    return bl::new_error(gs::StatusCode::ERR_INVALID_ARGUMENT,
                         "No batch found from the suppliers");
  }
  auto num_columns = first_batch->num_columns();
  LOG(INFO) << "Got first batch with " << num_columns << " columns";
  auto supplier_with_first_batch =
      std::make_shared<SupplierWrapperWithFirstBatch>(suppliers_, first_batch);
  for (int i = 0; i < num_columns; i++) {
    ArrowStreamContextColumnBuilder column_builder(i,
                                                   {supplier_with_first_batch});
    ctx.set(i, column_builder.finish(nullptr));
  }
  return bl::result<Context>(std::move(ctx));
}

bl::result<Context> CSVDataSourceOpr::eval_table_reader(Context&& ctx) {
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
      return bl::new_error(gs::StatusCode::ERR_INVALID_ARGUMENT,
                           "Expect the same number of columns");
    }
    arrow_columns.push_back(batch->columns());
  }
  LOG(INFO) << "Got " << num_batch << " batches with " << num_columns
            << " columns";
  // Expect there are no aliases in the context.
  if (ctx.col_num() != 0) {
    LOG(ERROR) << "Expect a empty context, but got " << ctx.col_num();
    return bl::new_error(gs::StatusCode::ERR_INVALID_ARGUMENT,
                         "Expect a empty context");
  }
  for (int i = 0; i < num_columns; i++) {
    ArrowArrayContextColumnBuilder column_builder;
    for (int j = 0; j < num_batch; j++) {
      column_builder.push_back(arrow_columns[j][i]);
    }
    ctx.set(i, column_builder.finish(nullptr));
  }
  return bl::result<Context>(std::move(ctx));
}

std::unique_ptr<IUpdateOperator> DataSourceOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  if (!plan.query_plan().plan(op_idx).opr().has_source()) {
    LOG(ERROR) << "Data source operator is not found in the plan.";
  }
  auto source_opr = plan.query_plan().plan(op_idx).opr().source();
  if (source_opr.has_read_csv()) {
    auto csv_record_suppliers =
        create_csv_record_suppliers(source_opr.read_csv());
    return std::make_unique<CSVDataSourceOpr>(
        csv_record_suppliers, source_opr.read_csv().batch_reader());
  } else {
    LOG(FATAL) << "Unsupported csv data source, got: "
               << source_opr.ShortDebugString();
  }
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs