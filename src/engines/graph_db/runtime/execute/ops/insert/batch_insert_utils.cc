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

#include "src/engines/graph_db/runtime/execute/ops/insert/batch_insert_utils.h"
#include "src/engines/graph_db/runtime/common/columns/arrow_context_column.h"
#include "src/storages/rt_mutable_graph/loader/csv_fragment_loader.h"
#include "src/storages/rt_mutable_graph/loader/loader_utils.h"

namespace gs {

namespace runtime {

namespace ops {

PropertyType get_the_pk_type_from_schema(const Schema& schema,
                                         label_t label_id) {
  auto pks = schema.get_vertex_primary_key(label_id);
  if (pks.empty()) {
    LOG(FATAL) << "No primary key found for label id: " << label_id;
  }
  if (pks.size() > 1) {
    LOG(FATAL) << "Multiple primary keys found for label id: " << label_id;
  }
  auto pk = pks[0];
  if (std::get<0>(pk) == PropertyType::Empty()) {
    LOG(FATAL) << "Invalid primary key type for label id: " << label_id;
  }
  return std::get<0>(pk);
}

std::vector<std::shared_ptr<IRecordBatchSupplier>>
create_record_batch_supplier_from_arrow_array_column(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  std::vector<std::vector<std::shared_ptr<arrow::Array>>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> fields;

  arrays.resize(prop_mappings.size());
  for (size_t i = 0; i < prop_mappings.size(); ++i) {
    auto mapping = prop_mappings[i];
    auto tag_id = mapping.first;
    auto prop_name = mapping.second;
    auto column = ctx.get(tag_id);
    if (column == nullptr) {
      LOG(FATAL) << "Column not found for tag id: " << tag_id;
    }
    auto arrow_column =
        std::dynamic_pointer_cast<ArrowArrayContextColumn>(column);
    if (!arrow_column) {
      LOG(FATAL) << "Invalid column type for tag id: " << tag_id;
    }

    auto& column_arrays = arrow_column->GetColumns();
    // arrays[i].emplace(column_arrays.begin(), column_arrays.end());
    for (auto& array : column_arrays) {
      arrays[i].emplace_back(array);
    }
    fields.emplace_back(std::make_shared<arrow::Field>(
        prop_name, arrow_column->GetArrowType(), true));
  }
  if (!arrays.empty()) {
    size_t batch_size = arrays[0].size();
    for (size_t i = 1; i < arrays.size(); ++i) {
      auto& array = arrays[i];
      if (array.size() != batch_size) {
        LOG(FATAL) << "Array size mismatch for tag id: "
                   << prop_mappings[i].first;
      }
    }
  }
  auto schema = std::make_shared<arrow::Schema>(fields);
  suppliers.emplace_back(
      std::make_shared<ArrowRecordBatchArraySupplier>(arrays, schema));
  return suppliers;
}

std::vector<std::shared_ptr<IRecordBatchSupplier>>
create_record_batch_supplier_from_arrow_stream_column(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  LOG(INFO) << "column mappings size: " << prop_mappings.size();
  for (const auto& mapping : prop_mappings) {
    auto tag_id = mapping.first;
    auto column = ctx.get(tag_id);
    if (column == nullptr) {
      LOG(ERROR) << "Column not found for tag id: " << tag_id;
      throw std::runtime_error("Column not found for tag id: " +
                               std::to_string(tag_id));
    }
    if (column->column_type() != ContextColumnType::kArrowStream) {
      LOG(ERROR) << "Invalid column type for tag id: " << tag_id;
      throw std::runtime_error("Invalid column type for tag id: " +
                               std::to_string(tag_id));
    }
    auto casted_column =
        std::dynamic_pointer_cast<ArrowStreamContextColumn>(column);
    if (!casted_column) {
      LOG(ERROR) << "Failed to cast column for tag id: " << tag_id;
      throw std::runtime_error("Failed to cast column for tag id: " +
                               std::to_string(tag_id));
    }
    return casted_column->GetSuppliers();
  }
  LOG(ERROR) << "No valid column mappings found.";
  throw std::runtime_error("No valid column mappings found.");
}

std::vector<std::shared_ptr<IRecordBatchSupplier>> create_record_batch_supplier(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  // We expect all columns are of same type.
  ContextColumnType column_type = ContextColumnType::kNone;
  for (const auto& mapping : prop_mappings) {
    auto tag_id = mapping.first;
    auto column = ctx.get(tag_id);
    if (column == nullptr) {
      LOG(ERROR) << "Column not found for tag id: " << tag_id;
      throw std::runtime_error("Column not found for tag id: " +
                               std::to_string(tag_id));
    }
    if (column_type == ContextColumnType::kNone) {
      column_type = column->column_type();
    } else if (column_type != column->column_type()) {
      LOG(ERROR) << "Column type mismatch for tag id: " << tag_id;
      throw std::runtime_error("Column type mismatch for tag id: " +
                               std::to_string(tag_id));
    }
  }
  if (column_type == ContextColumnType::kArrowArray) {
    return create_record_batch_supplier_from_arrow_array_column(ctx,
                                                                prop_mappings);
  } else if (column_type == ContextColumnType::kArrowStream) {
    return create_record_batch_supplier_from_arrow_stream_column(ctx,
                                                                 prop_mappings);
  } else {
    LOG(ERROR) << "Unsupported column type: " << static_cast<int>(column_type);
    throw std::runtime_error("Unsupported column type: " +
                             std::to_string(static_cast<int>(column_type)));
  }
}

void to_arrow_csv_options(const physical::ReadCSV::options& csv_options,
                          arrow::csv::ConvertOptions& convert_options,
                          arrow::csv::ReadOptions& read_options,
                          arrow::csv::ParseOptions& parse_options) {
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCTimeStampParser>());
  convert_options.timestamp_parsers.emplace_back(
      std::make_shared<LDBCLongDateParser>());
  convert_options.timestamp_parsers.emplace_back(
      arrow::TimestampParser::MakeISO8601());
  // BOOLEAN parser
  put_boolean_option(convert_options);
  if (csv_options.delimiter().empty()) {
    VLOG(10) << "Using default CSV delimiter: " << DEFAULT_CSV_DELIMITER;
    put_delimiter_option(DEFAULT_CSV_DELIMITER, parse_options);
  } else {
    put_delimiter_option(csv_options.delimiter(), parse_options);
  }
  if (!csv_options.escape_char().empty()) {
    if (csv_options.escape_char().size() == 1) {
      parse_options.escaping = true;
      parse_options.escape_char = csv_options.escape_char()[0];
    } else {
      LOG(ERROR) << "Invalid escape char: " << csv_options.escape_char();
      parse_options.escaping = false;
    }
  }
  if (!csv_options.quote_char().empty()) {
    if (csv_options.quote_char().size() == 1) {
      parse_options.quoting = true;
      parse_options.quote_char = csv_options.quote_char()[0];
    } else {
      LOG(ERROR) << "Invalid quote char: " << csv_options.quote_char();
      parse_options.quoting = false;
    }
  }
  // put_block_size_option(loading_config_, read_options);
  // put_null_values(loading_config_, convert_options);
  if (csv_options.header()) {
    // TODO: FIXME, seems skip_rows skips the first row.
    read_options.skip_rows = 0;
  } else {
    read_options.skip_rows = 0;
  }

  // TODO: support selecting included columns.
}

std::vector<std::shared_ptr<IRecordBatchSupplier>> create_csv_record_suppliers(
    const physical::ReadCSV& read_csv) {
  std::vector<std::shared_ptr<IRecordBatchSupplier>> suppliers;
  auto file_path = read_csv.file_path();
  arrow::csv::ConvertOptions convert_options;
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;

  to_arrow_csv_options(read_csv.csv_options(), convert_options, read_options,
                       parse_options);
  if (read_csv.batch_reader()) {
    suppliers.emplace_back(std::dynamic_pointer_cast<IRecordBatchSupplier>(
        std::make_shared<CSVStreamRecordBatchSupplier>(
            file_path, convert_options, read_options, parse_options)));
  } else {
    suppliers.emplace_back(std::dynamic_pointer_cast<IRecordBatchSupplier>(
        std::make_shared<CSVTableRecordBatchSupplier>(
            file_path, convert_options, read_options, parse_options)));
  }
  return suppliers;
}

void parse_property_mappings(
    const google::protobuf::RepeatedPtrField<physical::PropertyMapping>&
        property_mappings,
    std::vector<std::pair<int32_t, std::string>>& prop_mappings) {
  for (const auto& mapping : property_mappings) {
    if (mapping.has_property() && mapping.property().has_key()) {
      auto prop_name = mapping.property().key().name();
      if (mapping.data().operators_size() != 1 ||
          !mapping.data().operators(0).has_var()) {
        LOG(FATAL) << "Invalid property mapping: " << prop_name;
      }
      auto tag_id = mapping.data().operators(0).var().tag().id();
      prop_mappings.emplace_back(tag_id, prop_name);
    }
  }
}
}  // namespace ops

}  // namespace runtime

}  // namespace gs