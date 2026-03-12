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

#include <vector>

#include "neug/utils/property/types.h"

namespace physical {
class PropertyMapping;
}
namespace google {
namespace protobuf {
template <typename T>
class RepeatedPtrField;
}
}  // namespace google

namespace arrow {
namespace csv {
struct ConvertOptions;
struct ParseOptions;
struct ReadOptions;
}  // namespace csv
}  // namespace arrow

namespace neug {
class IRecordBatchSupplier;
class Schema;
class StorageReadInterface;
namespace execution {
class Context;
class VertexRecord;
class EdgeRecord;
struct Path;

namespace ops {

static constexpr const char* DEFAULT_CSV_DELIMITER = "|";
static const std::string CSV_DELIMITER_KEY = "DELIMITER";
static const std::string CSV_DELIM_KEY = "DELIM";
static const std::string CSV_HEADER_KEY = "HEADER";
static const std::string CSV_QUOTE_KEY = "QUOTE";
static const std::string CSV_DOUBLE_QUOTE_KEY = "DOUBLE_QUOTE";
static const std::string CSV_ESCAPE_KEY = "ESCAPE";
static const std::string CSV_SKIP_KEY = "SKIP";
static const std::string CSV_PARALLEL_KEY = "PARALLEL";
static const std::string CSV_NULL_STRINGS_KEY = "NULL_STRINGS";
static const std::string CSV_STREAM_READER = "STREAM_READER";

bool check_csv_import_options(
    const std::unordered_map<std::string, std::string>& options);

std::string vertex_to_json_string(label_t label, vid_t vid,
                                  const StorageReadInterface& graph);

std::string edge_to_json_string(const EdgeRecord& edge,
                                const StorageReadInterface& graph);

std::string path_to_json_string(Path& path, const StorageReadInterface& graph);

// The datasource operator will really load the data into the memory, and stored
// as arrow::Array in Context.
// To insert the data into the graph, we need to construct the the supplier
// which could create the arrow::RecordBatch from the holded arrow::Array.
std::vector<std::shared_ptr<IRecordBatchSupplier>> create_record_batch_supplier(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings);

void to_arrow_csv_options(
    const std::string& file_path,
    const std::unordered_map<std::string, std::string>& csv_options,
    const std::vector<DataTypeId>& column_types,
    arrow::csv::ConvertOptions& convert_options,
    arrow::csv::ReadOptions& read_options,
    arrow::csv::ParseOptions& parse_options);

std::vector<std::string> match_files_with_pattern(const std::string& file_path);

std::vector<std::shared_ptr<IRecordBatchSupplier>> create_csv_record_suppliers(
    const std::string& file_path, const std::vector<DataType>& column_types,
    const std::unordered_map<std::string, std::string> csv_options);

void parse_property_mappings(
    const google::protobuf::RepeatedPtrField<physical::PropertyMapping>&
        property_mappings,
    std::vector<std::pair<int32_t, std::string>>& prop_mappings);

}  // namespace ops

}  // namespace execution

}  // namespace neug
