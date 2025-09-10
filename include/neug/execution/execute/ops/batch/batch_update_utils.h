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

#ifndef RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_UTILS_H_
#define RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_UTILS_H_

#include <glog/logging.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "neug/execution/common/context.h"
#include "neug/storages/loader/abstract_arrow_fragment_loader.h"
#include "neug/utils/property/types.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/cypher_dml.pb.h"
#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/common.pb.h"
#include "neug/utils/proto/plan/cypher_dml.pb.h"
#include "neug/utils/proto/plan/expr.pb.h"
#include "neug/utils/proto/plan/physical.pb.h"
#endif

namespace arrow {
namespace csv {
struct ConvertOptions;
struct ParseOptions;
struct ReadOptions;
}  // namespace csv
}  // namespace arrow

namespace gs {
class IRecordBatchSupplier;
class Schema;

namespace runtime {
class Context;

namespace ops {

static constexpr const char* DEFAULT_CSV_DELIMITER = "|";
static const std::string CSV_DELIMITER_KEY = "DELIMITER";
static const std::string CSV_DELIM_KEY = "DELIM";
static const std::string CSV_HEADER_KEY = "HEADER";
static const std::string CSV_QUOTE_KEY = "QUOTE";
static const std::string CSV_DOUBLE_QUOTE_KEY = "DOUBLE_QUOTE";
static const std::string CSV_ESCAPE_KEY = "ESCAPE";
static const std::string CSV_SKIP_KEY = "SKIP";
static const std::string CSV_IGNORE_ERRORS_KEY = "IGNORE_ERRORS";
static const std::string CSV_PARALLEL_KEY = "PARALLEL";
static const std::string CSV_NULL_STRINGS_KEY = "NULL_STRINGS";
static const std::string CSV_STREAM_READER = "STREAM_READER";

bool check_csv_import_options(
    const std::unordered_map<std::string, std::string>& options);

bool check_csv_export_options(
    const std::unordered_map<std::string, std::string>& options);

std::string vertex_to_json_string(label_t label, vid_t vid,
                                  const gs::runtime::GraphReadInterface& graph);

std::string edge_to_json_string(EdgeRecord& edge,
                                const gs::runtime::GraphReadInterface& graph);

std::string path_to_json_string(Path& path,
                                const gs::runtime::GraphReadInterface& graph);

PropertyType get_the_pk_type_from_schema(const Schema& schema,
                                         label_t label_id);

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
    const std::vector<PropertyType>& column_types,
    arrow::csv::ConvertOptions& convert_options,
    arrow::csv::ReadOptions& read_options,
    arrow::csv::ParseOptions& parse_options);

std::vector<std::shared_ptr<IRecordBatchSupplier>> create_csv_record_suppliers(
    const std::string& file_path, const std::vector<PropertyType>& column_types,
    const std::unordered_map<std::string, std::string> csv_options);

void parse_property_mappings(
    const google::protobuf::RepeatedPtrField<physical::PropertyMapping>&
        property_mappings,
    std::vector<std::pair<int32_t, std::string>>& prop_mappings);

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_UTILS_H_