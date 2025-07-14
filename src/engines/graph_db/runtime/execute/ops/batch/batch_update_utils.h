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
#include <utility>
#include <vector>

#include "src/engines/graph_db/runtime/common/context.h"
#include "src/proto_generated_gie/cypher_dml.pb.h"
#include "src/proto_generated_gie/physical.pb.h"
#include "src/storages/rt_mutable_graph/loader/abstract_arrow_fragment_loader.h"
#include "src/storages/rt_mutable_graph/types.h"
#include "src/utils/property/types.h"

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

PropertyType get_the_pk_type_from_schema(const Schema& schema,
                                         label_t label_id);

// The datasource operator will really load the data into the memory, and stored
// as arrow::Array in Context.
// To insert the data into the graph, we need to construct the the supplier
// which could create the arrow::RecordBatch from the holded arrow::Array.
std::vector<std::shared_ptr<IRecordBatchSupplier>> create_record_batch_supplier(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings);

void to_arrow_csv_options(const physical::ReadCSV::options& csv_options,
                          arrow::csv::ConvertOptions& convert_options,
                          arrow::csv::ReadOptions& read_options,
                          arrow::csv::ParseOptions& parse_options);

std::vector<std::shared_ptr<IRecordBatchSupplier>> create_csv_record_suppliers(
    const physical::ReadCSV& read_csv);

void parse_property_mappings(
    const google::protobuf::RepeatedPtrField<physical::PropertyMapping>&
        property_mappings,
    std::vector<std::pair<int32_t, std::string>>& prop_mappings);

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_UTILS_H_