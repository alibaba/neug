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

#include "neug/common/types/graph_types.h"
#include "neug/execution/common/context.h"
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

namespace neug {
class IDataChunkSupplier;
class Schema;
class StorageReadInterface;
namespace execution {

namespace ops {

std::string vertex_to_json_string(label_t label, vid_t vid,
                                  const StorageReadInterface& graph);

std::string edge_to_json_string(const EdgeRecord& edge,
                                const StorageReadInterface& graph);

std::string path_to_json_string(Path& path, const StorageReadInterface& graph);

std::shared_ptr<IDataChunkSupplier> create_data_chunk_supplier(
    const Context& ctx,
    const std::vector<std::pair<int32_t, std::string>>& prop_mappings);

std::vector<std::string> match_files_with_pattern(const std::string& file_path);

std::vector<std::shared_ptr<IDataChunkSupplier>> create_csv_chunk_suppliers(
    const std::string& file_path, const std::vector<DataType>& column_types,
    const std::unordered_map<std::string, std::string> csv_options);

void parse_property_mappings(
    const google::protobuf::RepeatedPtrField<physical::PropertyMapping>&
        property_mappings,
    std::vector<std::pair<int32_t, std::string>>& prop_mappings);

}  // namespace ops

}  // namespace execution

}  // namespace neug
