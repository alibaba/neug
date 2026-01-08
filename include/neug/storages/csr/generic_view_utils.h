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

#include "neug/storages/csr/generic_view.h"

namespace gs {

namespace runtime {
class EdgeRecord;
}  // namespace runtime
enum class DataTypeId : uint8_t;
class Property;

int32_t fuzzy_search_offset_from_nbr_list(const NbrList& nbr_list,
                                          vid_t expected_nbr,
                                          const void* expected_prop,
                                          const DataTypeId& type);

std::pair<int32_t, int32_t> record_to_csr_offset_pair(
    const GenericView& oe, const GenericView& ie,
    const gs::runtime::EdgeRecord& record,
    const std::vector<DataTypeId>& props);

int32_t search_ie_offset_with_oe_offset(const GenericView& oe,
                                        const GenericView& ie, vid_t src_lid,
                                        vid_t dst_lid, int32_t oe_offset,
                                        const std::vector<DataTypeId>& props);

int32_t search_oe_offset_with_ie_offset(const GenericView& oe,
                                        const GenericView& ie, vid_t src_lid,
                                        vid_t dst_lid, int32_t ie_offset,
                                        const std::vector<DataTypeId>& props);

// Determine the property type to be used in searching edge offsets
// For single property edges with non-string type, use that type directly
// For multi-property edges or single string type property, use uint64_t as the
// search type
inline DataTypeId determine_search_prop_type(
    const std::vector<DataTypeId>& props) {
  return (props.size() == 1 && props[0] != DataTypeId::kStringView)
             ? props[0]
             : DataTypeId::kUInt64;
}

}  // namespace gs
