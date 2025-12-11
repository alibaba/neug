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
class PropertyType;
class Property;

int32_t fuzzy_search_offset_from_nbr_list(const NbrList& nbr_list,
                                          vid_t expected_nbr,
                                          const void* expected_prop,
                                          const PropertyType& type);

std::pair<int32_t, int32_t> record_to_csr_offset_pair(
    const GenericView& oe, const GenericView& ie,
    const gs::runtime::EdgeRecord& record,
    const std::vector<PropertyType>& props);

int32_t search_ie_offset_with_oe_offset(const GenericView& oe,
                                        const GenericView& ie, vid_t src_lid,
                                        vid_t dst_lid, int32_t oe_offset,
                                        const std::vector<PropertyType>& props);

}  // namespace gs
