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

#ifndef RUNTIME_UTILS_UTILS_H_
#define RUNTIME_UTILS_UTILS_H_

#include <stddef.h>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/rt_any.h"
#include "neug/execution/common/types.h"
#include "neug/execution/utils/expr.h"
#include "neug/utils/property/types.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/algebra.pb.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/plan/type.pb.h"
#else
#include "neug/utils/proto/plan/algebra.pb.h"
#include "neug/utils/proto/plan/common.pb.h"
#include "neug/utils/proto/plan/physical.pb.h"
#include "neug/utils/proto/plan/type.pb.h"
#endif

namespace algebra {
class QueryParams;
}  // namespace algebra

namespace gs {

namespace runtime {
class IVertexColumn;

VOpt parse_opt(const physical::GetV_VOpt& opt);

Direction parse_direction(const physical::EdgeExpand_Direction& dir);

PathOpt parse_path_opt(const physical::PathExpand_PathOpt& path_opt_pb);

std::vector<label_t> parse_tables(const algebra::QueryParams& query_params);

std::vector<LabelTriplet> parse_label_triplets(
    const physical::PhysicalOpr_MetaData& meta);

bool vertex_property_topN(bool asc, size_t limit,
                          const std::shared_ptr<IVertexColumn>& col,
                          const GraphReadInterface& graph,
                          const std::string& prop_name,
                          std::vector<size_t>& offsets);

bool vertex_id_topN(bool asc, size_t limit,
                    const std::shared_ptr<IVertexColumn>& col,
                    const GraphReadInterface& graph,
                    std::vector<size_t>& offsets);

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_UTILS_UTILS_H_