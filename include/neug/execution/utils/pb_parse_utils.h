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

#include <stddef.h>
#include <memory>
#include <string>
#include <vector>

#include "neug/execution/common/types/graph_types.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/utils/encoder.h"

namespace algebra {
class QueryParams;
}  // namespace algebra

namespace neug {

namespace execution {

VOpt parse_opt(const physical::GetV_VOpt& opt);

Direction parse_direction(const physical::EdgeExpand_Direction& dir);

JoinKind parse_join_kind(const physical::Join_JoinKind& kind);

PathOpt parse_path_opt(const physical::PathExpand_PathOpt& path_opt_pb);

std::vector<label_t> parse_tables(const algebra::QueryParams& query_params);

std::vector<LabelTriplet> parse_label_triplets(
    const physical::PhysicalOpr_MetaData& meta);

AggrKind parse_aggregate(physical::GroupBy_AggFunc::Aggregate v);

}  // namespace execution

}  // namespace neug
