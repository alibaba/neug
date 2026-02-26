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
#include "neug/execution/utils/pb_parse_utils.h"

#include <glog/logging.h>
#include <google/protobuf/wrappers.pb.h>
#include <cstdint>

#include <ostream>
#include <set>
#include <string_view>
#include <tuple>

#include "neug/generated/proto/plan/algebra.pb.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/plan/type.pb.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"

namespace neug {

namespace execution {

VOpt parse_opt(const physical::GetV_VOpt& opt) {
  if (opt == physical::GetV_VOpt::GetV_VOpt_START) {
    return VOpt::kStart;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_END) {
    return VOpt::kEnd;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_OTHER) {
    return VOpt::kOther;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_BOTH) {
    return VOpt::kBoth;
  } else if (opt == physical::GetV_VOpt::GetV_VOpt_ITSELF) {
    return VOpt::kItself;
  } else {
    LOG(FATAL) << "unexpected GetV::Opt";
    return VOpt::kItself;
  }
}

Direction parse_direction(const physical::EdgeExpand_Direction& dir) {
  if (dir == physical::EdgeExpand_Direction_OUT) {
    return Direction::kOut;
  } else if (dir == physical::EdgeExpand_Direction_IN) {
    return Direction::kIn;
  } else if (dir == physical::EdgeExpand_Direction_BOTH) {
    return Direction::kBoth;
  }
  LOG(FATAL) << "not support..." << dir;
  return Direction::kOut;
}

JoinKind parse_join_kind(const physical::Join_JoinKind& kind) {
  switch (kind) {
  case physical::Join_JoinKind::Join_JoinKind_INNER:
    return JoinKind::kInnerJoin;
  case physical::Join_JoinKind::Join_JoinKind_SEMI:
    return JoinKind::kSemiJoin;
  case physical::Join_JoinKind::Join_JoinKind_ANTI:
    return JoinKind::kAntiJoin;
  case physical::Join_JoinKind::Join_JoinKind_LEFT_OUTER:
    return JoinKind::kLeftOuterJoin;
  case physical::Join_JoinKind::Join_JoinKind_TIMES:
    return JoinKind::kTimesJoin;
  default:
    LOG(ERROR) << "unsupported join kind" << kind;
    return JoinKind::kInnerJoin;
  }
}

std::vector<label_t> parse_tables(const algebra::QueryParams& query_params) {
  std::vector<label_t> tables;
  int tn = query_params.tables_size();
  for (int i = 0; i < tn; ++i) {
    const common::NameOrId& table = query_params.tables(i);
    tables.push_back(static_cast<label_t>(table.id()));
  }
  return tables;
}

std::vector<LabelTriplet> parse_label_triplets(
    const physical::PhysicalOpr_MetaData& meta) {
  std::vector<LabelTriplet> labels;
  if (meta.has_type()) {
    const common::IrDataType& t = meta.type();
    if (t.has_graph_type()) {
      const common::GraphDataType& gt = t.graph_type();
      if (gt.element_opt() == common::GraphDataType_GraphElementOpt::
                                  GraphDataType_GraphElementOpt_EDGE ||
          gt.element_opt() == common::GraphDataType_GraphElementOpt::
                                  GraphDataType_GraphElementOpt_PATH) {
        int label_num = gt.graph_data_type_size();
        for (int label_i = 0; label_i < label_num; ++label_i) {
          const ::common::GraphDataType_GraphElementLabel& gdt =
              gt.graph_data_type(label_i).label();
          labels.emplace_back(static_cast<label_t>(gdt.src_label().value()),
                              static_cast<label_t>(gdt.dst_label().value()),
                              static_cast<label_t>(gdt.label()));
        }
      }
    }
  }
  return labels;
}

PathOpt parse_path_opt(const physical::PathExpand_PathOpt& path_opt_pb) {
  if (path_opt_pb ==
      physical::PathExpand_PathOpt::PathExpand_PathOpt_ARBITRARY) {
    return PathOpt::kArbitrary;
  } else if (path_opt_pb ==
             physical::PathExpand_PathOpt::PathExpand_PathOpt_ANY_SHORTEST) {
    return PathOpt::kAnyShortest;
  } else if (path_opt_pb ==
             physical::PathExpand_PathOpt::PathExpand_PathOpt_TRAIL) {
    return PathOpt::kTrail;
  } else if (path_opt_pb ==
             physical::PathExpand_PathOpt::PathExpand_PathOpt_SIMPLE) {
    return PathOpt::kSimple;
  } else if (path_opt_pb ==
             physical::PathExpand_PathOpt::PathExpand_PathOpt_ALL_SHORTEST) {
    return PathOpt::kAllShortest;
  } else if (path_opt_pb == physical::PathExpand_PathOpt::
                                PathExpand_PathOpt_ANY_WEIGHTED_SHORTEST) {
    return PathOpt::kAnyWeightedShortest;
  } else {
    LOG(FATAL) << "unexpected PathOpt";
    return PathOpt::kArbitrary;
  }
}

AggrKind parse_aggregate(physical::GroupBy_AggFunc::Aggregate v) {
  if (v == physical::GroupBy_AggFunc::SUM) {
    return AggrKind::kSum;
  } else if (v == physical::GroupBy_AggFunc::MIN) {
    return AggrKind::kMin;
  } else if (v == physical::GroupBy_AggFunc::MAX) {
    return AggrKind::kMax;
  } else if (v == physical::GroupBy_AggFunc::COUNT) {
    return AggrKind::kCount;
  } else if (v == physical::GroupBy_AggFunc::COUNT_DISTINCT) {
    return AggrKind::kCountDistinct;
  } else if (v == physical::GroupBy_AggFunc::TO_SET) {
    return AggrKind::kToSet;
  } else if (v == physical::GroupBy_AggFunc::FIRST) {
    return AggrKind::kFirst;
  } else if (v == physical::GroupBy_AggFunc::TO_LIST) {
    return AggrKind::kToList;
  } else if (v == physical::GroupBy_AggFunc::AVG) {
    return AggrKind::kAvg;
  } else {
    LOG(FATAL) << "unsupport" << static_cast<int>(v);
    return AggrKind::kSum;
  }
}

}  // namespace execution

}  // namespace neug
