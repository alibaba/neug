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

#include <limits>
#include <vector>

#include "neug/execution/common/types/graph_types.h"

namespace neug {
namespace execution {

struct ScanParams {
  int alias;
  std::vector<label_t> tables;

  ScanParams() : alias(-1) {}
};

struct GetVParams {
  VOpt opt;
  int tag;
  std::vector<label_t> tables;
  int alias;
};

struct EdgeExpandParams {
  int v_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  bool is_optional;
};

struct PathExpandParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
  PathOpt opt;
};

struct ShortestPathParams {
  int start_tag;
  std::vector<LabelTriplet> labels;
  int alias;
  int v_alias;
  Direction dir;
  int hop_lower;
  int hop_upper;
};

struct JoinParams {
  std::vector<int> left_columns;
  std::vector<int> right_columns;
  JoinKind join_type;
};

}  // namespace execution
}  // namespace neug
