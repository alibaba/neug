

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

#include "neug/execution/execute/ops/retrieve/scan_utils.h"
#include "neug/execution/common/types/value.h"
#include "neug/execution/utils/pb_parse_utils.h"

namespace neug {
namespace execution {
namespace ops {

template <typename T>
std::vector<Property> parse_ids_from_idx_predicate(
    const algebra::IndexPredicate_Triplet& triplet, const ParamsMap& params) {
  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
    std::vector<Property> ret;
    if (triplet.const_().item_case() == common::Value::kI32) {
      ret.emplace_back(
          PropUtils<T>::to_prop(static_cast<T>(triplet.const_().i32())));
    } else if (triplet.const_().item_case() == common::Value::kI64) {
      ret.emplace_back(
          PropUtils<T>::to_prop(static_cast<T>(triplet.const_().i64())));
    } else if (triplet.const_().item_case() == common::Value::kU32) {
      ret.emplace_back(
          PropUtils<T>::to_prop(static_cast<T>(triplet.const_().u32())));
    } else if (triplet.const_().item_case() == common::Value::kU64) {
      ret.emplace_back(
          PropUtils<T>::to_prop(static_cast<T>(triplet.const_().u64())));
    } else if (triplet.const_().item_case() == common::Value::kI64Array) {
      const auto& arr = triplet.const_().i64_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        ret.emplace_back(PropUtils<T>::to_prop(static_cast<T>(arr.item(i))));
      }
    } else if (triplet.const_().item_case() == common::Value::kI32Array) {
      const auto& arr = triplet.const_().i32_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        ret.emplace_back(PropUtils<T>::to_prop(static_cast<T>(arr.item(i))));
      }
    }
    return ret;
  }

  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
    auto param_type = parse_from_ir_data_type(triplet.param().data_type());

    if (param_type.id() == DataTypeId::kInt32) {
      return std::vector<Property>{PropUtils<T>::to_prop(
          params.at(triplet.param().name()).template GetValue<T>())};
    } else if (param_type.id() == DataTypeId::kInt64) {
      return std::vector<Property>{PropUtils<T>::to_prop(
          params.at(triplet.param().name()).template GetValue<T>())};
    }
  }
  default:
    break;
  }
  return {};
}

std::vector<Property> parse_ids_from_idx_predicate(
    const algebra::IndexPredicate_Triplet& triplet, const ParamsMap& params) {
  std::vector<Property> ret;
  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
    if (triplet.const_().item_case() == common::Value::kStr) {
      ret.emplace_back(Property::from_string_view(triplet.const_().str()));

    } else if (triplet.const_().item_case() == common::Value::kStrArray) {
      const auto& arr = triplet.const_().str_array();
      for (int i = 0; i < arr.item_size(); ++i) {
        ret.emplace_back(Property::from_string_view(arr.item(i)));
      }
    }
    return ret;
  }

  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
    auto param_type = parse_from_ir_data_type(triplet.param().data_type());

    if (param_type.id() == DataTypeId::kVarchar) {
      auto sw = value_to_property(params.at(triplet.param().name()));
      ret.emplace_back(sw);
      return ret;
    }
  }
  default:
    break;
  }
  return ret;
}
std::vector<Property> ScanUtils::parse_ids_with_type(
    DataTypeId type, const algebra::IndexPredicate_Triplet& triplet,
    const ParamsMap& params) {
  switch (type) {
  case DataTypeId::kInt64: {
    return parse_ids_from_idx_predicate<int64_t>(triplet, params);
  }
  case DataTypeId::kInt32: {
    return parse_ids_from_idx_predicate<int32_t>(triplet, params);
  }
  case DataTypeId::kUInt64: {
    return parse_ids_from_idx_predicate<uint64_t>(triplet, params);
  }
  case DataTypeId::kUInt32: {
    return parse_ids_from_idx_predicate<uint32_t>(triplet, params);
  }
  case DataTypeId::kVarchar: {
    return parse_ids_from_idx_predicate(triplet, params);
  }
  default:
    LOG(FATAL) << "unsupported type" << static_cast<int>(type);
    break;
  }
  return {};
}
bool ScanUtils::check_idx_predicate(const physical::Scan& scan_opr) {
  if (scan_opr.scan_opt() != physical::Scan::VERTEX) {
    return false;
  }

  if (!scan_opr.has_params()) {
    return false;
  }

  if (!scan_opr.has_idx_predicate()) {
    return false;
  }
  const algebra::IndexPredicate& predicate = scan_opr.idx_predicate();
  if (predicate.or_predicates_size() != 1) {
    return false;
  }
  if (predicate.or_predicates(0).predicates_size() != 1) {
    return false;
  }
  const algebra::IndexPredicate_Triplet& triplet =
      predicate.or_predicates(0).predicates(0);
  if (!triplet.has_key()) {
    return false;
  }

  if (triplet.cmp() != common::Logical::EQ &&
      triplet.cmp() != common::Logical::WITHIN) {
    return false;
  }

  switch (triplet.value_case()) {
  case algebra::IndexPredicate_Triplet::ValueCase::kConst: {
  } break;
  case algebra::IndexPredicate_Triplet::ValueCase::kParam: {
  } break;
  default: {
    return false;
  } break;
  }

  return true;
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
