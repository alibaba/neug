/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "neug/compiler/common/vector/value_vector.h"

namespace gs {
namespace function {

struct UnionTag {
  static void operation(common::union_entry_t& unionValue,
                        common::neug_string_t& tag,
                        common::ValueVector& unionVector,
                        common::ValueVector& tagVector) {
    auto tagIdxVector = common::UnionVector::getTagVector(&unionVector);
    auto tagIdx =
        tagIdxVector->getValue<common::union_field_idx_t>(unionValue.entry.pos);
    auto tagName =
        common::UnionType::getFieldName(unionVector.dataType, tagIdx);
    if (tagName.length() > common::neug_string_t::SHORT_STR_LENGTH) {
      tag.overflowPtr = reinterpret_cast<uint64_t>(
          common::StringVector::getInMemOverflowBuffer(&tagVector)
              ->allocateSpace(tagName.length()));
      memcpy(reinterpret_cast<char*>(tag.overflowPtr), tagName.c_str(),
             tagName.length());
      memcpy(tag.prefix, tagName.c_str(), common::neug_string_t::PREFIX_LENGTH);
    } else {
      memcpy(tag.prefix, tagName.c_str(), tagName.length());
    }
    tag.len = tagName.length();
  }
};

}  // namespace function
}  // namespace gs
