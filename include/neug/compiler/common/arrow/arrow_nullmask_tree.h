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

#include "neug/compiler/common/arrow/arrow.h"
#include "neug/compiler/common/null_mask.h"
#include "neug/compiler/common/vector/value_vector.h"

namespace neug {
namespace common {

class ArrowNullMaskTree {
 public:
  ArrowNullMaskTree(const ArrowSchema* schema, const ArrowArray* array,
                    uint64_t srcOffset, uint64_t count,
                    const NullMask* parentMask = nullptr);

  void copyToValueVector(ValueVector* vec, uint64_t dstOffset, uint64_t count);
  bool isNull(int64_t idx) { return mask->isNull(idx + offset); }
  ArrowNullMaskTree* getChild(int idx) { return &(*children)[idx]; }
  ArrowNullMaskTree* getDictionary() { return dictionary.get(); }
  ArrowNullMaskTree offsetBy(int64_t offset);

 private:
  bool copyFromBuffer(const void* buffer, uint64_t srcOffset, uint64_t count);
  bool applyParentBitmap(const NullMask* buffer);

  template <typename offsetsT>
  void scanListPushDown(const ArrowSchema* schema, const ArrowArray* array,
                        uint64_t srcOffset, uint64_t count);

  void scanArrayPushDown(const ArrowSchema* schema, const ArrowArray* array,
                         uint64_t srcOffset, uint64_t count);

  void scanStructPushDown(const ArrowSchema* schema, const ArrowArray* array,
                          uint64_t srcOffset, uint64_t count);

  int64_t offset;
  std::shared_ptr<NullMask> mask;
  std::shared_ptr<std::vector<ArrowNullMaskTree>> children;
  std::shared_ptr<ArrowNullMaskTree> dictionary;
};

}  // namespace common
}  // namespace neug
