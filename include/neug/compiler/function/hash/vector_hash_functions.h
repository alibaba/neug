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
#include "neug/compiler/function/function.h"

namespace gs {
namespace function {

struct UnaryHashFunctionExecutor {
  template <typename OPERAND_TYPE, typename RESULT_TYPE>
  static void execute(const common::ValueVector& operand,
                      const common::SelectionVector& operandSelectVec,
                      common::ValueVector& result,
                      const common::SelectionVector& resultSelectVec);
};

struct BinaryHashFunctionExecutor {
  template <typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE,
            typename FUNC>
  static void execute(const common::ValueVector& left,
                      const common::SelectionVector& leftSelVec,
                      const common::ValueVector& right,
                      const common::SelectionVector& rightSelVec,
                      common::ValueVector& result,
                      const common::SelectionVector& resultSelVec);
};

struct VectorHashFunction {
  static void computeHash(const common::ValueVector& operand,
                          const common::SelectionVector& operandSelectVec,
                          common::ValueVector& result,
                          const common::SelectionVector& resultSelectVec);

  static void combineHash(const common::ValueVector& left,
                          const common::SelectionVector& leftSelVec,
                          const common::ValueVector& right,
                          const common::SelectionVector& rightSelVec,
                          common::ValueVector& result,
                          const common::SelectionVector& resultSelVec);
};

struct SHA256Function {
  static constexpr const char* name = "SHA256";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace gs
