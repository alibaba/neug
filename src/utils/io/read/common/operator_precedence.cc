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

#include "neug/utils/io/read/common/operator_precedence.h"

namespace neug {
namespace reader {

int OperatorPrecedence::getPrecedence(const ::common::ExprOpr& opr) {
  switch (opr.item_case()) {
  case ::common::ExprOpr::kLogical: {
    switch (opr.logical()) {
    case ::common::Logical::AND:
      return 11;
    case ::common::Logical::OR:
      return 12;
    case ::common::Logical::NOT:
      return 2;
    case ::common::Logical::EQ:
    case ::common::Logical::NE:
      return 7;
    case ::common::Logical::GE:
    case ::common::Logical::GT:
    case ::common::Logical::LT:
    case ::common::Logical::LE:
      return 6;
    default:
      return 16;
    }
  }
  case ::common::ExprOpr::kArith: {
    switch (opr.arith()) {
    case ::common::Arithmetic::ADD:
    case ::common::Arithmetic::SUB:
      return 4;
    case ::common::Arithmetic::MUL:
    case ::common::Arithmetic::DIV:
    case ::common::Arithmetic::MOD:
      return 3;
    default:
      return 16;
    }
  }
  case ::common::ExprOpr::kBrace:
    return 17;
  default:
    return 16;
  }
}

}  // namespace reader
}  // namespace neug
