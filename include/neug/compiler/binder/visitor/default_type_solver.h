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

#include "neug/compiler/binder/bound_statement_visitor.h"

namespace neug {
namespace binder {

// Assign a default data type (STRING) for expressions with ANY data type for a
// given statement. E.g. RETURN NULL; Expression NULL can be resolved as any
// type based on semantic. We don't iterate all expressions because
// - predicates must have been resolved to BOOL type
// - lhs expressions for update must have been resolved to column type
// So we only need to resolve for expressions appear in the projection clause.
// This assumption might change as we add more features.
class DefaultTypeSolver final : public BoundStatementVisitor {
 private:
  void visitProjectionBody(const BoundProjectionBody& projectionBody) override;
};

}  // namespace binder
}  // namespace neug
