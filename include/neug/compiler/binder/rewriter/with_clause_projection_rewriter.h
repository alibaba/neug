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

// WithClauseProjectionRewriter first analyze the properties need to be scanned
// for each query. And then rewrite node/rel expression in WITH clause as their
// properties. So We avoid eagerly evaluate node/rel in WITH clause projection.
// E.g. MATCH (a) WITH a MATCH (a)->(b); will be rewritten as MATCH (a) WITH
// a._id MATCH (a)->(b);
class WithClauseProjectionRewriter final : public BoundStatementVisitor {
 public:
  void visitSingleQueryUnsafe(NormalizedSingleQuery& singleQuery) override;
};

}  // namespace binder
}  // namespace neug
