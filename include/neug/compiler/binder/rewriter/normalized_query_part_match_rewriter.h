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
namespace main {
class ClientContext;
}
namespace binder {

// Merge consecutive match pattern in a query part. E.g.
// MATCH (a) WHERE a.ID = 0
// MATCH (b) WHERE b.ID = 1
// MATCH (a)-[]->(b)
// will be rewritten as
// MATCH (a)-[]->(b) WHERE a.ID = 0 AND b.ID = 1
// This rewrite does not apply to MATCH with HINT or OPTIONAL MATCH
class NormalizedQueryPartMatchRewriter final : public BoundStatementVisitor {
 public:
  explicit NormalizedQueryPartMatchRewriter(main::ClientContext* clientContext)
      : clientContext{clientContext} {}

 private:
  void visitQueryPartUnsafe(NormalizedQueryPart& queryPart) override;

 private:
  main::ClientContext* clientContext;
};

}  // namespace binder
}  // namespace neug
