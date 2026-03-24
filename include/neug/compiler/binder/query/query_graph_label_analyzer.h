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

#include "neug/compiler/main/client_context.h"
#include "query_graph.h"

namespace neug {
namespace binder {

class QueryGraphLabelAnalyzer {
 public:
  explicit QueryGraphLabelAnalyzer(const main::ClientContext& clientContext,
                                   bool throwOnViolate)
      : throwOnViolate{throwOnViolate}, clientContext{clientContext} {}

  void pruneLabel(QueryGraph& graph, bool skipNodes = false) const;

 private:
  void pruneNode(const QueryGraph& graph, NodeExpression& node) const;
  void pruneRel(RelExpression& rel) const;

 private:
  bool throwOnViolate;
  const main::ClientContext& clientContext;
};

}  // namespace binder
}  // namespace neug
