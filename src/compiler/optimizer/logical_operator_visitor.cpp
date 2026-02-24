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

#include "neug/compiler/optimizer/logical_operator_visitor.h"

using namespace neug::planner;

namespace neug {
namespace optimizer {

void LogicalOperatorVisitor::visitOperatorSwitch(LogicalOperator* op) {
  switch (op->getOperatorType()) {
  case LogicalOperatorType::ACCUMULATE: {
    visitAccumulate(op);
  } break;
  case LogicalOperatorType::AGGREGATE: {
    visitAggregate(op);
  } break;
  case LogicalOperatorType::COPY_FROM: {
    visitCopyFrom(op);
  } break;
  case LogicalOperatorType::COPY_TO: {
    visitCopyTo(op);
  } break;
  case LogicalOperatorType::DELETE: {
    visitDelete(op);
  } break;
  case LogicalOperatorType::DISTINCT: {
    visitDistinct(op);
  } break;
  case LogicalOperatorType::EMPTY_RESULT: {
    visitEmptyResult(op);
  } break;
  case LogicalOperatorType::EXPRESSIONS_SCAN: {
    visitExpressionsScan(op);
  } break;
  case LogicalOperatorType::EXTEND: {
    visitExtend(op);
  } break;
  case LogicalOperatorType::GET_V: {
    visitGetV(op);  // This is not implemented in the base class
  } break;
  case LogicalOperatorType::FILTER: {
    visitFilter(op);
  } break;
  case LogicalOperatorType::FLATTEN: {
    visitFlatten(op);
  } break;
  case LogicalOperatorType::HASH_JOIN: {
    visitHashJoin(op);
  } break;
  case LogicalOperatorType::INTERSECT: {
    visitIntersect(op);
  } break;
  case LogicalOperatorType::INSERT: {
    visitInsert(op);
  } break;
  case LogicalOperatorType::LIMIT: {
    visitLimit(op);
  } break;
  case LogicalOperatorType::MERGE: {
    visitMerge(op);
  } break;
  case LogicalOperatorType::NODE_LABEL_FILTER: {
    visitNodeLabelFilter(op);
  } break;
  case LogicalOperatorType::ORDER_BY: {
    visitOrderBy(op);
  } break;
  case LogicalOperatorType::PATH_PROPERTY_PROBE: {
    visitPathPropertyProbe(op);
  } break;
  case LogicalOperatorType::PROJECTION: {
    visitProjection(op);
  } break;
  case LogicalOperatorType::RECURSIVE_EXTEND: {
    visitRecursiveExtend(op);
  } break;
  case LogicalOperatorType::SCAN_NODE_TABLE: {
    visitScanNodeTable(op);
  } break;
  case LogicalOperatorType::SET_PROPERTY: {
    visitSetProperty(op);
  } break;
  case LogicalOperatorType::TABLE_FUNCTION_CALL: {
    visitTableFunctionCall(op);
  } break;
  case LogicalOperatorType::UNION_ALL: {
    visitUnion(op);
  } break;
  case LogicalOperatorType::UNWIND: {
    visitUnwind(op);
  } break;
  case LogicalOperatorType::CROSS_PRODUCT: {
    visitCrossProduct(op);
  }
  default:
    return;
  }
}

std::shared_ptr<LogicalOperator>
LogicalOperatorVisitor::visitOperatorReplaceSwitch(
    std::shared_ptr<LogicalOperator> op) {
  switch (op->getOperatorType()) {
  case LogicalOperatorType::ACCUMULATE: {
    return visitAccumulateReplace(op);
  }
  case LogicalOperatorType::AGGREGATE: {
    return visitAggregateReplace(op);
  }
  case LogicalOperatorType::COPY_FROM: {
    return visitCopyFromReplace(op);
  }
  case LogicalOperatorType::COPY_TO: {
    return visitCopyToReplace(op);
  }
  case LogicalOperatorType::DELETE: {
    return visitDeleteReplace(op);
  }
  case LogicalOperatorType::DISTINCT: {
    return visitDistinctReplace(op);
  }
  case LogicalOperatorType::EMPTY_RESULT: {
    return visitEmptyResultReplace(op);
  }
  case LogicalOperatorType::EXPRESSIONS_SCAN: {
    return visitExpressionsScanReplace(op);
  }
  case LogicalOperatorType::EXTEND: {
    return visitExtendReplace(op);
  }
  case LogicalOperatorType::GET_V: {
    // This is not implemented in the base class
    return visitGetVReplace(op);
  }
  case LogicalOperatorType::FILTER: {
    return visitFilterReplace(op);
  }
  case LogicalOperatorType::FLATTEN: {
    return visitFlattenReplace(op);
  }
  case LogicalOperatorType::HASH_JOIN: {
    return visitHashJoinReplace(op);
  }
  case LogicalOperatorType::INTERSECT: {
    return visitIntersectReplace(op);
  }
  case LogicalOperatorType::INSERT: {
    return visitInsertReplace(op);
  }
  case LogicalOperatorType::LIMIT: {
    return visitLimitReplace(op);
  }
  case LogicalOperatorType::MERGE: {
    return visitMergeReplace(op);
  }
  case LogicalOperatorType::NODE_LABEL_FILTER: {
    return visitNodeLabelFilterReplace(op);
  }
  case LogicalOperatorType::ORDER_BY: {
    return visitOrderByReplace(op);
  }
  case LogicalOperatorType::PATH_PROPERTY_PROBE: {
    return visitPathPropertyProbeReplace(op);
  }
  case LogicalOperatorType::PROJECTION: {
    return visitProjectionReplace(op);
  }
  case LogicalOperatorType::RECURSIVE_EXTEND: {
    return visitRecursiveExtendReplace(op);
  }
  case LogicalOperatorType::SCAN_NODE_TABLE: {
    return visitScanNodeTableReplace(op);
  }
  case LogicalOperatorType::SET_PROPERTY: {
    return visitSetPropertyReplace(op);
  }
  case LogicalOperatorType::TABLE_FUNCTION_CALL: {
    return visitTableFunctionCallReplace(op);
  }
  case LogicalOperatorType::UNION_ALL: {
    return visitUnionReplace(op);
  }
  case LogicalOperatorType::UNWIND: {
    return visitUnwindReplace(op);
  }
  case LogicalOperatorType::CROSS_PRODUCT: {
    return visitCrossProductReplace(op);
  }
  default:
    return op;
  }
}

}  // namespace optimizer
}  // namespace neug
