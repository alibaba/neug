#include "common/enums/query_rel_type.h"

#include "common/assert.h"
#include "function/gds/gds_function_collection.h"

using namespace gs::function;

namespace gs {
namespace common {

PathSemantic QueryRelTypeUtils::getPathSemantic(QueryRelType queryRelType) {
  switch (queryRelType) {
  case QueryRelType::VARIABLE_LENGTH_WALK:
    return PathSemantic::WALK;
  case QueryRelType::VARIABLE_LENGTH_TRAIL:
    return PathSemantic::TRAIL;
  case QueryRelType::VARIABLE_LENGTH_ACYCLIC:
  case QueryRelType::SHORTEST:
  case QueryRelType::ALL_SHORTEST:
  case QueryRelType::WEIGHTED_SHORTEST:
  case QueryRelType::ALL_WEIGHTED_SHORTEST:
    return PathSemantic::ACYCLIC;
  default:
    KU_UNREACHABLE;
  }
}

std::unique_ptr<function::RJAlgorithm> QueryRelTypeUtils::getFunction(
    QueryRelType type) {
  KU_UNREACHABLE;
}

}  // namespace common
}  // namespace gs
