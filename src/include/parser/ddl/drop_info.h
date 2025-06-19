#pragma once
#include <string>

#include "src/include/common/enums/conflict_action.h"
#include "src/include/common/enums/drop_type.h"

namespace gs {
namespace parser {

struct DropInfo {
  std::string name;
  common::DropType dropType;
  common::ConflictAction conflictAction;
};

}  // namespace parser
}  // namespace gs
