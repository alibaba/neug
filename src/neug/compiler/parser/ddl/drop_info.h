#pragma once
#include <string>

#include "neug/compiler/common/enums/conflict_action.h"
#include "neug/compiler/common/enums/drop_type.h"

namespace gs {
namespace parser {

struct DropInfo {
  std::string name;
  common::DropType dropType;
  common::ConflictAction conflictAction;
};

}  // namespace parser
}  // namespace gs
