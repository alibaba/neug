#pragma once

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/enums/delete_type.h"
#include "neug/compiler/common/enums/table_type.h"

namespace gs {
namespace binder {

struct BoundDeleteInfo {
  common::DeleteNodeType deleteType;
  common::TableType tableType;
  std::shared_ptr<Expression> pattern;

  BoundDeleteInfo(common::DeleteNodeType deleteType,
                  common::TableType tableType,
                  std::shared_ptr<Expression> pattern)
      : deleteType{deleteType},
        tableType{tableType},
        pattern{std::move(pattern)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundDeleteInfo);

  std::string toString() const { return "Delete " + pattern->toString(); }

 private:
  BoundDeleteInfo(const BoundDeleteInfo& other)
      : deleteType{other.deleteType},
        tableType{other.tableType},
        pattern{other.pattern} {}
};

}  // namespace binder
}  // namespace gs
