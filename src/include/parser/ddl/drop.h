#pragma once

#include "drop_info.h"
#include "src/include/parser/statement.h"

namespace gs {
namespace parser {

class Drop : public Statement {
  static constexpr common::StatementType type_ = common::StatementType::DROP;

 public:
  explicit Drop(DropInfo dropInfo)
      : Statement{type_}, dropInfo{std::move(dropInfo)} {}

  const DropInfo& getDropInfo() const { return dropInfo; }

 private:
  DropInfo dropInfo;
};

}  // namespace parser
}  // namespace gs
