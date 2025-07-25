#pragma once

#include "neug/compiler/parser/statement.h"
#include "parsed_data/attach_info.h"

namespace gs {
namespace parser {

class AttachDatabase final : public Statement {
 public:
  explicit AttachDatabase(AttachInfo attachInfo)
      : Statement{common::StatementType::ATTACH_DATABASE},
        attachInfo{std::move(attachInfo)} {}

  const AttachInfo& getAttachInfo() const { return attachInfo; }

 private:
  AttachInfo attachInfo;
};

}  // namespace parser
}  // namespace gs
