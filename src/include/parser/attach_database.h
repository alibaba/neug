#pragma once

#include "parsed_data/attach_info.h"
#include "src/include/parser/statement.h"

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
