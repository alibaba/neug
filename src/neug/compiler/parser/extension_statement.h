#pragma once

#include "neug/compiler/extension/extension_action.h"
#include "statement.h"

namespace gs {
namespace parser {

using namespace gs::extension;

class ExtensionStatement final : public Statement {
 public:
  explicit ExtensionStatement(std::unique_ptr<ExtensionAuxInfo> info)
      : Statement{common::StatementType::EXTENSION}, info{std::move(info)} {}

  std::unique_ptr<ExtensionAuxInfo> getAuxInfo() const { return info->copy(); }

 private:
  std::unique_ptr<ExtensionAuxInfo> info;
};

}  // namespace parser
}  // namespace gs
