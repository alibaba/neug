#pragma once

#include "logical_simple.h"
#include "neug/compiler/extension/extension_action.h"

namespace neug {
namespace planner {

using namespace neug::extension;

class LogicalExtension final : public LogicalSimple {
 public:
  LogicalExtension(std::unique_ptr<ExtensionAuxInfo> auxInfo,
                   std::shared_ptr<binder::Expression> outputExpression)
      : LogicalSimple{LogicalOperatorType::EXTENSION,
                      std::move(outputExpression)},
        auxInfo{std::move(auxInfo)} {}

  std::string getActionForPrinting() const {
    switch (auxInfo->action) {
    case ExtensionAction::INSTALL: {
      return "INSTALL";
    }
    case ExtensionAction::LOAD: {
      return "LOAD";
    }
    case ExtensionAction::UNINSTALL: {
      return "UNINSTALL";
    }
    default: {
      NEUG_UNREACHABLE;
    }
    }
  }

  std::string getExpressionsForPrinting() const override {
    return getActionForPrinting() + ", " + auxInfo->path;
  }

  const ExtensionAuxInfo& getAuxInfo() const { return *auxInfo; }

  std::unique_ptr<LogicalOperator> copy() override {
    return std::make_unique<LogicalExtension>(auxInfo->copy(),
                                              outputExpression);
  }

 private:
  std::unique_ptr<ExtensionAuxInfo> auxInfo;
};

}  // namespace planner
}  // namespace neug
