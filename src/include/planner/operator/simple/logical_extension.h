#pragma once

#include "logical_simple.h"
#include "src/include/extension/extension_action.h"

namespace gs {
namespace planner {

using namespace gs::extension;

class LogicalExtension final : public LogicalSimple {
 public:
  LogicalExtension(std::unique_ptr<ExtensionAuxInfo> auxInfo,
                   std::shared_ptr<binder::Expression> outputExpression)
      : LogicalSimple{LogicalOperatorType::EXTENSION,
                      std::move(outputExpression)},
        auxInfo{std::move(auxInfo)} {}

  std::string getExpressionsForPrinting() const override { return path; }

  const ExtensionAuxInfo& getAuxInfo() const { return *auxInfo; }

  std::unique_ptr<LogicalOperator> copy() override {
    return std::make_unique<LogicalExtension>(auxInfo->copy(),
                                              outputExpression);
  }

 private:
  std::unique_ptr<ExtensionAuxInfo> auxInfo;
  std::string path;
};

}  // namespace planner
}  // namespace gs
