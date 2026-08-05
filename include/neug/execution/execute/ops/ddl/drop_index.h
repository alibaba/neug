#pragma once

#include "neug/execution/execute/operator.h"

namespace neug::execution::ops {

class DropIndexOprBuilder : public IOperatorBuilder {
 public:
  neug::result<OpBuildResultT> Build(const Schema& schema,
                                     const ContextMeta& ctx_meta,
                                     const physical::PhysicalPlan& plan,
                                     int op_id) override;
  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kDropIndex};
  }
};

}  // namespace neug::execution::ops
