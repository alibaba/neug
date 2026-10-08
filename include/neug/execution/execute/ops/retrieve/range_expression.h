#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "neug/execution/common/context.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/expression/expr.h"
#include "neug/generated/proto/plan/algebra.pb.h"
#include "neug/utils/api.h"

namespace neug {
class IStorageInterface;

namespace execution::ops {

struct ResolvedRange {
  uint32_t lower;
  uint32_t upper;
};

class NEUG_API RangeExpression {
 public:
  RangeExpression(const algebra::Range& range, const ContextMeta& ctx_meta);

  ResolvedRange bind(const IStorageInterface* storage,
                     const ParamsMap& params) const;

 private:
  std::unique_ptr<ExprBase> offset_;
  std::unique_ptr<ExprBase> limit_;
  std::string offset_parameter_name_;
  std::string limit_parameter_name_;
};

}  // namespace execution::ops
}  // namespace neug
