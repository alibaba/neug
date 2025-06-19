#pragma once

#include "src/include/binder/expression/expression.h"

namespace gs {
namespace gopt {
class GPrecedence {
 public:
  int getPrecedence(const binder::Expression& expr);
  bool isLeftAssociative(const binder::Expression& expr);
  bool needBrace(const binder::Expression& parent,
                 const binder::Expression& child);
};
}  // namespace gopt
}  // namespace gs