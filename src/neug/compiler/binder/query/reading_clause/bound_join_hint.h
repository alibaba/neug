#pragma once

#include "neug/compiler/binder/expression/expression.h"

namespace gs {
namespace binder {

struct BoundJoinHintNode {
  std::shared_ptr<Expression> nodeOrRel;
  std::vector<std::shared_ptr<BoundJoinHintNode>> children;

  BoundJoinHintNode() = default;
  explicit BoundJoinHintNode(std::shared_ptr<Expression> nodeOrRel)
      : nodeOrRel{std::move(nodeOrRel)} {}

  void addChild(std::shared_ptr<BoundJoinHintNode> child) {
    children.push_back(std::move(child));
  }

  bool isLeaf() const { return children.empty(); }
  bool isBinary() const { return children.size() == 2; }
  bool isMultiWay() const { return children.size() > 2; }
};

}  // namespace binder
}  // namespace gs
