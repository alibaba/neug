#pragma once

#include "binder/expression/expression.h"
#include "common/enums/extend_direction.h"
#include "common/enums/path_semantic.h"
#include "function/gds/gds_state.h"
#include "graph/graph_entry.h"
#include "rj_output_writer.h"

namespace gs {
namespace function {

struct RJBindData {
  graph::GraphEntry graphEntry;

  std::shared_ptr<binder::Expression> nodeInput = nullptr;
  std::shared_ptr<binder::Expression> nodeOutput = nullptr;
  uint16_t lowerBound = 0;
  uint16_t upperBound = 0;
  common::PathSemantic semantic = common::PathSemantic::WALK;

  common::ExtendDirection extendDirection = common::ExtendDirection::FWD;

  bool flipPath = false;
  bool writePath = true;

  std::shared_ptr<binder::Expression> directionExpr = nullptr;
  std::shared_ptr<binder::Expression> lengthExpr = nullptr;
  std::shared_ptr<binder::Expression> pathNodeIDsExpr = nullptr;
  std::shared_ptr<binder::Expression> pathEdgeIDsExpr = nullptr;

  std::shared_ptr<binder::Expression> weightPropertyExpr = nullptr;
  std::shared_ptr<binder::Expression> weightOutputExpr = nullptr;

  explicit RJBindData(graph::GraphEntry graphEntry)
      : graphEntry{std::move(graphEntry)} {}
  RJBindData(const RJBindData& other);

  PathsOutputWriterInfo getPathWriterInfo() const;

  std::string toString() const {
    return "RJBindData{"
           "graphEntry=" +
           graphEntry.toString() +
           ", nodeInput=" + (nodeInput ? nodeInput->toString() : "null") +
           ", nodeOutput=" + (nodeOutput ? nodeOutput->toString() : "null") +
           ", lowerBound=" + std::to_string(lowerBound) +
           ", upperBound=" + std::to_string(upperBound) + ", extendDirection=" +
           std::to_string(static_cast<uint8_t>(extendDirection)) +
           ", relPredicate=" + pathEdgeIDsExpr->toString() +
           ", nodePredicate=" + pathNodeIDsExpr->toString() + "}";
  }
};

class RJAlgorithm {
 public:
  virtual ~RJAlgorithm() = default;

  virtual std::string getFunctionName() const = 0;
  virtual binder::expression_vector getResultColumns(
      const RJBindData& bindData) const = 0;

  virtual std::unique_ptr<RJAlgorithm> copy() const = 0;
};

class DefaultRJAlgorithm : public RJAlgorithm {
 public:
  std::string getFunctionName() const override { return "DefaultRJ"; }

  binder::expression_vector getResultColumns(
      const RJBindData& bindData) const override {
    return {};
  }

  std::unique_ptr<RJAlgorithm> copy() const override {
    return std::make_unique<DefaultRJAlgorithm>(*this);
  }
};

}  // namespace function
}  // namespace gs
