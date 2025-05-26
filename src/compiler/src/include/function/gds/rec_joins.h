#pragma once

#include "binder/expression/expression.h"
#include "common/enums/extend_direction.h"
#include "common/enums/path_semantic.h"
#include "function/gds/gds_state.h"
#include "graph/graph_entry.h"
#include "rj_output_writer.h"

namespace kuzu {
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
};

class RJAlgorithm {
 public:
  virtual ~RJAlgorithm() = default;

  virtual std::string getFunctionName() const = 0;
  virtual binder::expression_vector getResultColumns(
      const RJBindData& bindData) const = 0;

  virtual std::unique_ptr<RJAlgorithm> copy() const = 0;
};

}  // namespace function
}  // namespace kuzu
