#pragma once

#include "src/include/catalog/catalog.h"
#include "src/include/common/mask.h"
#include "src/include/function/table/bind_data.h"
#include "src/include/graph/graph.h"
#include "src/include/graph/graph_entry.h"

namespace gs {

namespace main {
class ClientContext;
}

namespace function {

struct KUZU_API GDSConfig {
  virtual ~GDSConfig() = default;

  template <class TARGET>
  const TARGET& constCast() const {
    return *common::ku_dynamic_cast<const TARGET*>(this);
  }
};

struct KUZU_API GDSOptionalParams {
  virtual ~GDSOptionalParams() = default;

  virtual std::unique_ptr<GDSConfig> getConfig() const = 0;
  virtual std::unique_ptr<GDSOptionalParams> copy() const = 0;
};

struct KUZU_API GDSBindData : public TableFuncBindData {
  graph::GraphEntry graphEntry;
  std::shared_ptr<binder::Expression> nodeOutput;
  std::unique_ptr<GDSOptionalParams> optionalParams;

  GDSBindData(binder::expression_vector columns, graph::GraphEntry graphEntry,
              std::shared_ptr<binder::Expression> nodeOutput,
              std::unique_ptr<GDSOptionalParams> optionalParams = nullptr)
      : TableFuncBindData{std::move(columns)},
        graphEntry{graphEntry.copy()},
        nodeOutput{std::move(nodeOutput)},
        optionalParams{std::move(optionalParams)} {}

  GDSBindData(const GDSBindData& other)
      : TableFuncBindData{other},
        graphEntry{other.graphEntry.copy()},
        nodeOutput{other.nodeOutput},
        optionalParams{other.optionalParams == nullptr
                           ? nullptr
                           : other.optionalParams->copy()} {}

  std::unique_ptr<GDSConfig> getConfig() const {
    return optionalParams->getConfig();
  }

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<GDSBindData>(*this);
  }

 private:
};

struct KUZU_API GDSFuncSharedState : public TableFuncSharedState {
  std::unique_ptr<graph::Graph> graph;

  GDSFuncSharedState(std::unique_ptr<graph::Graph> graph)
      : TableFuncSharedState{}, graph{std::move(graph)} {}

  void setGraphNodeMask(std::unique_ptr<common::NodeOffsetMaskMap> maskMap);
  common::NodeOffsetMaskMap* getGraphNodeMaskMap() const {
    return graphNodeMask.get();
  }

 public:
 private:
  std::unique_ptr<common::NodeOffsetMaskMap> graphNodeMask = nullptr;
};

class KUZU_API GDSFunction {
  static constexpr char NODE_COLUMN_NAME[] = "node";

 public:
  static graph::GraphEntry bindGraphEntry(main::ClientContext& context,
                                          const std::string& name);
  static graph::GraphEntry bindGraphEntry(
      main::ClientContext& context,
      const graph::ParsedGraphEntry& parsedGraphEntry);
  static std::shared_ptr<binder::Expression> bindNodeOutput(
      const TableFuncBindInput& bindInput,
      const std::vector<catalog::TableCatalogEntry*>& nodeEntries);
  static std::string bindColumnName(const parser::YieldVariable& yieldVariable,
                                    std::string expressionName);

  static std::unique_ptr<TableFuncSharedState> initSharedState(
      const TableFuncInitSharedStateInput& input);
  static void getLogicalPlan(
      planner::Planner* planner,
      const binder::BoundReadingClause& readingClause,
      binder::expression_vector predicates,
      std::vector<std::unique_ptr<planner::LogicalPlan>>& logicalPlans);
};

}  // namespace function
}  // namespace gs
