#include "neug/execution/execute/ops/ddl/drop_index.h"

#include <string>

#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/pb_utils.h"

namespace neug::execution::ops {

class DropIndexOpr : public IOperator {
 public:
  DropIndexOpr(std::string indexName, bool ignore_conflict)
      : indexName_(std::move(indexName)), ignore_conflict_(ignore_conflict) {}

  std::string get_operator_name() const override { return "DropIndexOpr"; }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap&,
                             Context&& ctx, OprTimer*) override {
    auto* updateInterface = dynamic_cast<StorageUpdateInterface*>(&graph);
    if (!updateInterface) {
      RETURN_STATUS_ERROR(StatusCode::ERR_NOT_SUPPORTED,
                          "DROP INDEX can only be executed in update mode");
    }

    if (!updateInterface->GetIndexByName(indexName_)) {
      if (ignore_conflict_) {
        return std::move(ctx);
      }
      RETURN_STATUS_ERROR(StatusCode::ERR_SCHEMA_MISMATCH,
                          "Index does not exist: " + indexName_);
    }

    RETURN_IF_NOT_OK(updateInterface->DropIndex(indexName_));
    return std::move(ctx);
  }

 private:
  std::string indexName_;
  bool ignore_conflict_;
};

neug::result<OpBuildResultT> DropIndexOprBuilder::Build(
    const Schema&, const ContextMeta& ctxMeta,
    const physical::PhysicalPlan& plan, int opId) {
  const auto& dropIndex = plan.plan(opId).opr().drop_index();
  const bool ignore_conflict =
      !conflict_action_to_bool(dropIndex.conflict_action());
  return std::make_pair(
      std::make_unique<DropIndexOpr>(dropIndex.name(), ignore_conflict),
      ctxMeta);
}

}  // namespace neug::execution::ops
