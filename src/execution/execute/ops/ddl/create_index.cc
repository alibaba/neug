/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/execution/execute/ops/ddl/create_index.h"

#include <memory>
#include <string>
#include "neug/common/types.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/index/storage_index.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/result.h"

namespace neug {
namespace execution {
namespace ops {

namespace {

std::unique_ptr<IndexMeta> CreateIndexMeta(
    const Schema& schema, const physical::CreateIndex& create_index) {
  auto index_meta = std::make_unique<IndexMeta>();
  index_meta->name = create_index.name();

  if (create_index.vertex_type().has_name()) {
    index_meta->schema.label_id =
        schema.get_vertex_label_id(create_index.vertex_type().name());
  } else {
    index_meta->schema.label_id =
        static_cast<label_t>(create_index.vertex_type().id());
  }

  index_meta->type = create_index.create_index_type();
  index_meta->schema.property_name = create_index.property();
  index_meta->schema.property_type =
      parse_from_data_type(create_index.property_type());
  for (const auto& [key, value] : create_index.options()) {
    index_meta->options[key] = value;
  }
  return index_meta;
}

}  // namespace

class CreateIndexOpr : public IOperator {
 public:
  CreateIndexOpr(physical::CreateIndex create_index, bool ignore_conflict)
      : create_index_(std::move(create_index)),
        ignore_conflict_(ignore_conflict) {}

  std::string get_operator_name() const override { return "CreateIndexOpr"; }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override {
    auto* update_interface = dynamic_cast<StorageUpdateInterface*>(&graph);
    if (!update_interface) {
      RETURN_STATUS_ERROR(StatusCode::ERR_NOT_SUPPORTED,
                          "CREATE INDEX can only be executed in update mode");
    }

    auto existing_index =
        update_interface->GetIndexByName(create_index_.name());
    if (existing_index) {
      if (ignore_conflict_) {
        return std::move(ctx);
      }
      RETURN_STATUS_ERROR(StatusCode::ERR_ILLEGAL_OPERATION,
                          "Index already exists: " + create_index_.name());
    }

    auto index_meta = CreateIndexMeta(graph.schema(), create_index_);
    GS_RESULT_CHECK(update_interface->CreateIndex(std::move(index_meta)));
    return std::move(ctx);
  }

 private:
  physical::CreateIndex create_index_;
  bool ignore_conflict_;
};

neug::result<OpBuildResultT> CreateIndexOprBuilder::Build(
    const Schema& /*schema*/, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_id) {
  ContextMeta meta = ctx_meta;
  const auto& create_index = plan.plan(op_id).opr().create_index();

  bool ignore_conflict =
      !conflict_action_to_bool(create_index.conflict_action());

  return std::make_pair(
      std::make_unique<CreateIndexOpr>(create_index, ignore_conflict), meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
