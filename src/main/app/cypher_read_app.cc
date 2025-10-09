#include "neug/main/app/cypher_read_app.h"

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <exception>
#include <map>
#include <ostream>
#include <string_view>
#include <utility>
#include <vector>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/main/app/cypher_app_utils.h"
#include "neug/main/neug_db.h"
#include "neug/storages/graph/schema.h"
#include "neug/main/neug_db_session.h"
#include "neug/utils/app_utils.h"
#include "neug/utils/result.h"

namespace gs {

bool CypherReadApp::Query(const NeugDBSession& graph, Decoder& input,
                          Encoder& output) {
  std::string_view r_bytes = input.get_bytes();
  uint8_t type = static_cast<uint8_t>(r_bytes.back());
  std::string_view bytes = std::string_view(r_bytes.data(), r_bytes.size() - 1);
  if (type == Schema::ADHOC_READ_PLUGIN_ID) {
    physical::PhysicalPlan plan;
    if (!plan.ParseFromString(std::string(bytes))) {
      LOG(ERROR) << "Parse plan failed...";
      return false;
    }

    LOG(INFO) << "plan: " << plan.DebugString();
    auto txn = graph.GetReadTransaction();

    gs::runtime::GraphReadInterface gri(txn);

    std::unique_ptr<runtime::OprTimer> timer = nullptr;
    gs::result<gs::runtime::Context> ctx =
        runtime::ParseAndExecuteReadPipeline(gri, plan, timer.get());

    if (!ctx) {
      LOG(ERROR) << "Error: " << ctx.error().ToString();
      // We encode the error message to the output, so that the client can
      // get the error message.
      output.put_string(ctx.error().ToString());
      return false;
    }
    runtime::Sink::sink(ctx.value(), gri, output);
    return true;
  } else {
    THROW_RUNTIME_ERROR("Unsupported plugin type: " + std::to_string(type));
  }
  return true;
}
AppWrapper CypherReadAppFactory::CreateApp(const NeugDB& db) {
  return AppWrapper(new CypherReadApp(db), NULL);
}
}  // namespace gs