#pragma once

// TODO(Guodong): Change these includes into forward declaration.
#include "src/include/common/profiler.h"
#include "src/include/main/client_context.h"

namespace gs {
namespace processor {

class FactorizedTable;

struct KUZU_API ExecutionContext {
  uint64_t queryID;
  common::Profiler* profiler;
  main::ClientContext* clientContext;

  ExecutionContext(common::Profiler* profiler,
                   main::ClientContext* clientContext, uint64_t queryID)
      : queryID{queryID}, profiler{profiler}, clientContext{clientContext} {}
};

}  // namespace processor
}  // namespace gs
