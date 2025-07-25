#pragma once

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/types/value/value.h"
namespace gs {
namespace binder {

struct BoundQueryScanSourceInfo {
  common::case_insensitive_map_t<common::Value> options;

  explicit BoundQueryScanSourceInfo(
      common::case_insensitive_map_t<common::Value> options)
      : options{std::move(options)} {}
};

}  // namespace binder
}  // namespace gs
