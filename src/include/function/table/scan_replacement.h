#pragma once

#include "src/include/function/table/bind_input.h"
#include "src/include/function/table/table_function.h"

namespace gs {
namespace function {

struct ScanReplacementData {
  TableFunction func;
  TableFuncBindInput bindInput;
};

using scan_replace_func_t =
    std::function<std::unique_ptr<ScanReplacementData>(const std::string&)>;

struct ScanReplacement {
  explicit ScanReplacement(scan_replace_func_t replaceFunc)
      : replaceFunc{std::move(replaceFunc)} {}

  scan_replace_func_t replaceFunc;
};

}  // namespace function
}  // namespace gs
