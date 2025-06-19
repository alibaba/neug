#pragma once

#include "src/include/catalog/catalog_entry/catalog_entry_type.h"
#include "src/include/function/function.h"

using namespace gs::catalog;

namespace gs {
namespace function {

using get_function_set_fun = std::function<function_set()>;

struct FunctionCollection {
  get_function_set_fun getFunctionSetFunc;

  const char* name;

  CatalogEntryType catalogEntryType;

  static FunctionCollection* getFunctions();
};

}  // namespace function
}  // namespace gs
