#pragma once

#include "neug/compiler/common/api.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class KUZU_API CatalogException : public Exception {
 public:
  explicit CatalogException(const std::string& msg)
      : Exception("Catalog exception: " + msg){};
};

}  // namespace common
}  // namespace gs
