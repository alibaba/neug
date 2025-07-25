#pragma once

#include <sstream>
#include <string>

#include "kuzu_fwd.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/profiler.h"

namespace gs {
namespace main {

class OpProfileBox {
 public:
  OpProfileBox(std::string opName, const std::string& paramsName,
               std::vector<std::string> attributes);

  inline std::string getOpName() const { return opName; }

  inline uint32_t getNumParams() const { return paramsNames.size(); }

  std::string getParamsName(uint32_t idx) const;

  std::string getAttribute(uint32_t idx) const;

  inline uint32_t getNumAttributes() const { return attributes.size(); }

  uint32_t getAttributeMaxLen() const;

 private:
  std::string opName;
  std::vector<std::string> paramsNames;
  std::vector<std::string> attributes;
};

}  // namespace main
}  // namespace gs
