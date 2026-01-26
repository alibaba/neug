#pragma once

#include <memory>
#include <string>

namespace neug {

struct OPPrintInfo {
  OPPrintInfo() {}
  virtual ~OPPrintInfo() = default;

  virtual std::string toString() const { return std::string(); }

  virtual std::unique_ptr<OPPrintInfo> copy() const {
    return std::make_unique<OPPrintInfo>();
  }
};

}  // namespace neug
