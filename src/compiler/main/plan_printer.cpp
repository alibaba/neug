#include "src/include/main/plan_printer.h"

#include <sstream>

#include "src/include/planner/operator/logical_plan.h"

using namespace gs::common;
using namespace gs::planner;

namespace gs {
namespace main {

OpProfileBox::OpProfileBox(std::string opName, const std::string& paramsName,
                           std::vector<std::string> attributes)
    : opName{std::move(opName)}, attributes{std::move(attributes)} {
  std::stringstream paramsStream{paramsName};
  std::string paramStr = "";
  std::string subStr;
  bool subParam = false;
  while (paramsStream.good()) {
    getline(paramsStream, subStr, ',');
    if (subStr.find('(') != std::string::npos &&
        subStr.find(')') == std::string::npos) {
      paramStr = subStr;
      subParam = true;
      continue;
    }
    if (subParam && subStr.find(')') == std::string::npos) {
      paramStr += "," + subStr;
      continue;
    }
    if (subParam) {
      subStr = paramStr + ")";
      paramStr = "";
      subParam = false;
    }
    if (subStr.find_first_not_of(" \t\n\v\f\r") != std::string::npos) {
      paramsNames.push_back(subStr);
    }
  }
}

uint32_t OpProfileBox::getAttributeMaxLen() const {
  auto maxAttributeLen = opName.length();
  for (auto& param : paramsNames) {
    maxAttributeLen = std::max(param.length(), maxAttributeLen);
  }
  for (auto& attribute : attributes) {
    maxAttributeLen = std::max(attribute.length(), maxAttributeLen);
  }
  return maxAttributeLen;
}

std::string OpProfileBox::getParamsName(uint32_t idx) const {
  KU_ASSERT(idx < paramsNames.size());
  return paramsNames[idx];
}

std::string OpProfileBox::getAttribute(uint32_t idx) const {
  KU_ASSERT(idx < attributes.size());
  return attributes[idx];
}

void printSpaceIfNecessary(uint32_t idx, std::ostringstream& oss) {
  if (idx > 0) {
    oss << " ";
  }
}

static std::string dashedLineAccountingForIndex(uint32_t width,
                                                uint32_t indent) {
  return std::string(width - (1 + indent) * 2, '-');
}

}  // namespace main
}  // namespace gs
