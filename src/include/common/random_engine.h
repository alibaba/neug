#pragma once

#include <mutex>

#include "src/include/common/api.h"
#include "third_party/pcg/pcg_random.hpp"

namespace gs {

namespace main {
class ClientContext;
}

namespace common {

struct RandomState {
  pcg32 pcg;

  RandomState() {}
};

class KUZU_API RandomEngine {
 public:
  RandomEngine();
  RandomEngine(uint64_t seed, uint64_t stream);

  uint32_t nextRandomInteger();
  uint32_t nextRandomInteger(uint32_t upper);

 private:
  std::mutex mtx;
  RandomState randomState;
};

}  // namespace common
}  // namespace gs
