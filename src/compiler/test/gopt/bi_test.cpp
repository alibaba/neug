#include "gopt_test.h"
namespace gs {
namespace gopt {

class BITest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/sf_0.1.yaml");
  std::string statsData = getGOptResource("stats/sf_0.1-statistics.json");

  std::string getBIResource(std::string resource) {
    return getGOptResource("bi_test/" + resource);
  };

  std::string getBIResourcePath(std::string resource) {
    return getGOptResourcePath("bi_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(BITest, BI_1_MINI) {
  std::string query =
      "MATCH (message:COMMENT:POST) RETURN count(message) AS messageCount, "
      "sum(message.length) / count(message) AS averageMessageLength, "
      "count(message.length) AS sumMessageLength ";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getBIResource("BI_1_MINI_physical"));
}

}  // namespace gopt
}  // namespace gs