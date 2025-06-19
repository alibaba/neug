#include "gopt_test.h"

namespace gs {
namespace gopt {
class ProjectTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/tinysnb_schema.yaml");
  std::string statsData = "";
  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};

  std::string getProjectResource(const std::string& resource) {
    return getGOptResource("project_test/" + resource);
  }
};

TEST_F(ProjectTest, LIMIT) {
  std::string query =
      "MATCH (a:person) RETURN a.fName ORDER BY a.fName LIMIT 1 + 1;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getProjectResource("LIMIT_physical"));
}
}  // namespace gopt
}  // namespace gs