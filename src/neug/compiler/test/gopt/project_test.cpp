#include <yaml-cpp/node/emit.h>
#include <memory>
#include "gopt_test.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_result_schema.h"
#include "neug/compiler/planner/operator/logical_projection.h"

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
TEST_F(ProjectTest, RETURN_STAR) {
  std::string query = "MATCH (a:organisation) RETURN *;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getProjectResource("RETURN_STAR_physical"));
}

TEST_F(ProjectTest, VERTEX_STAR) {
  std::string query = "MATCH (a:organisation) RETURN a.*;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getProjectResource("VERTEX_STAR_physical"));
}

TEST_F(ProjectTest, EDGE_STAR) {
  std::string query =
      "MATCH (a:person)-[e:knows]->(b:person) WHERE a.ID =7 RETURN e.*;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getProjectResource("EDGE_STAR_physical"));
}

TEST_F(ProjectTest, DUPLICATE_EDGE) {
  std::string query =
      "MATCH (a:person) WHERE a.fName='Alice' RETURN a.age, a.age AS k;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<gopt::GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  VerifyFactory::verifyPhysicalByJson(
      *physical, getProjectResource("DUPLICATE_EDGE_physical"));
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  VerifyFactory::verifyResultByYaml(
      resultYaml, getProjectResource("DUPLICATE_EDGE_result"));
}

}  // namespace gopt
}  // namespace gs