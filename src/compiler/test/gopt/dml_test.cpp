#include <gtest/gtest.h>
#include "gopt_test.h"
namespace gs {
namespace gopt {

class DMLTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/person_schema.yaml");
  std::string statsData = getGOptResource("stats/person_stats.json");
  std::string getDMLResource(std::string resource) {
    return getGOptResource("dml_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};

  std::string replaceResource(const std::string& query) {
    auto testResourceVal = getGOptResourcePath("dml_test");
    std::string processedLine = query;
    std::string pattern = "DML_RESOURCE";
    size_t pos = processedLine.find(pattern);
    while (pos != std::string::npos) {
      processedLine.replace(pos, strlen(pattern.c_str()), testResourceVal);
      pos = processedLine.find(pattern, pos + 1);
    }
    return processedLine;
  }
};

TEST_F(DMLTest, COPY_PERSON) {
  std::string query =
      replaceResource("COPY person from 'DML_RESOURCE/person.csv';");
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  ASSERT_TRUE(physical != nullptr);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}

TEST_F(DMLTest, COPY_KNOWS) {
  std::string query =
      replaceResource("COPY knows from 'DML_RESOURCE/knows.csv';");
  auto logical = planLogical(query, schemaData, statsData, rules);
  auto aliasManager = std::make_shared<GAliasManager>(*logical);
  auto physical = planPhysical(*logical, aliasManager);
  ASSERT_TRUE(physical != nullptr);
  auto resultYaml = GResultSchema::infer(*logical, aliasManager, getCatalog());
  auto returns = resultYaml["returns"];
  ASSERT_TRUE(returns.IsSequence() && returns.size() == 0);
}
}  // namespace gopt
}  // namespace gs