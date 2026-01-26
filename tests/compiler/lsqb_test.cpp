/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string>
#include "gopt_test.h"
#include "neug/compiler/gopt/g_result_schema.h"

namespace neug {
namespace gopt {

class LSQBTest : public GOptTest {
 public:
  std::string schemaData = getGOptResource("schema/lsqb_schema.yaml");
  std::string statsData = getGOptResource("stats/lsqb3_stats.json");

  std::string getLSQBResourcePath(std::string resource) {
    return getGOptResourcePath("lsqb_test/" + resource);
  };

  std::string getLSQBResource(std::string resource) {
    return getGOptResource("lsqb_test/" + resource);
  };

  std::vector<std::string> rules = {"FilterPushDown", "ExpandGetVFusion"};
};

TEST_F(LSQBTest, LQ_1_SUB) {
  std::string query =
      "MATCH "
      "(:Forum)-[:Forum_containerOf_Post]->(:Post)<-[Comment_replyOf_Post]-(:"
      "Comment) return count(*);";
  // use lsqb1 stats to reproduce the bug
  auto logical = planLogical(query, schemaData,
                             getGOptResource("stats/lsqb_stats.json"), rules);
  auto physical = planPhysical(*logical);
  VerifyFactory::verifyPhysicalByJson(*physical,
                                      getLSQBResource("LQ_1_SUB_physical"));
}

TEST_F(LSQBTest, LQ_1) {
  std::string query =
      "MATCH "
      "(:Country)<-[:City_isPartOf_Country]-(:City)<-[:Person_isLocatedIn_City]"
      "-(:Person)<-[:Forum_hasMember_Person]-(:Forum)-[:Forum_containerOf_Post]"
      "->(:Post)<-[:Comment_replyOf_Post]-(:Comment)-[:Comment_hasTag_Tag]->(:"
      "Tag)-[:Tag_hasType_TagClass]->(:TagClass) RETURN count(*) as count;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLSQBResource("LQ_1_logical"));
}

TEST_F(LSQBTest, LQ_2) {
  std::string query =
      "MATCH (person1:Person)-[:Person_knows_Person]->(person2:Person), "
      "(person1)<-[:Comment_hasCreator_Person]-(comment:Comment)-[:Comment_"
      "replyOf_Post]->(Post:Post)-[:Post_hasCreator_Person]->(person2) "
      "RETURN count(*) AS count;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLSQBResource("LQ_2_logical"));
}

TEST_F(LSQBTest, LQ_4) {
  std::string query =
      "MATCH "
      "(:Tag)<-[:Post_hasTag_Tag|:Comment_hasTag_Tag]-(message:Post:Comment)-"
      "[:"
      "Post_hasCreator_Person|:Comment_hasCreator_Person]->(creator:Person), "
      "(message)<-[:Person_likes_Comment|:Person_likes_Post]-(liker:Person), "
      "(message)<-[:Comment_replyOf_Comment|:Comment_replyOf_Post]-(comment:"
      "Comment) "
      "RETURN count(*) AS count;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLSQBResource("LQ_4_logical"));
}

TEST_F(LSQBTest, LQ_5) {
  std::string query =
      "MATCH "
      "(tag1:Tag)<-[:Post_hasTag_Tag|:Comment_hasTag_Tag]-(message:Comment:"
      "Post)<-[:Comment_replyOf_Post|:Comment_replyOf_Comment]-(comment:"
      "Comment)-[:Comment_hasTag_Tag]->(tag2:Tag) "
      "WHERE id(tag1) <> id(tag2) "
      "RETURN count(*) AS count;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLSQBResource("LQ_5_logical"));
}

TEST_F(LSQBTest, LQ_6) {
  std::string query =
      "MATCH "
      "(person1:Person)-[:Person_knows_Person]->(person2:Person)-[:Person_"
      "knows_Person]->(person3:Person)-[:Person_hasInterest_Tag]->(tag:Tag) "
      "WHERE id(person1) <> id(person3) "
      "RETURN count(*) AS count;";
  auto logical = planLogical(query, schemaData, statsData, rules);
  VerifyFactory::verifyLogicalByStr(*logical, getLSQBResource("LQ_6_logical"));
}

}  // namespace gopt
}  // namespace neug
