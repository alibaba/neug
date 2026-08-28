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

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <sqlite3.h>

#include "fts_index.h"
#include "fts_index_scan.h"
#include "neug/execution/expression/accessors/const_accessor.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/index/index_id_accessor.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column.h"

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#else
#include <unistd.h>
#endif

namespace neug::fts_ext {
namespace {

class NullStringColumn final : public ColumnBase {
 public:
  size_t size() const override { return 0; }
  void resize(size_t) override {}
  void resize(size_t, const Value&) override {}
  DataTypeId type() const override { return DataTypeId::kVarchar; }
  void set_any(size_t, const Value&, bool) override {}
  Value get_any(size_t) const override { return Value(); }
  void Open(Checkpoint&, const ModuleDescriptor&, MemoryLevel) override {}
  void Dump(Checkpoint&, CheckpointManifest&, const std::string&) override {}
  std::unique_ptr<Module> Clone() const override {
    return std::make_unique<NullStringColumn>();
  }
  void Detach(Checkpoint&, MemoryLevel) override {}
  std::string ModuleTypeName() const override { return "null_string_column"; }
};

NullStringColumn test_string_column;

class TemporaryDatabaseDirectory {
 public:
  TemporaryDatabaseDirectory() {
    const auto suffix =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("neug_fts_smoke_" + std::to_string(suffix));
  }

  ~TemporaryDatabaseDirectory() { std::filesystem::remove_all(path_); }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

class ScopedEnvironmentVariable {
 public:
  ScopedEnvironmentVariable(std::string name, const std::string& value)
      : name_(std::move(name)) {
    if (const char* previous = std::getenv(name_.c_str())) {
      previous_ = previous;
    }
    if (setenv(name_.c_str(), value.c_str(), 1) != 0) {
      throw std::runtime_error("Failed to set environment variable " + name_);
    }
  }

  ~ScopedEnvironmentVariable() {
    if (previous_) {
      setenv(name_.c_str(), previous_->c_str(), 1);
    } else {
      unsetenv(name_.c_str());
    }
  }

 private:
  std::string name_;
  std::optional<std::string> previous_;
};

class TestCheckpoint {
 public:
  explicit TestCheckpoint(const std::string& database_path) {
    manager_.Open(database_path);
    auto staging = manager_.CreateStaging();
    checkpoint_ = staging.checkpoint();
    staging.Discard();
  }

  Checkpoint& operator*() const { return *checkpoint_; }
  Checkpoint* operator->() const { return checkpoint_.get(); }

 private:
  CheckpointManager manager_;
  std::shared_ptr<Checkpoint> checkpoint_;
};

std::filesystem::path GetExecutablePath() {
#if defined(__APPLE__)
  uint32_t size = 0;
  _NSGetExecutablePath(nullptr, &size);
  std::string buffer(size, '\0');
  if (_NSGetExecutablePath(buffer.data(), &size) != 0) {
    return {};
  }
  return std::filesystem::canonical(buffer.c_str());
#else
  return std::filesystem::read_symlink("/proc/self/exe");
#endif
}

std::string FindBuildRoot() {
  auto directory = GetExecutablePath().parent_path();
  const auto extension_path =
      std::filesystem::path("extension/fts/libfts.neug_extension");
  for (int i = 0; i < 8; ++i) {
    if (std::filesystem::exists(directory / extension_path)) {
      return directory.string();
    }
    if (directory == directory.parent_path()) {
      break;
    }
    directory = directory.parent_path();
  }
  return "";
}

TEST(JiebaFTSTokenizerTest, LoadsBuiltInDictsFromMemory) {
  TemporaryDatabaseDirectory temporary_directory;
  const ScopedEnvironmentVariable temp_root(
      "NEUG_DB_TMP_DIR", temporary_directory.path().string());

  JiebaFTSTokenizer tokenizer(JiebaMode::kMix);

  EXPECT_FALSE(std::filesystem::exists(temporary_directory.path()));
}

TEST(FTSExtensionTest, LoadSucceeds) {
  const auto build_root = FindBuildRoot();
  ASSERT_FALSE(build_root.empty());
  ASSERT_EQ(setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1), 0);

  TemporaryDatabaseDirectory database_directory;
  neug::NeugDB database;
  ASSERT_TRUE(database.Open(database_directory.path()));
  auto connection = database.Connect();
  ASSERT_NE(connection, nullptr);

  auto load = connection->Query("LOAD fts;");
  ASSERT_TRUE(load.has_value()) << load.error().ToString();
}

TEST(FTSExtensionTest, JiebaOptionSupportsChineseSearch) {
  const auto build_root = FindBuildRoot();
  ASSERT_FALSE(build_root.empty());
  ASSERT_EQ(setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1), 0);

  TemporaryDatabaseDirectory database_directory;
  NeugDB database;
  ASSERT_TRUE(database.Open(database_directory.path()));
  auto connection = database.Connect();
  ASSERT_NE(connection, nullptr);
  ASSERT_TRUE(connection->Query("LOAD fts;").has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, "
                          "text STRING);")
                  .has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE (:Item {id: 1, "
                          "text: '我来到北京清华大学'}), "
                          "(:Item {id: 2, text: '上海交通大学'});")
                  .has_value());
  auto create = connection->Query(
      "CREATE INDEX item_text_fts ON Item USING FTS (text) "
      "WITH (tokenizer = 'jieba', jieba_mode = 'mix');");
  ASSERT_TRUE(create.has_value()) << create.error().ToString();

  auto result = connection->Query(
      "MATCH (n:Item) RETURN n.id, bm25(n.text, '清华大学') AS score "
      "ORDER BY score ASC LIMIT 10;");
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ASSERT_EQ(result->length(), 1);
  EXPECT_EQ(result->response().arrays(0).int64_array().values(0), 1);
}

TEST(FTSIndexScanInputTest, BindsConstantQueryExpression) {
  FTSIndexScanFuncInput input;
  input.property_names = {"text"};
  input.query_string =
      std::make_unique<execution::ConstExpr>(Value::STRING("search terms"));

  auto bound = input.bindParams({});
  const auto* fts_input =
      dynamic_cast<const FTSIndexScanFuncInput*>(bound.get());
  ASSERT_NE(fts_input, nullptr);
  EXPECT_EQ(fts_input->bound_query_string.GetValue<std::string>(),
            "search terms");
}

TEST(FTSIndexScanInputTest, BindsAndValidatesDynamicQueryParameter) {
  FTSIndexScanFuncInput input;
  input.property_names = {"text"};
  input.query_string = std::make_unique<execution::ParamExpr>(
      "query", DataType(DataTypeId::kVarchar));

  EXPECT_THROW(input.bindParams({}), exception::InvalidArgumentException);
  EXPECT_THROW(
      input.bindParams({{"query", Value(DataType(DataTypeId::kVarchar))}}),
      exception::InvalidArgumentException);
  EXPECT_THROW(input.bindParams({{"query", Value::INT64(42)}}),
               exception::InvalidArgumentException);

  auto first = input.bindParams({{"query", Value::STRING("search terms")}});
  const auto* first_input =
      dynamic_cast<const FTSIndexScanFuncInput*>(first.get());
  ASSERT_NE(first_input, nullptr);
  EXPECT_EQ(first_input->bound_query_string.GetValue<std::string>(),
            "search terms");

  auto second = input.bindParams({{"query", Value::STRING("other terms")}});
  const auto* second_input =
      dynamic_cast<const FTSIndexScanFuncInput*>(second.get());
  ASSERT_NE(second_input, nullptr);
  EXPECT_EQ(second_input->bound_query_string.GetValue<std::string>(),
            "other terms");
}

std::unique_ptr<FTSIndex> MakeOpenedIndex(
    Checkpoint& checkpoint, const std::string& tokenizer = "unicode61",
    const std::optional<std::string>& jieba_mode = std::nullopt) {
  auto meta = std::make_unique<IndexMeta>();
  meta->name = "item_text_fts";
  meta->type = "FTS";
  meta->schema.label_id = 0;
  meta->schema.columns.push_back({"text", DataType(DataTypeId::kVarchar)});
  if (tokenizer != "unicode61") {
    meta->options["tokenizer"] = tokenizer;
  }
  if (jieba_mode) {
    meta->options["jieba_mode"] = *jieba_mode;
  }
  auto index = std::make_unique<FTSIndex>();
  auto status =
      index->Init(std::move(meta), std::make_unique<DefaultIndexIDAccessor>());
  EXPECT_TRUE(status.ok()) << status.error_message();
  status = index->Rebind(IndexBindContext{{&test_string_column}});
  EXPECT_TRUE(status.ok()) << status.error_message();
  index->Open(checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);
  return index;
}

IndexValue MakeTextIndexValue(Value value) {
  return IndexValue{0, std::move(value)};
}

struct CollectedToken {
  std::string text;
  int start;
  int end;
};

int CollectToken(void* context, int, const char* token, int token_size,
                 int start, int end) {
  auto* tokens = static_cast<std::vector<CollectedToken>*>(context);
  tokens->push_back(CollectedToken{std::string(token, token_size), start, end});
  return SQLITE_OK;
}

std::unique_ptr<FTSIndex> MakeUnopenedIndex(
    const std::string& name = "item_text_fts") {
  auto meta = std::make_unique<IndexMeta>();
  meta->name = name;
  meta->type = "FTS";
  meta->schema.label_id = 0;
  meta->schema.columns.push_back({"text", DataType(DataTypeId::kVarchar)});
  auto index = std::make_unique<FTSIndex>();
  auto status =
      index->Init(std::move(meta), std::make_unique<DefaultIndexIDAccessor>());
  EXPECT_TRUE(status.ok()) << status.error_message();
  status = index->Rebind(IndexBindContext{{&test_string_column}});
  EXPECT_TRUE(status.ok()) << status.error_message();
  return index;
}

FTSQueryParams MakeQuery(std::string query,
                         std::optional<uint64_t> limit = std::nullopt) {
  FTSQueryParams params;
  params.query_string = std::move(query);
  params.property_names = {"text"};
  params.weights["text"] = 1.0;
  params.limit = limit;
  return params;
}

TEST(JiebaFTSTokenizerTest, SupportsMpHmmAndMixModes) {
  struct ModeExpectation {
    JiebaMode mode;
    std::vector<std::string> expected;
  };
  const std::vector<ModeExpectation> cases = {
      {JiebaMode::kMp, {"他", "来到", "了", "网易", "杭", "研", "大厦"}},
      {JiebaMode::kHmm, {"他来", "到", "了", "网易", "杭", "研大厦"}},
      {JiebaMode::kMix, {"他", "来到", "了", "网易", "杭研", "大厦"}},
  };

  const std::string input = "他来到了网易杭研大厦";
  for (const auto& test_case : cases) {
    JiebaFTSTokenizer tokenizer(test_case.mode);
    std::vector<CollectedToken> tokens;
    ASSERT_EQ(tokenizer.Tokenize(&tokens, input.data(), input.size(),
                                 FTS5_TOKENIZE_DOCUMENT, CollectToken),
              SQLITE_OK);
    std::vector<std::string> actual;
    for (const auto& token : tokens) {
      actual.push_back(token.text);
      EXPECT_EQ(input.substr(token.start, token.end - token.start), token.text);
    }
    EXPECT_EQ(actual, test_case.expected);
  }
}

TEST(JiebaFTSTokenizerTest, AddsCustomDictToBuiltInDict) {
  TemporaryDatabaseDirectory directory;
  std::filesystem::create_directories(directory.path());
  const auto dict_path = directory.path() / "user.dict.utf8";
  std::ofstream(dict_path) << "棉花糖星球\n";

  auto tokenizer = FTSTokenizer::Create({{"tokenizer", "jieba"},
                                         {"jieba_mode", "mp"},
                                         {"jieba_dict", dict_path.string()}});
  const std::string custom_word = "棉花糖星球";
  std::vector<CollectedToken> custom_tokens;
  ASSERT_EQ(tokenizer->Tokenize(&custom_tokens, custom_word.data(),
                                custom_word.size(), FTS5_TOKENIZE_DOCUMENT,
                                CollectToken),
            SQLITE_OK);
  ASSERT_EQ(custom_tokens.size(), 1u);
  EXPECT_EQ(custom_tokens.front().text, custom_word);

  const std::string built_in_word = "网易";
  std::vector<CollectedToken> built_in_tokens;
  ASSERT_EQ(tokenizer->Tokenize(&built_in_tokens, built_in_word.data(),
                                built_in_word.size(), FTS5_TOKENIZE_DOCUMENT,
                                CollectToken),
            SQLITE_OK);
  ASSERT_EQ(built_in_tokens.size(), 1u);
  EXPECT_EQ(built_in_tokens.front().text, built_in_word);
}

TEST(JiebaFTSTokenizerTest, RejectsInvalidCustomDictPath) {
  TemporaryDatabaseDirectory directory;
  const auto dict_path = directory.path() / "missing.dict.utf8";

  EXPECT_THROW(FTSTokenizer::Create({{"tokenizer", "jieba"},
                                     {"jieba_dict", dict_path.string()}}),
               std::invalid_argument);
}

TEST(JiebaFTSTokenizerTest, RejectsUserDictPathSeparators) {
  TemporaryDatabaseDirectory directory;
  std::filesystem::create_directories(directory.path());

  for (const char separator : {'|', ';'}) {
    const auto dict_path =
        directory.path() / ("user" + std::string(1, separator) + "dict.utf8");
    std::ofstream(dict_path) << "棉花糖星球\n";

    EXPECT_THROW(FTSTokenizer::Create({{"tokenizer", "jieba"},
                                       {"jieba_dict", dict_path.string()}}),
                 std::invalid_argument)
        << separator;
  }
}

TEST(JiebaFTSTokenizerTest, NormalizesAsciiAndSkipsPunctuation) {
  JiebaFTSTokenizer tokenizer(JiebaMode::kMix);
  const std::string input = "NeuG，是图数据库！";
  std::vector<CollectedToken> tokens;
  ASSERT_EQ(tokenizer.Tokenize(&tokens, input.data(), input.size(),
                               FTS5_TOKENIZE_DOCUMENT, CollectToken),
            SQLITE_OK);
  std::vector<std::string> actual;
  for (const auto& token : tokens) {
    actual.push_back(token.text);
  }
  EXPECT_EQ(actual, (std::vector<std::string>{"neug", "是", "图", "数据库"}));
}

TEST(JiebaFTSTokenizerTest, PreservesFullwidthLettersAndDigits) {
  JiebaFTSTokenizer tokenizer(JiebaMode::kMix);
  const std::string input = "ＡＢＣ１２３，。！？";
  std::vector<CollectedToken> tokens;
  ASSERT_EQ(tokenizer.Tokenize(&tokens, input.data(), input.size(),
                               FTS5_TOKENIZE_DOCUMENT, CollectToken),
            SQLITE_OK);

  std::string actual;
  for (const auto& token : tokens) {
    actual += token.text;
  }
  EXPECT_EQ(actual, "ＡＢＣ１２３");
}

TEST(JiebaFTSTokenizerTest, SupportsConcurrentReadOnlyTokenization) {
  auto tokenizer = std::make_shared<const JiebaFTSTokenizer>(JiebaMode::kMix);
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int thread = 0; thread < 8; ++thread) {
    threads.emplace_back([&] {
      const std::string input = "NeuG是一个高性能图数据库";
      for (int iteration = 0; iteration < 100; ++iteration) {
        std::vector<CollectedToken> tokens;
        if (tokenizer->Tokenize(&tokens, input.data(), input.size(),
                                FTS5_TOKENIZE_DOCUMENT,
                                CollectToken) != SQLITE_OK ||
            tokens.empty()) {
          ++failures;
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(failures.load(), 0);
}

TEST(FTSIndexTest, JiebaIndexesAndSearchesChineseText) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeOpenedIndex(*checkpoint, "jieba", "mix");
  ASSERT_TRUE(
      index->Upsert(7, MakeTextIndexValue(Value::STRING("我来到北京清华大学")))
          .ok());
  ASSERT_TRUE(
      index->Upsert(8, MakeTextIndexValue(Value::STRING("上海交通大学"))).ok());

  for (const auto& query : {"北京", "清华大学"}) {
    auto result = index->Search(MakeQuery(query));
    ASSERT_TRUE(result.has_value()) << result.error().ToString();
    ASSERT_EQ(result->size(), 1u) << query;
    EXPECT_EQ(result->front().vid, 7u) << query;
  }
  auto missing = index->Search(MakeQuery("广州"));
  ASSERT_TRUE(missing.has_value()) << missing.error().ToString();
  EXPECT_TRUE(missing->empty());
}

TEST(FTSIndexTest, SearchSupportsWordsPhrasesAndPrefixes) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeOpenedIndex(*checkpoint);
  auto first =
      index->Upsert(7, MakeTextIndexValue(Value::STRING("quick brown fox")));
  ASSERT_TRUE(first.ok()) << first.error_message();
  auto second =
      index->Upsert(3, MakeTextIndexValue(Value::STRING("quick blue hare")));
  ASSERT_TRUE(second.ok()) << second.error_message();
  auto third =
      index->Upsert(9, MakeTextIndexValue(Value::STRING("slow brown bear")));
  ASSERT_TRUE(third.ok()) << third.error_message();

  const std::vector<std::pair<std::string, std::vector<vid_t>>> cases = {
      {"quick", {7, 3}},
      {"\"quick brown\"", {7}},
      {"bro*", {7, 9}},
      {"missing", {}}};
  for (const auto& [query, expected] : cases) {
    auto params = MakeQuery(query);
    auto results = index->Search(params);
    ASSERT_TRUE(results.has_value())
        << query << ": " << results.error().ToString();
    ASSERT_EQ(results->size(), expected.size()) << query;
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(results->at(i).vid, expected[i]) << query;
      EXPECT_LE(results->at(i).score, 0.0);
    }
  }

  for (const auto& query : {"", "   "}) {
    auto results = index->Search(MakeQuery(query));
    ASSERT_FALSE(results.has_value());
    EXPECT_EQ(results.error().error_code(), StatusCode::ERR_QUERY_EXECUTION);
  }

  auto unterminated_quote = index->Search(MakeQuery("unterminated\""));
  ASSERT_FALSE(unterminated_quote.has_value());
  EXPECT_EQ(unterminated_quote.error().error_code(),
            StatusCode::ERR_QUERY_EXECUTION);
  EXPECT_NE(unterminated_quote.error().ToString().find("FTS query failed"),
            std::string::npos);

  auto dotted = index->Search(MakeQuery("quick.brown"));
  ASSERT_FALSE(dotted.has_value());
  EXPECT_EQ(dotted.error().error_code(), StatusCode::ERR_QUERY_EXECUTION);
  const auto dotted_error = dotted.error().ToString();
  EXPECT_NE(dotted_error.find("FTS query failed"), std::string::npos);
  EXPECT_NE(dotted_error.find("unquoted character '.'"), std::string::npos);
  EXPECT_NE(dotted_error.find("position 5"), std::string::npos);
  EXPECT_NE(dotted_error.find(
                "wrap the query in double quotes to form a phrase or escape "
                "the character"),
            std::string::npos);
  EXPECT_EQ(dotted_error.find("example:"), std::string::npos);
}

TEST(FTSExtensionTest, FusedTopKQueryReturnsNodesAndScores) {
  const auto build_root = FindBuildRoot();
  ASSERT_FALSE(build_root.empty());
  ASSERT_EQ(setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1), 0);

  TemporaryDatabaseDirectory database_directory;
  NeugDB database;
  ASSERT_TRUE(database.Open(database_directory.path()));
  auto connection = database.Connect();
  ASSERT_NE(connection, nullptr);
  ASSERT_TRUE(connection->Query("LOAD fts;").has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, "
                          "text STRING);")
                  .has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE (:Item {id: 1, text: 'search text alpha'}), "
                          "(:Item {id: 2, text: 'search text beta'}), "
                          "(:Item {id: 3, text: 'gamma'});")
                  .has_value());
  auto create_index =
      connection->Query("CREATE INDEX item_text_fts ON Item USING FTS (text);");
  ASSERT_TRUE(create_index.has_value()) << create_index.error().ToString();

  auto result = connection->Query(
      "MATCH (n:Item) "
      "RETURN n.id, bm25(n.text, 'search text') AS score "
      "ORDER BY score ASC LIMIT 2;");
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ASSERT_EQ(result->length(), 2);
  ASSERT_EQ(result->response().arrays_size(), 2);
  EXPECT_EQ(result->response().arrays(0).int64_array().values(0), 1);
  EXPECT_EQ(result->response().arrays(0).int64_array().values(1), 2);
  EXPECT_LE(result->response().arrays(1).double_array().values(0), 0.0);
  EXPECT_LE(result->response().arrays(1).double_array().values(1), 0.0);
  auto explain = connection->Query(
      "EXPLAIN MATCH (n:Item) "
      "RETURN n.id, bm25(n.text, 'search text') AS score "
      "ORDER BY score ASC LIMIT 2;");
  ASSERT_TRUE(explain.has_value()) << explain.error().ToString();
  const auto plan_text = explain->profile_result_text();
  EXPECT_NE(plan_text.find("IndexScanOpr"), std::string::npos);
  EXPECT_EQ(plan_text.find("OrderByOpr"), std::string::npos);
  EXPECT_EQ(plan_text.find("LimitOpr"), std::string::npos);

  const std::vector<std::string> query_literals = {"''", "'   '"};
  for (const auto& query_literal : query_literals) {
    auto invalid_query = connection->Query(
        "MATCH (n:Item) RETURN n.id, bm25(n.text, " + query_literal +
        ") AS score ORDER BY score ASC LIMIT 2;");
    EXPECT_FALSE(invalid_query.has_value()) << query_literal;
  }
  auto invalid_match = connection->Query(
      "MATCH (n:Item) RETURN n.id, "
      "bm25(n.text, \"a 'quoted' phrase\") AS score "
      "ORDER BY score ASC LIMIT 2;");
  EXPECT_FALSE(invalid_match.has_value());
}

TEST(FTSExtensionTest, PrimaryKeyRangeFilterUsesFTSIndexScan) {
  const auto build_root = FindBuildRoot();
  ASSERT_FALSE(build_root.empty());
  ASSERT_EQ(setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1), 0);

  TemporaryDatabaseDirectory database_directory;
  NeugDB database;
  ASSERT_TRUE(database.Open(database_directory.path()));
  auto connection = database.Connect();
  ASSERT_NE(connection, nullptr);
  ASSERT_TRUE(connection->Query("LOAD fts;").has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, "
                          "text STRING);")
                  .has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE (:Item {id: 1, text: 'search alpha'}), "
                          "(:Item {id: 2, text: 'search beta'}), "
                          "(:Item {id: 3, text: 'gamma'});")
                  .has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE INDEX item_text_fts ON Item USING FTS "
                          "(text);")
                  .has_value());

  auto result = connection->Query(
      "MATCH (n:Item) WHERE n.id > 1 "
      "RETURN n.id, bm25(n.text, 'search') AS score "
      "ORDER BY score ASC LIMIT 10;");
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ASSERT_EQ(result->length(), 1);
  EXPECT_EQ(result->response().arrays(0).int64_array().values(0), 2);
}

TEST(FTSExtensionTest, ReopenPreservesIndexAndAcceptsNewRows) {
  const auto build_root = FindBuildRoot();
  ASSERT_FALSE(build_root.empty());
  ASSERT_EQ(setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1), 0);
  TemporaryDatabaseDirectory database_directory;

  {
    NeugDB database;
    ASSERT_TRUE(database.Open(database_directory.path()));
    auto connection = database.Connect();
    ASSERT_TRUE(connection->Query("LOAD fts;").has_value());
    ASSERT_TRUE(connection
                    ->Query("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, "
                            "text STRING);")
                    .has_value());
    ASSERT_TRUE(
        connection->Query("CREATE (:Item {id: 1, text: 'durable fox'});")
            .has_value());
    ASSERT_TRUE(connection
                    ->Query("CREATE INDEX item_text_fts ON Item USING "
                            "FTS (text);")
                    .has_value());
    connection.reset();
    database.Close();
  }

  {
    NeugDB database;
    ASSERT_TRUE(database.Open(database_directory.path()));
    auto connection = database.Connect();
    ASSERT_TRUE(connection->Query("LOAD fts;").has_value());
    auto restored = connection->Query(
        "MATCH (n:Item) RETURN n.id, "
        "bm25(n.text, 'durable') AS score "
        "ORDER BY score ASC LIMIT 10;");
    ASSERT_TRUE(restored.has_value()) << restored.error().ToString();
    ASSERT_EQ(restored->length(), 1);
    EXPECT_EQ(restored->response().arrays(0).int64_array().values(0), 1);

    ASSERT_TRUE(
        connection->Query("CREATE (:Item {id: 2, text: 'durable hare'});")
            .has_value());
    auto appended = connection->Query(
        "MATCH (n:Item) RETURN n.id, "
        "bm25(n.text, 'durable') AS score "
        "ORDER BY score ASC LIMIT 10;");
    ASSERT_TRUE(appended.has_value()) << appended.error().ToString();
    EXPECT_EQ(appended->length(), 2);
  }
}

TEST(FTSExtensionTest, OrderByAndLimitAreIndependentAndUse64BitLimits) {
  const auto build_root = FindBuildRoot();
  ASSERT_FALSE(build_root.empty());
  ASSERT_EQ(setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1), 0);

  TemporaryDatabaseDirectory database_directory;
  NeugDB database;
  ASSERT_TRUE(database.Open(database_directory.path()));
  auto connection = database.Connect();
  ASSERT_NE(connection, nullptr);
  ASSERT_TRUE(connection->Query("LOAD fts;").has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, "
                          "text STRING);")
                  .has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE (:Item {id: 1, text: 'alpha alpha'}), "
                          "(:Item {id: 2, text: 'alpha beta beta'}), "
                          "(:Item {id: 3, text: 'alpha gamma gamma gamma'});")
                  .has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE INDEX item_text_fts ON Item USING "
                          "FTS (text);")
                  .has_value());

  const std::string prefix =
      "MATCH (n:Item) RETURN n.id, bm25(n.text, 'alpha') AS score";
  auto default_order = connection->Query(prefix + ";");
  ASSERT_TRUE(default_order.has_value()) << default_order.error().ToString();
  ASSERT_EQ(default_order->length(), 3);
  const auto& default_scores =
      default_order->response().arrays(1).double_array().values();
  EXPECT_LE(default_scores.Get(0), default_scores.Get(1));
  EXPECT_LE(default_scores.Get(1), default_scores.Get(2));

  auto asc = connection->Query(prefix + " ORDER BY score ASC;");
  ASSERT_TRUE(asc.has_value()) << asc.error().ToString();
  EXPECT_EQ(asc->length(), 3);

  auto desc = connection->Query(prefix + " ORDER BY score DESC;");
  ASSERT_TRUE(desc.has_value()) << desc.error().ToString();
  ASSERT_EQ(desc->length(), 3);
  const auto& desc_scores = desc->response().arrays(1).double_array().values();
  EXPECT_GE(desc_scores.Get(0), desc_scores.Get(1));
  EXPECT_GE(desc_scores.Get(1), desc_scores.Get(2));

  auto limit_only = connection->Query(prefix + " LIMIT 2;");
  ASSERT_TRUE(limit_only.has_value()) << limit_only.error().ToString();
  EXPECT_EQ(limit_only->length(), 2);
  auto desc_limit = connection->Query(prefix + " ORDER BY score DESC LIMIT 1;");
  ASSERT_TRUE(desc_limit.has_value()) << desc_limit.error().ToString();
  ASSERT_EQ(desc_limit->length(), 1);
  EXPECT_DOUBLE_EQ(desc_limit->response().arrays(1).double_array().values(0),
                   desc_scores.Get(0));

  auto zero = connection->Query(prefix + " LIMIT 0;");
  ASSERT_TRUE(zero.has_value()) << zero.error().ToString();
  EXPECT_EQ(zero->length(), 0);
  for (const auto* limit :
       {"4294967295", "4294967296", "9223372036854775807"}) {
    auto huge = connection->Query(prefix + " LIMIT " + limit + ";");
    ASSERT_TRUE(huge.has_value()) << limit << ": " << huge.error().ToString();
    EXPECT_EQ(huge->length(), 3) << limit;
  }

  auto wrong_type = connection->Query(
      "MATCH (n:Item) RETURN n.id, bm25(n.text, 42) AS score "
      "ORDER BY score ASC LIMIT 1;");
  EXPECT_FALSE(wrong_type.has_value());
}

TEST(FTSExtensionTest, MissingIndexReturnsError) {
  const auto build_root = FindBuildRoot();
  ASSERT_FALSE(build_root.empty());
  ASSERT_EQ(setenv("NEUG_EXTENSION_HOME_PYENV", build_root.c_str(), 1), 0);

  TemporaryDatabaseDirectory database_directory;
  NeugDB database;
  ASSERT_TRUE(database.Open(database_directory.path()));
  auto connection = database.Connect();
  ASSERT_NE(connection, nullptr);
  ASSERT_TRUE(connection->Query("LOAD fts;").has_value());
  ASSERT_TRUE(connection
                  ->Query("CREATE NODE TABLE Item(id INT64 PRIMARY KEY, "
                          "text STRING);")
                  .has_value());
  auto result = connection->Query(
      "MATCH (n:Item) "
      "RETURN n.id, bm25(n.text, 'alpha') AS score "
      "ORDER BY score ASC LIMIT 1;");
  ASSERT_FALSE(result.has_value());
  EXPECT_NE(result.error().ToString().find("FTS index not found"),
            std::string::npos);
}

TEST(FTSIndexTest, RejectsInvalidMetadataAndParams) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto meta = std::make_unique<IndexMeta>();
  meta->schema.columns.push_back({"text", DataType::INT64});
  FTSIndex index;
  auto status =
      index.Init(std::move(meta), std::make_unique<DefaultIndexIDAccessor>());
  ASSERT_TRUE(status.ok()) << status.error_message();
  EXPECT_ANY_THROW(
      index.Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory));

  auto valid_index = MakeOpenedIndex(*checkpoint);
  FTSQueryParams params;
  params.query_string = "alpha";
  params.limit = 0;
  auto result = valid_index->Search(params);
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  EXPECT_TRUE(result->empty());
  auto null_value = valid_index->Upsert(1, MakeTextIndexValue(Value()));
  EXPECT_TRUE(null_value.ok()) << null_value.error_message();
  ASSERT_TRUE(
      valid_index->Upsert(1, MakeTextIndexValue(Value::STRING("defined token")))
          .ok());
  auto defined = valid_index->Search(MakeQuery("defined"));
  ASSERT_TRUE(defined.has_value()) << defined.error().ToString();
  ASSERT_EQ(defined->size(), 1u);
  ASSERT_TRUE(valid_index->Upsert(1, MakeTextIndexValue(Value())).ok());
  ASSERT_TRUE(valid_index->Upsert(1, MakeTextIndexValue(Value())).ok());
  auto deleted_by_null = valid_index->Search(MakeQuery("defined"));
  ASSERT_TRUE(deleted_by_null.has_value())
      << deleted_by_null.error().ToString();
  EXPECT_TRUE(deleted_by_null->empty());
  auto wrong_type =
      valid_index->Upsert(2, MakeTextIndexValue(Value::INT64(42)));
  EXPECT_EQ(wrong_type.error_code(), StatusCode::ERR_INVALID_ARGUMENT);
}

TEST(FTSIndexTest, ValidatesNameAndFTSOptions) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());

  const std::vector<std::pair<std::string, std::pair<std::string, std::string>>>
      invalid_cases = {{"bad-name", {"", ""}},
                       {"valid_name", {"tokenizer", "unknown"}},
                       {"valid_name", {"Tokenizer", "unicode61"}},
                       {"valid_name", {"prefix", "2 bad"}},
                       {"valid_name", {"detail", "full"}},
                       {"valid_name", {"jieba_mode", "mix"}},
                       {"valid_name", {"jieba_dict", "user.dict.utf8"}},
                       {"valid_name", {"rank", "bm25"}}};
  for (const auto& [name, option] : invalid_cases) {
    auto index = MakeUnopenedIndex(name);
    if (!option.first.empty()) {
      const_cast<IndexMeta&>(index->GetMeta()).options[option.first] =
          option.second;
    }
    EXPECT_ANY_THROW(
        index->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory))
        << name << " " << option.first;
  }

  auto unsupported_search = MakeUnopenedIndex("jieba_search");
  auto& search_options =
      const_cast<IndexMeta&>(unsupported_search->GetMeta()).options;
  search_options["tokenizer"] = "jieba";
  search_options["jieba_mode"] = "search";
  EXPECT_ANY_THROW(unsupported_search->Open(*checkpoint, ModuleDescriptor{},
                                            MemoryLevel::kInMemory));

  auto valid = MakeUnopenedIndex("configured_fts");
  auto& options = const_cast<IndexMeta&>(valid->GetMeta()).options;
  options["tokenizer"] = "porter unicode61";
  options["prefix"] = "2 3";
  EXPECT_NO_THROW(
      valid->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory));

  const std::vector<std::pair<std::string, std::string>>
      parameterized_tokenizers = {
          {"unicode61_fts", "unicode61 remove_diacritics 2"},
          {"trigram_fts", "trigram case_sensitive 1"}};
  for (const auto& [name, tokenizer] : parameterized_tokenizers) {
    auto parameterized = MakeUnopenedIndex(name);
    const_cast<IndexMeta&>(parameterized->GetMeta()).options["tokenizer"] =
        tokenizer;
    EXPECT_NO_THROW(parameterized->Open(*checkpoint, ModuleDescriptor{},
                                        MemoryLevel::kInMemory))
        << tokenizer;
  }

  auto invalid_tokenizer_option =
      MakeUnopenedIndex("invalid_tokenizer_option_fts");
  auto& invalid_options =
      const_cast<IndexMeta&>(invalid_tokenizer_option->GetMeta()).options;
  invalid_options["tokenizer"] = "unicode61 unknown_option 1";
  EXPECT_ANY_THROW(invalid_tokenizer_option->Open(
      *checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory));
}

TEST(FTSIndexTest, FiltersSupersededAndDeletedRowsWithScores) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeOpenedIndex(*checkpoint);
  ASSERT_TRUE(
      index->Upsert(7, MakeTextIndexValue(Value::STRING("legacy token"))).ok());
  ASSERT_TRUE(
      index->Upsert(8, MakeTextIndexValue(Value::STRING("legacy token"))).ok());
  ASSERT_TRUE(
      index->Upsert(7, MakeTextIndexValue(Value::STRING("current token")))
          .ok());

  auto legacy = index->Search(MakeQuery("legacy"));
  ASSERT_TRUE(legacy.has_value()) << legacy.error().ToString();
  ASSERT_EQ(legacy->size(), 1);
  EXPECT_EQ(legacy->front().vid, 8u);

  ASSERT_TRUE(index->Delete(8).ok());
  auto after_delete = index->Search(MakeQuery("legacy"));
  ASSERT_TRUE(after_delete.has_value()) << after_delete.error().ToString();
  EXPECT_TRUE(after_delete->empty());

  auto current = index->Search(MakeQuery("current"));
  ASSERT_TRUE(current.has_value()) << current.error().ToString();
  ASSERT_EQ(current->size(), 1);
  EXPECT_EQ(current->front().vid, 7u);
  EXPECT_LE(current->front().score, 0.0);
}

TEST(FTSIndexTest, SearchesPastAnyNumberOfSupersededCandidates) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeOpenedIndex(*checkpoint);

  constexpr vid_t kSupersededCount = 256;
  for (vid_t vid = 0; vid < kSupersededCount; ++vid) {
    ASSERT_TRUE(
        index->Upsert(vid, MakeTextIndexValue(Value::STRING("legacy token")))
            .ok());
  }
  for (vid_t vid = 0; vid < kSupersededCount; ++vid) {
    ASSERT_TRUE(
        index->Upsert(vid, MakeTextIndexValue(Value::STRING("current token")))
            .ok());
  }
  ASSERT_TRUE(index
                  ->Upsert(kSupersededCount,
                           MakeTextIndexValue(Value::STRING("legacy token")))
                  .ok());

  auto result = index->Search(MakeQuery("legacy", 1));
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ(result->front().vid, kSupersededCount);
}

TEST(FTSIndexTest, CloneDetachIsolatesVisibilityAndSharesDatabase) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeOpenedIndex(*checkpoint);
  ASSERT_TRUE(
      index->Upsert(7, MakeTextIndexValue(Value::STRING("shared original")))
          .ok());

  auto cloned_module = index->Clone();
  auto clone = std::unique_ptr<FTSIndex>(
      static_cast<FTSIndex*>(cloned_module.release()));
  clone->Detach(*checkpoint, MemoryLevel::kInMemory);

  ASSERT_TRUE(
      clone->Upsert(8, MakeTextIndexValue(Value::STRING("clone only"))).ok());
  auto clone_only = clone->Search(MakeQuery("clone"));
  ASSERT_TRUE(clone_only.has_value()) << clone_only.error().ToString();
  ASSERT_EQ(clone_only->size(), 1u);
  EXPECT_EQ(clone_only->front().vid, 8u);

  auto original_clone_query = index->Search(MakeQuery("clone"));
  ASSERT_TRUE(original_clone_query.has_value())
      << original_clone_query.error().ToString();
  EXPECT_TRUE(original_clone_query->empty());

  ASSERT_TRUE(
      index->Upsert(9, MakeTextIndexValue(Value::STRING("original only")))
          .ok());
  auto original_only = index->Search(MakeQuery("original"));
  ASSERT_TRUE(original_only.has_value()) << original_only.error().ToString();
  ASSERT_EQ(original_only->size(), 2u);
  std::unordered_set<vid_t> original_vids;
  for (const auto& result : *original_only) {
    original_vids.insert(result.vid);
  }
  EXPECT_EQ(original_vids, (std::unordered_set<vid_t>{7u, 9u}));

  auto clone_original_query = clone->Search(MakeQuery("original"));
  ASSERT_TRUE(clone_original_query.has_value())
      << clone_original_query.error().ToString();
  ASSERT_EQ(clone_original_query->size(), 1u);
  EXPECT_EQ(clone_original_query->front().vid, 7u);

  ASSERT_TRUE(clone->Delete(7).ok());
  auto clone_after_delete = clone->Search(MakeQuery("shared"));
  ASSERT_TRUE(clone_after_delete.has_value())
      << clone_after_delete.error().ToString();
  EXPECT_TRUE(clone_after_delete->empty());
  auto original_after_clone_delete = index->Search(MakeQuery("shared"));
  ASSERT_TRUE(original_after_clone_delete.has_value())
      << original_after_clone_delete.error().ToString();
  ASSERT_EQ(original_after_clone_delete->size(), 1u);
  EXPECT_EQ(original_after_clone_delete->front().vid, 7u);

  // Destroying one side must not close the SQLite connection shared by clones.
  index.reset();
  clone_only = clone->Search(MakeQuery("clone"));
  ASSERT_TRUE(clone_only.has_value()) << clone_only.error().ToString();
  ASSERT_EQ(clone_only->size(), 1u);
  EXPECT_EQ(clone_only->front().vid, 8u);
}

TEST(FTSIndexTest, EmptyIndexOpenAndDump) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeUnopenedIndex();
  index->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);

  CheckpointManifest manifest;
  index->Dump(*checkpoint, manifest, "index_item_text_fts");
  const auto* descriptor = manifest.FindModule("index_item_text_fts");
  ASSERT_NE(descriptor, nullptr);
  auto path = descriptor->get_path("fts_file");
  ASSERT_TRUE(path.has_value());
  EXPECT_TRUE(std::filesystem::is_regular_file(*path));
  std::ifstream file(*path, std::ios::binary);
  ASSERT_TRUE(file.is_open());
  char header[16]{};
  file.read(header, sizeof(header));
  EXPECT_EQ(std::string(header, 15), "SQLite format 3");
}

TEST(FTSIndexTest, OuterReopenPreservesSearchAndAllowsAppend) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeUnopenedIndex();
  index->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);
  ASSERT_TRUE(
      index->Upsert(7, MakeTextIndexValue(Value::STRING("persisted fox")))
          .ok());

  CheckpointManifest manifest;
  index->Dump(*checkpoint, manifest, "index_item_text_fts");
  const auto* descriptor = manifest.FindModule("index_item_text_fts");
  ASSERT_NE(descriptor, nullptr);

  FTSIndex restored;
  restored.Open(*checkpoint, manifest, *descriptor, MemoryLevel::kInMemory);
  ASSERT_TRUE(restored.Rebind(IndexBindContext{{&test_string_column}}).ok());
  auto before_append = restored.Search(MakeQuery("persisted"));
  ASSERT_TRUE(before_append.has_value()) << before_append.error().ToString();
  ASSERT_EQ(before_append->size(), 1);
  EXPECT_EQ(before_append->front().vid, 7u);
  ASSERT_TRUE(
      restored.Upsert(8, MakeTextIndexValue(Value::STRING("persisted hare")))
          .ok());
  auto after_append = restored.Search(MakeQuery("persisted"));
  ASSERT_TRUE(after_append.has_value()) << after_append.error().ToString();
  ASSERT_EQ(after_append->size(), 2);
  CheckpointManifest restored_manifest;
  restored.Dump(*checkpoint, restored_manifest, "index_item_text_fts");
  const auto* restored_descriptor =
      restored_manifest.FindModule("index_item_text_fts");
  ASSERT_NE(restored_descriptor, nullptr);
  auto restored_path = restored_descriptor->get_path("fts_file");
  ASSERT_TRUE(restored_path.has_value());
  EXPECT_TRUE(std::filesystem::is_regular_file(*restored_path));
}

TEST(FTSIndexTest, JiebaModePersistsAcrossDumpAndReopen) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeUnopenedIndex("jieba_persisted_fts");
  auto& options = const_cast<IndexMeta&>(index->GetMeta()).options;
  options["tokenizer"] = "jieba";
  options["jieba_mode"] = "mp";
  index->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);
  ASSERT_TRUE(
      index->Upsert(7, MakeTextIndexValue(Value::STRING("我来到北京清华大学")))
          .ok());

  CheckpointManifest manifest;
  index->Dump(*checkpoint, manifest, "index_jieba_persisted_fts");
  const auto* descriptor = manifest.FindModule("index_jieba_persisted_fts");
  ASSERT_NE(descriptor, nullptr);

  FTSIndex restored;
  restored.Open(*checkpoint, manifest, *descriptor, MemoryLevel::kInMemory);
  EXPECT_EQ(restored.GetMeta().options.at("tokenizer"), "jieba");
  EXPECT_EQ(restored.GetMeta().options.at("jieba_mode"), "mp");
  auto result = restored.Search(MakeQuery("清华大学"));
  ASSERT_TRUE(result.has_value()) << result.error().ToString();
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ(result->front().vid, 7u);
}

TEST(FTSIndexTest, JiebaDictPathPersistsAsAbsolutePath) {
  TemporaryDatabaseDirectory directory;
  std::filesystem::create_directories(directory.path());
  const auto dict_path = directory.path() / "user.dict.utf8";
  std::ofstream(dict_path) << "棉花糖星球\n";
  const auto relative_dict_path =
      std::filesystem::relative(dict_path, std::filesystem::current_path());

  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeUnopenedIndex("jieba_dict_path_fts");
  auto& options = const_cast<IndexMeta&>(index->GetMeta()).options;
  options["tokenizer"] = "jieba";
  options["jieba_dict"] = relative_dict_path.string();
  index->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);

  const auto persisted_dict_path =
      std::filesystem::path(index->GetMeta().options.at("jieba_dict"));
  EXPECT_TRUE(persisted_dict_path.is_absolute());
  EXPECT_EQ(std::filesystem::canonical(persisted_dict_path),
            std::filesystem::canonical(dict_path));

  CheckpointManifest manifest;
  index->Dump(*checkpoint, manifest, "index_jieba_dict_path_fts");
  const auto* descriptor = manifest.FindModule("index_jieba_dict_path_fts");
  ASSERT_NE(descriptor, nullptr);

  FTSIndex restored;
  restored.Open(*checkpoint, manifest, *descriptor, MemoryLevel::kInMemory);
  EXPECT_EQ(restored.GetMeta().options.at("jieba_dict"),
            persisted_dict_path.string());
}

TEST(FTSIndexTest, MissingPersistedFileFailsOpen) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeUnopenedIndex();
  index->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);
  CheckpointManifest manifest;
  index->Dump(*checkpoint, manifest, "index_item_text_fts");
  const auto* descriptor = manifest.FindModule("index_item_text_fts");
  ASSERT_NE(descriptor, nullptr);
  auto path = descriptor->get_path("fts_file");
  ASSERT_TRUE(path.has_value());

  index.reset();
  ASSERT_TRUE(std::filesystem::remove(*path));
  FTSIndex missing;
  try {
    missing.Open(*checkpoint, manifest, *descriptor, MemoryLevel::kInMemory);
    FAIL() << "Expected a missing persisted FTS file to fail";
  } catch (const exception::CheckpointException& error) {
    EXPECT_NE(
        std::string(error.what()).find("Persisted FTS index file is missing"),
        std::string::npos);
    EXPECT_NE(std::string(error.what()).find(*path), std::string::npos);
  }
}

TEST(FTSIndexTest, CorruptPersistedSQLiteFileFailsOpen) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeUnopenedIndex();
  index->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);
  CheckpointManifest manifest;
  index->Dump(*checkpoint, manifest, "index_item_text_fts");
  const auto* descriptor = manifest.FindModule("index_item_text_fts");
  ASSERT_NE(descriptor, nullptr);
  auto path = descriptor->get_path("fts_file");
  ASSERT_TRUE(path.has_value());
  index.reset();

  std::ofstream corrupt(*path, std::ios::binary | std::ios::trunc);
  corrupt << "not a sqlite database";
  corrupt.close();
  FTSIndex restored;
  EXPECT_ANY_THROW(restored.Open(*checkpoint, manifest, *descriptor,
                                 MemoryLevel::kInMemory));
}

TEST(FTSIndexTest, PersistedSQLiteWithoutFTSTableFailsOpen) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto index = MakeUnopenedIndex();
  index->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);
  CheckpointManifest manifest;
  index->Dump(*checkpoint, manifest, "index_item_text_fts");
  const auto* descriptor = manifest.FindModule("index_item_text_fts");
  ASSERT_NE(descriptor, nullptr);
  auto path = descriptor->get_path("fts_file");
  ASSERT_TRUE(path.has_value());
  index.reset();

  ASSERT_TRUE(std::filesystem::remove(*path));
  SQLiteConnection sqlite;
  sqlite.Open(*path);
  sqlite.Execute("CREATE TABLE unrelated(value INTEGER);");
  sqlite.Close();
  FTSIndex restored;
  EXPECT_ANY_THROW(restored.Open(*checkpoint, manifest, *descriptor,
                                 MemoryLevel::kInMemory));
}

TEST(FTSIndexTest, MultipleIndexesUseIsolatedFiles) {
  TemporaryDatabaseDirectory directory;
  TestCheckpoint checkpoint(directory.path().string());
  auto first = MakeUnopenedIndex("first_fts");
  auto second = MakeUnopenedIndex("second_fts");
  first->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);
  second->Open(*checkpoint, ModuleDescriptor{}, MemoryLevel::kInMemory);

  CheckpointManifest manifest;
  first->Dump(*checkpoint, manifest, "index_first_fts");
  second->Dump(*checkpoint, manifest, "index_second_fts");
  const auto* first_descriptor = manifest.FindModule("index_first_fts");
  const auto* second_descriptor = manifest.FindModule("index_second_fts");
  ASSERT_NE(first_descriptor, nullptr);
  ASSERT_NE(second_descriptor, nullptr);
  auto first_path = first_descriptor->get_path("fts_file");
  auto second_path = second_descriptor->get_path("fts_file");
  ASSERT_TRUE(first_path.has_value());
  ASSERT_TRUE(second_path.has_value());
  EXPECT_NE(*first_path, *second_path);
  EXPECT_TRUE(std::filesystem::is_regular_file(*first_path));
  EXPECT_TRUE(std::filesystem::is_regular_file(*second_path));
}

}  // namespace
}  // namespace neug::fts_ext
