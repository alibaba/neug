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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <cppjieba/DictTrie.hpp>
#include <cppjieba/HMMModel.hpp>
#include <cppjieba/HMMSegment.hpp>
#include <cppjieba/MPSegment.hpp>
#include <cppjieba/MixSegment.hpp>

namespace neug::fts_ext {

class SQLiteConnection;

enum class JiebaMode { kMp, kHmm, kMix };

using FTS5TokenCallback = int (*)(void*, int, const char*, int, int, int);

using FTSTokenizerConfig = std::unordered_map<std::string, std::string>;

class FTSTokenizer {
 public:
  virtual ~FTSTokenizer() = default;

  static std::shared_ptr<const FTSTokenizer> Create(FTSTokenizerConfig config,
                                                    std::string& full_name);

  virtual std::string_view Name() const noexcept = 0;
  virtual void Register(SQLiteConnection& connection) const = 0;
  virtual int Tokenize(void* context, const char* text, int text_size,
                       int flags, FTS5TokenCallback emit) const = 0;

 protected:
  std::shared_ptr<const FTSTokenizer> base_tokenizer_;
};

class StopwordFTSTokenizer final : public FTSTokenizer {
 public:
  StopwordFTSTokenizer(FTSTokenizerConfig config, std::string& full_name);

  std::string_view Name() const noexcept override { return "stopwords"; }
  void Register(SQLiteConnection& connection) const override;
  int Tokenize(void* context, const char* text, int text_size, int flags,
               FTS5TokenCallback emit) const override;

 private:
  void LoadStopwords(std::string_view stopwords);

  // Persistently owns the stopword dictionary strings; stopwords_ stores
  // string views into this storage for lookup.
  std::vector<std::string> stopword_storage_;
  std::unordered_set<std::string_view> stopwords_;
};

class BuiltinFTSTokenizer final : public FTSTokenizer {
 public:
  BuiltinFTSTokenizer(FTSTokenizerConfig config, std::string& full_name,
                      std::string tokenizer_name);

  std::string_view Name() const noexcept override { return tokenizer_name_; }
  void Register(SQLiteConnection& connection) const override;
  int Tokenize(void* context, const char* text, int text_size, int flags,
               FTS5TokenCallback emit) const override;

 private:
  std::string tokenizer_name_;
};

class JiebaFTSTokenizer final : public FTSTokenizer {
 public:
  JiebaFTSTokenizer(FTSTokenizerConfig config, std::string& full_name);

  JiebaFTSTokenizer(const JiebaFTSTokenizer&) = delete;
  JiebaFTSTokenizer& operator=(const JiebaFTSTokenizer&) = delete;
  JiebaFTSTokenizer(JiebaFTSTokenizer&&) = delete;
  JiebaFTSTokenizer& operator=(JiebaFTSTokenizer&&) = delete;

  std::string_view Name() const noexcept override { return "jieba"; }
  void Register(SQLiteConnection& connection) const override;
  int Tokenize(void* context, const char* text, int text_size, int flags,
               FTS5TokenCallback emit) const override;

 private:
  static JiebaMode ParseMode(FTSTokenizerConfig& config);
  static std::string ParseDict(FTSTokenizerConfig& config);

  JiebaMode mode_;

  // Keep dictionaries before segmenters so that they are destroyed last.
  cppjieba::DictTrie dict_trie_;
  cppjieba::HMMModel hmm_model_;
  cppjieba::MPSegment mp_segment_;
  cppjieba::HMMSegment hmm_segment_;
  cppjieba::MixSegment mix_segment_;
};

}  // namespace neug::fts_ext
