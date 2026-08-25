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

#include "fts_tokenizer.h"

#include <sqlite3.h>
#include <zlib.h>

#include <atomic>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "fts_sqlite.h"

#if defined(__APPLE__) || defined(__linux__)
#include <dlfcn.h>
#include <unistd.h>
#endif

namespace neug::fts_ext {
namespace {

#include "../dict/hmm_model_zlib.inc"
#include "../dict/jieba_dict_small_zlib.inc"

struct JiebaDictFile {
  std::string_view filename;
  const unsigned char* compressed_data;
  size_t compressed_size;
  size_t original_size;
};

constexpr JiebaDictFile kJiebaDict{"jieba.dict.utf8", kJiebaDictCompressed,
                                   sizeof(kJiebaDictCompressed),
                                   kJiebaDictCompressedOriginalSize};
constexpr JiebaDictFile kJiebaHmmModel{"hmm_model.utf8", kHmmModelCompressed,
                                       sizeof(kHmmModelCompressed),
                                       kHmmModelCompressedOriginalSize};

bool IsPunctuationRune(cppjieba::Rune rune) {
  if (rune < 0x80) {
    const auto character = static_cast<unsigned char>(rune);
    return std::isspace(character) || std::ispunct(character);
  }
  return (rune >= 0x2000 && rune <= 0x206F) ||
         (rune >= 0x3000 && rune <= 0x303F) ||
         (rune >= 0xFE10 && rune <= 0xFE1F) ||
         (rune >= 0xFE30 && rune <= 0xFE4F) ||
         (rune >= 0xFF00 && rune <= 0xFF65);
}

bool ShouldIgnore(const std::string& token) {
  cppjieba::RuneStrArray runes;
  if (token.empty() || !cppjieba::DecodeUTF8RunesInString(token, runes)) {
    return true;
  }
  for (const auto& rune : runes) {
    if (!IsPunctuationRune(rune.rune)) {
      return false;
    }
  }
  return true;
}

void LowercaseASCII(std::string& token) {
  for (auto& character : token) {
    const auto byte = static_cast<unsigned char>(character);
    if (byte < 0x80) {
      character = static_cast<char>(std::tolower(byte));
    }
  }
}

std::filesystem::path ResolveJiebaDictDirectory() {
#if defined(__APPLE__) || defined(__linux__)
  static const int dict_anchor = 0;
  Dl_info info{};
  if (dladdr(static_cast<const void*>(&dict_anchor), &info) == 0 ||
      info.dli_fname == nullptr) {
    throw std::runtime_error(
        "Failed to locate libfts.neug_extension for Jieba dictionaries");
  }
  return std::filesystem::path(info.dli_fname).parent_path();
#else
  throw std::runtime_error(
      "Locating bundled Jieba dictionaries is unsupported on this platform");
#endif
}

void ExtractJiebaDict(const std::filesystem::path& path,
                      const JiebaDictFile& dict) {
  try {
    std::vector<unsigned char> contents(dict.original_size);
    uLongf contents_size = contents.size();
    uncompress(contents.data(), &contents_size, dict.compressed_data,
               dict.compressed_size);
    static std::atomic<uint64_t> temporary_file_id{0};
    auto temporary_path = path;
#if defined(__APPLE__) || defined(__linux__)
    temporary_path += ".tmp." + std::to_string(getpid()) + "." +
                      std::to_string(temporary_file_id.fetch_add(1));
#endif
    std::ofstream output;
    output.exceptions(std::ios::failbit | std::ios::badbit);
    output.open(temporary_path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(contents.data()),
                 contents.size());
    output.close();
    std::filesystem::rename(temporary_path, path);
  } catch (const std::exception& error) {
    throw std::runtime_error("Failed to extract Jieba dictionary " +
                             path.string() + ": " + error.what());
  }
}

void ValidateJiebaDictPath(const std::filesystem::path& path) {
  std::error_code error;
  const bool is_regular_file = std::filesystem::is_regular_file(path, error);
  if (!is_regular_file) {
    auto message = "Jieba dictionary is not a regular file: " + path.string();
    if (error) {
      message += ": " + error.message();
    }
    throw std::invalid_argument(std::move(message));
  }
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::invalid_argument("Jieba dictionary is not readable: " +
                                path.string());
  }
}

std::string ResolveJiebaDictPath(const JiebaDictFile& dict) {
  const auto path = ResolveJiebaDictDirectory() / dict.filename;
  std::error_code error;
  if (!std::filesystem::is_regular_file(path, error) || error) {
    ExtractJiebaDict(path, dict);
  }
  ValidateJiebaDictPath(path);
  return path.string();
}

std::string ResolveJiebaUserDictPath(std::string path) {
  if (!path.empty()) {
    ValidateJiebaDictPath(path);
  }
  return path;
}

JiebaMode ParseJiebaMode(const std::optional<std::string>& mode) {
  if (!mode || *mode == "mix") {
    return JiebaMode::kMix;
  }
  if (*mode == "mp") {
    return JiebaMode::kMp;
  }
  if (*mode == "hmm") {
    return JiebaMode::kHmm;
  }
  throw std::invalid_argument("Invalid jieba_mode: " + *mode +
                              "; expected mp, hmm, or mix");
}

int JiebaCreate(void* user_data, const char**, int argument_count,
                Fts5Tokenizer** output) noexcept {
  if (user_data == nullptr || output == nullptr || argument_count != 0) {
    return SQLITE_ERROR;
  }
  *output = reinterpret_cast<Fts5Tokenizer*>(user_data);
  return SQLITE_OK;
}

void JiebaDelete(Fts5Tokenizer*) noexcept {}

int JiebaTokenize(Fts5Tokenizer* tokenizer, void* context, int flags,
                  const char* text, int text_size, const char*, int,
                  FTS5TokenCallback emit) noexcept {
  if (tokenizer == nullptr || text == nullptr || text_size < 0 ||
      emit == nullptr) {
    return SQLITE_ERROR;
  }
  try {
    const auto* fts_tokenizer =
        reinterpret_cast<const FTSTokenizer*>(tokenizer);
    return fts_tokenizer->Tokenize(context, text, text_size, flags, emit);
  } catch (const std::bad_alloc&) { return SQLITE_NOMEM; } catch (...) {
    return SQLITE_ERROR;
  }
}

}  // namespace

BuiltinFTSTokenizer::BuiltinFTSTokenizer(std::string name)
    : name_(std::move(name)) {}

void BuiltinFTSTokenizer::Register(SQLiteConnection&) const {}

int BuiltinFTSTokenizer::Tokenize(void*, const char*, int, int,
                                  FTS5TokenCallback) const {
  return SQLITE_ERROR;
}

JiebaFTSTokenizer::JiebaFTSTokenizer(JiebaMode mode, std::string jieba_dict)
    : mode_(mode),
      dict_trie_(ResolveJiebaDictPath(kJiebaDict),
                 ResolveJiebaUserDictPath(std::move(jieba_dict))),
      hmm_model_(ResolveJiebaDictPath(kJiebaHmmModel)),
      mp_segment_(&dict_trie_),
      hmm_segment_(&hmm_model_),
      mix_segment_(&dict_trie_, &hmm_model_) {}

int JiebaFTSTokenizer::Tokenize(void* context, const char* text, int text_size,
                                int flags, FTS5TokenCallback emit) const {
  static_cast<void>(flags);
  if (text == nullptr || text_size < 0 || emit == nullptr) {
    return SQLITE_ERROR;
  }

  std::string input(text, static_cast<size_t>(text_size));
  std::vector<cppjieba::Word> words;
  switch (mode_) {
  case JiebaMode::kMp:
    mp_segment_.Cut(input, words);
    break;
  case JiebaMode::kHmm:
    hmm_segment_.Cut(input, words);
    break;
  case JiebaMode::kMix:
    mix_segment_.Cut(input, words, true);
    break;
  }

  for (auto& word : words) {
    if (ShouldIgnore(word.word)) {
      continue;
    }
    LowercaseASCII(word.word);
    const auto start = static_cast<uint64_t>(word.offset);
    const auto end = start + word.word.size();
    if (start > static_cast<uint64_t>(std::numeric_limits<int>::max()) ||
        end > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
      return SQLITE_TOOBIG;
    }
    const auto code =
        emit(context, 0, word.word.data(), static_cast<int>(word.word.size()),
             static_cast<int>(start), static_cast<int>(end));
    if (code != SQLITE_OK) {
      return code;
    }
  }
  return SQLITE_OK;
}

void JiebaFTSTokenizer::Register(SQLiteConnection& connection) const {
  auto* api = connection.GetFTS5API();
  if (api->iVersion < 3 || api->xCreateTokenizer_v2 == nullptr) {
    throw std::runtime_error("SQLite FTS5 tokenizer v2 API is unavailable");
  }

  static fts5_tokenizer_v2 tokenizer_api{
      2,
      JiebaCreate,
      JiebaDelete,
      JiebaTokenize,
  };
  const auto code = api->xCreateTokenizer_v2(
      api, kName.data(), const_cast<JiebaFTSTokenizer*>(this), &tokenizer_api,
      nullptr);
  if (code != SQLITE_OK) {
    throw std::runtime_error("Failed to register Jieba FTS5 tokenizer: " +
                             std::string(sqlite3_errstr(code)));
  }
}

std::shared_ptr<const FTSTokenizer> FTSTokenizer::Create(
    FTSTokenizerConfig config) {
  std::string name;
  if (auto option = config.find("tokenizer"); option != config.end()) {
    name = option->second;
    config.erase(option);
  } else {
    // Use SQLite's unicode61 tokenizer by default.
    name = "unicode61";
  }

  static const std::unordered_set<std::string> builtin_names = {
      "unicode61", "ascii", "porter", "trigram"};
  const auto base_name = name.substr(0, name.find(' '));

  std::shared_ptr<const FTSTokenizer> tokenizer;
  if (builtin_names.contains(base_name)) {
    tokenizer = std::make_shared<BuiltinFTSTokenizer>(name);
  } else if (name == "jieba") {
    std::optional<std::string> mode;
    if (auto option = config.find("jieba_mode"); option != config.end()) {
      mode = std::move(option->second);
      config.erase(option);
    }
    std::string jieba_dict;
    if (auto option = config.find("jieba_dict"); option != config.end()) {
      jieba_dict = std::move(option->second);
      config.erase(option);
    }
    tokenizer = std::make_shared<JiebaFTSTokenizer>(ParseJiebaMode(mode),
                                                    std::move(jieba_dict));
  } else {
    throw std::invalid_argument("Unsupported FTS tokenizer: " + name);
  }

  if (!config.empty()) {
    throw std::invalid_argument("Unsupported parameter for tokenizer '" + name +
                                "': " + config.begin()->first);
  }
  return tokenizer;
}

}  // namespace neug::fts_ext
