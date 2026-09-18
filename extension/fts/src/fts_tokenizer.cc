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

#include <glog/logging.h>
#include <sqlite3.h>
#include <zlib.h>

#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "fts_sqlite.h"

namespace neug::fts_ext {
namespace {

#include "../dict/hmm_model_zlib.inc"
#include "../dict/jieba_dict_small_zlib.inc"
#include "../dict/jieba_stopwords_zlib.inc"

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
constexpr JiebaDictFile kJiebaStopwords{
    "stop_words.utf8", kJiebaStopwordsCompressed,
    sizeof(kJiebaStopwordsCompressed), kJiebaStopwordsCompressedOriginalSize};
constexpr std::string_view kEnglishStopwords[] = {
    "a",
    "a's",
    "able",
    "about",
    "above",
    "according",
    "accordingly",
    "across",
    "actually",
    "after",
    "afterwards",
    "again",
    "against",
    "ain't",
    "all",
    "allow",
    "allows",
    "almost",
    "alone",
    "along",
    "already",
    "also",
    "although",
    "always",
    "am",
    "among",
    "amongst",
    "an",
    "and",
    "another",
    "any",
    "anybody",
    "anyhow",
    "anyone",
    "anything",
    "anyway",
    "anyways",
    "anywhere",
    "apart",
    "appear",
    "appreciate",
    "appropriate",
    "are",
    "aren't",
    "around",
    "as",
    "aside",
    "ask",
    "asking",
    "associated",
    "at",
    "available",
    "away",
    "awfully",
    "b",
    "be",
    "became",
    "because",
    "become",
    "becomes",
    "becoming",
    "been",
    "before",
    "beforehand",
    "behind",
    "being",
    "believe",
    "below",
    "beside",
    "besides",
    "best",
    "better",
    "between",
    "beyond",
    "both",
    "brief",
    "but",
    "by",
    "c",
    "c'mon",
    "c's",
    "came",
    "can",
    "can't",
    "cannot",
    "cant",
    "cause",
    "causes",
    "certain",
    "certainly",
    "changes",
    "clearly",
    "co",
    "com",
    "come",
    "comes",
    "concerning",
    "consequently",
    "consider",
    "considering",
    "contain",
    "containing",
    "contains",
    "corresponding",
    "could",
    "couldn't",
    "course",
    "currently",
    "d",
    "definitely",
    "described",
    "despite",
    "did",
    "didn't",
    "different",
    "do",
    "does",
    "doesn't",
    "doing",
    "don't",
    "done",
    "down",
    "downwards",
    "during",
    "e",
    "each",
    "edu",
    "eg",
    "eight",
    "either",
    "else",
    "elsewhere",
    "enough",
    "entirely",
    "especially",
    "et",
    "etc",
    "even",
    "ever",
    "every",
    "everybody",
    "everyone",
    "everything",
    "everywhere",
    "ex",
    "exactly",
    "example",
    "except",
    "f",
    "far",
    "few",
    "fifth",
    "first",
    "five",
    "followed",
    "following",
    "follows",
    "for",
    "former",
    "formerly",
    "forth",
    "four",
    "from",
    "further",
    "furthermore",
    "g",
    "get",
    "gets",
    "getting",
    "given",
    "gives",
    "go",
    "goes",
    "going",
    "gone",
    "got",
    "gotten",
    "greetings",
    "h",
    "had",
    "hadn't",
    "happens",
    "hardly",
    "has",
    "hasn't",
    "have",
    "haven't",
    "having",
    "he",
    "he's",
    "hello",
    "help",
    "hence",
    "her",
    "here",
    "here's",
    "hereafter",
    "hereby",
    "herein",
    "hereupon",
    "hers",
    "herself",
    "hi",
    "him",
    "himself",
    "his",
    "hither",
    "hopefully",
    "how",
    "howbeit",
    "however",
    "i",
    "i'd",
    "i'll",
    "i'm",
    "i've",
    "ie",
    "if",
    "ignored",
    "immediate",
    "in",
    "inasmuch",
    "inc",
    "indeed",
    "indicate",
    "indicated",
    "indicates",
    "inner",
    "insofar",
    "instead",
    "into",
    "inward",
    "is",
    "isn't",
    "it",
    "it'd",
    "it'll",
    "it's",
    "its",
    "itself",
    "j",
    "just",
    "k",
    "keep",
    "keeps",
    "kept",
    "know",
    "knows",
    "known",
    "l",
    "last",
    "lately",
    "later",
    "latter",
    "latterly",
    "least",
    "less",
    "lest",
    "let",
    "let's",
    "like",
    "liked",
    "likely",
    "little",
    "look",
    "looking",
    "looks",
    "ltd",
    "m",
    "mainly",
    "many",
    "may",
    "maybe",
    "me",
    "mean",
    "meanwhile",
    "merely",
    "might",
    "more",
    "moreover",
    "most",
    "mostly",
    "much",
    "must",
    "my",
    "myself",
    "n",
    "name",
    "namely",
    "nd",
    "near",
    "nearly",
    "necessary",
    "need",
    "needs",
    "neither",
    "never",
    "nevertheless",
    "new",
    "next",
    "nine",
    "no",
    "nobody",
    "non",
    "none",
    "noone",
    "nor",
    "normally",
    "not",
    "nothing",
    "novel",
    "now",
    "nowhere",
    "o",
    "obviously",
    "of",
    "off",
    "often",
    "oh",
    "ok",
    "okay",
    "old",
    "on",
    "once",
    "one",
    "ones",
    "only",
    "onto",
    "or",
    "other",
    "others",
    "otherwise",
    "ought",
    "our",
    "ours",
    "ourselves",
    "out",
    "outside",
    "over",
    "overall",
    "own",
    "p",
    "particular",
    "particularly",
    "per",
    "perhaps",
    "placed",
    "please",
    "plus",
    "possible",
    "presumably",
    "probably",
    "provides",
    "q",
    "que",
    "quite",
    "qv",
    "r",
    "rather",
    "rd",
    "re",
    "really",
    "reasonably",
    "regarding",
    "regardless",
    "regards",
    "relatively",
    "respectively",
    "right",
    "s",
    "said",
    "same",
    "saw",
    "say",
    "saying",
    "says",
    "second",
    "secondly",
    "see",
    "seeing",
    "seem",
    "seemed",
    "seeming",
    "seems",
    "seen",
    "self",
    "selves",
    "sensible",
    "sent",
    "serious",
    "seriously",
    "seven",
    "several",
    "shall",
    "she",
    "should",
    "shouldn't",
    "since",
    "six",
    "so",
    "some",
    "somebody",
    "somehow",
    "someone",
    "something",
    "sometime",
    "sometimes",
    "somewhat",
    "somewhere",
    "soon",
    "sorry",
    "specified",
    "specify",
    "specifying",
    "still",
    "sub",
    "such",
    "sup",
    "sure",
    "t",
    "t's",
    "take",
    "taken",
    "tell",
    "tends",
    "th",
    "than",
    "thank",
    "thanks",
    "thanx",
    "that",
    "that's",
    "thats",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "thence",
    "there",
    "there's",
    "thereafter",
    "thereby",
    "therefore",
    "therein",
    "theres",
    "thereupon",
    "these",
    "they",
    "they'd",
    "they'll",
    "they're",
    "they've",
    "think",
    "third",
    "this",
    "thorough",
    "thoroughly",
    "those",
    "though",
    "three",
    "through",
    "throughout",
    "thru",
    "thus",
    "to",
    "together",
    "too",
    "took",
    "toward",
    "towards",
    "tried",
    "tries",
    "truly",
    "try",
    "trying",
    "twice",
    "two",
    "u",
    "un",
    "under",
    "unfortunately",
    "unless",
    "unlikely",
    "until",
    "unto",
    "up",
    "upon",
    "us",
    "use",
    "used",
    "useful",
    "uses",
    "using",
    "usually",
    "uucp",
    "v",
    "value",
    "various",
    "very",
    "via",
    "viz",
    "vs",
    "w",
    "want",
    "wants",
    "was",
    "wasn't",
    "way",
    "we",
    "we'd",
    "we'll",
    "we're",
    "we've",
    "welcome",
    "well",
    "went",
    "were",
    "weren't",
    "what",
    "what's",
    "whatever",
    "when",
    "whence",
    "whenever",
    "where",
    "where's",
    "whereafter",
    "whereas",
    "whereby",
    "wherein",
    "whereupon",
    "wherever",
    "whether",
    "which",
    "while",
    "whither",
    "who",
    "who's",
    "whoever",
    "whole",
    "whom",
    "whose",
    "why",
    "will",
    "willing",
    "wish",
    "with",
    "within",
    "without",
    "won't",
    "wonder",
    "would",
    "wouldn't",
    "x",
    "y",
    "yes",
    "yet",
    "you",
    "you'd",
    "you'll",
    "you're",
    "you've",
    "your",
    "yours",
    "yourself",
    "yourselves",
    "z",
    "zero",
};

struct StopwordTokenizerContext {
  fts5_api* api{};
  const std::unordered_set<std::string>* stopwords{};

  static void Destroy(void* context) noexcept {
    delete static_cast<StopwordTokenizerContext*>(context);
  }
};

struct StopwordTokenizer {
  fts5_tokenizer_v2 base_api{};
  Fts5Tokenizer* base_tokenizer{};
  const std::unordered_set<std::string>* stopwords{};
};

struct StopwordFilterContext {
  void* output_context;
  FTS5TokenCallback emit;
  const std::unordered_set<std::string>* stopwords;
};

int StopwordTokenFilter(void* context, int token_flags, const char* token,
                        int token_size, int start, int end) noexcept {
  const auto* filter = static_cast<const StopwordFilterContext*>(context);
  try {
    if (filter->stopwords->contains(std::string(token, token_size))) {
      return SQLITE_OK;
    }
    return filter->emit(filter->output_context, token_flags, token, token_size,
                        start, end);
  } catch (const std::bad_alloc&) { return SQLITE_NOMEM; } catch (...) {
    return SQLITE_ERROR;
  }
}

void StopwordTokenizerDelete(Fts5Tokenizer* tokenizer) noexcept {
  auto* stopword_tokenizer = reinterpret_cast<StopwordTokenizer*>(tokenizer);
  if (stopword_tokenizer->base_tokenizer != nullptr) {
    stopword_tokenizer->base_api.xDelete(stopword_tokenizer->base_tokenizer);
  }
  delete stopword_tokenizer;
}

int StopwordTokenizerCreate(void* context, const char** arguments,
                            int argument_count,
                            Fts5Tokenizer** output) noexcept {
  const auto* tokenizer_context =
      static_cast<const StopwordTokenizerContext*>(context);
  void* base_context = nullptr;
  fts5_tokenizer_v2* base_api = nullptr;
  auto code = tokenizer_context->api->xFindTokenizer_v2(
      tokenizer_context->api, arguments[0], &base_context, &base_api);
  if (code != SQLITE_OK) {
    return code;
  }

  auto* tokenizer = new (std::nothrow) StopwordTokenizer();
  if (tokenizer == nullptr) {
    return SQLITE_NOMEM;
  }
  tokenizer->base_api = *base_api;
  tokenizer->stopwords = tokenizer_context->stopwords;
  code = tokenizer->base_api.xCreate(base_context, arguments + 1,
                                     argument_count - 1,
                                     &tokenizer->base_tokenizer);
  if (code != SQLITE_OK) {
    StopwordTokenizerDelete(reinterpret_cast<Fts5Tokenizer*>(tokenizer));
    return code;
  }
  *output = reinterpret_cast<Fts5Tokenizer*>(tokenizer);
  return SQLITE_OK;
}

int StopwordTokenizerTokenize(Fts5Tokenizer* tokenizer, void* context,
                              int flags, const char* text, int text_size,
                              const char* locale, int locale_size,
                              FTS5TokenCallback emit) noexcept {
  auto* stopword_tokenizer = reinterpret_cast<StopwordTokenizer*>(tokenizer);
  StopwordFilterContext filter_context{context, emit,
                                       stopword_tokenizer->stopwords};
  return stopword_tokenizer->base_api.xTokenize(
      stopword_tokenizer->base_tokenizer, &filter_context, flags, text,
      text_size, locale, locale_size, StopwordTokenFilter);
}

void RegisterStopwordTokenizer(
    SQLiteConnection& connection,
    const std::unordered_set<std::string>& stopwords) {
  auto* api = connection.GetFTS5API();

  auto context = std::make_unique<StopwordTokenizerContext>();
  context->api = api;
  context->stopwords = &stopwords;

  static fts5_tokenizer_v2 tokenizer_api{2, StopwordTokenizerCreate,
                                         StopwordTokenizerDelete,
                                         StopwordTokenizerTokenize};
  const auto create_code = api->xCreateTokenizer_v2(
      api, BuiltinFTSTokenizer::kName.data(), context.get(), &tokenizer_api,
      StopwordTokenizerContext::Destroy);
  if (create_code != SQLITE_OK) {
    throw std::runtime_error("Failed to register stopword FTS5 tokenizer: " +
                             std::string(sqlite3_errstr(create_code)));
  }
  context.release();
}

bool IsPunctuationRune(cppjieba::Rune rune) {
  if (rune < 0x80) {
    const auto character = static_cast<unsigned char>(rune);
    return std::isspace(character) || std::ispunct(character);
  }
  return (rune >= 0x2000 && rune <= 0x206F) ||
         (rune >= 0x3000 && rune <= 0x303F) ||
         (rune >= 0xFE10 && rune <= 0xFE1F) ||
         (rune >= 0xFE30 && rune <= 0xFE4F) ||
         (rune >= 0xFF01 && rune <= 0xFF0F) ||
         (rune >= 0xFF1A && rune <= 0xFF20) ||
         (rune >= 0xFF3B && rune <= 0xFF40) ||
         (rune >= 0xFF5B && rune <= 0xFF65);
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

std::unordered_set<std::string> ParseStopwordList(std::string_view input) {
  const auto invalid = [](std::string_view reason) {
    throw std::invalid_argument("Cannot parse stopwords: " +
                                std::string(reason));
  };
  std::istringstream stream{std::string(input)};
  char delimiter;
  if (!(stream >> delimiter) || delimiter != '[') {
    invalid("expected '[' at the beginning.");
  }
  std::unordered_set<std::string> stopwords;
  stream >> std::ws;
  while (stream.peek() != ']') {
    if (stream.peek() == std::char_traits<char>::eof()) {
      invalid("expected ']' at the end.");
    }
    const auto quote = static_cast<char>(stream.peek());
    if (quote != '\'' && quote != '"') {
      invalid("expected a quoted word.");
    }
    std::string stopword;
    if (!(stream >> std::quoted(stopword, quote))) {
      invalid("unterminated quoted string.");
    }
    if (stopword.empty()) {
      invalid("stopwords cannot be empty.");
    }
    LowercaseASCII(stopword);
    stopwords.emplace(std::move(stopword));
    stream >> std::ws;
    if (stream.peek() != ']' && (!(stream >> delimiter) || delimiter != ',')) {
      invalid("expected words to be separated by ','.");
    }
    stream >> std::ws;
  }
  stream.get();
  stream >> std::ws;
  if (stream.peek() != std::char_traits<char>::eof()) {
    invalid("unexpected characters after ']'.");
  }
  return stopwords;
}

std::string DecompressJiebaDict(const JiebaDictFile& dict) {
  std::string contents(dict.original_size, '\0');
  uLongf contents_size = contents.size();
  const auto result =
      uncompress(reinterpret_cast<Bytef*>(contents.data()), &contents_size,
                 dict.compressed_data, dict.compressed_size);
  if (result != Z_OK || contents_size != dict.original_size) {
    throw std::runtime_error("Failed to decompress embedded Jieba dictionary " +
                             std::string(dict.filename));
  }
  return contents;
}

std::string ResolveJiebaUserDictPath(std::string path) {
  if (path.empty()) {
    return path;
  }
  if (path.find_first_of("|;") != std::string::npos) {
    throw std::invalid_argument(
        "Jieba user dictionary path must not contain '|' or ';': " + path);
  }

  std::error_code error;
  const bool is_regular_file = std::filesystem::is_regular_file(path, error);
  if (!is_regular_file) {
    auto message = "Jieba dictionary is not a regular file: " + path;
    if (error) {
      message += ": " + error.message();
    }
    throw std::invalid_argument(std::move(message));
  }
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::invalid_argument("Jieba dictionary is not readable: " + path);
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

int JiebaCreate(void* context, const char**, int argument_count,
                Fts5Tokenizer** output) noexcept {
  if (context == nullptr || output == nullptr || argument_count != 0) {
    return SQLITE_ERROR;
  }
  *output = reinterpret_cast<Fts5Tokenizer*>(context);
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
  } catch (const std::bad_alloc&) {
    return SQLITE_NOMEM;
  } catch (const std::exception& error) {
    LOG(ERROR) << "Jieba tokenization failed: " << error.what();
    return SQLITE_ERROR;
  } catch (...) {
    LOG(ERROR) << "Jieba tokenization failed with an unknown exception";
    return SQLITE_ERROR;
  }
}

}  // namespace

void FTSTokenizer::LoadStopwords(std::string_view stopwords) {
  stopwords_.clear();
  if (stopwords == "none") {
    return;
  }
  if (stopwords == "english") {
    for (const auto stopword : kEnglishStopwords) {
      stopwords_.emplace(stopword);
    }
    return;
  }
  if (stopwords == "jieba") {
    std::istringstream stream(DecompressJiebaDict(kJiebaStopwords));
    for (std::string stopword; std::getline(stream, stopword);) {
      if (!stopword.empty() && stopword.back() == '\r') {
        stopword.pop_back();
      }
      if (!stopword.empty()) {
        stopwords_.emplace(std::move(stopword));
      }
    }
    return;
  }
  stopwords_ = ParseStopwordList(stopwords);
}

void BuiltinFTSTokenizer::Register(SQLiteConnection& connection) const {
  RegisterStopwordTokenizer(connection, stopwords_);
}

BuiltinFTSTokenizer::BuiltinFTSTokenizer(std::string builtin_name)
    : builtin_name_(std::move(builtin_name)),
      full_name_(std::string(kName) + " " + builtin_name_) {}

int BuiltinFTSTokenizer::Tokenize(void*, const char*, int, int,
                                  FTS5TokenCallback) const {
  return SQLITE_ERROR;
}

JiebaFTSTokenizer::JiebaFTSTokenizer(JiebaMode mode, std::string jieba_dict)
    : mode_(mode),
      dict_trie_(std::istringstream(DecompressJiebaDict(kJiebaDict)),
                 ResolveJiebaUserDictPath(std::move(jieba_dict))),
      hmm_model_(std::istringstream(DecompressJiebaDict(kJiebaHmmModel))),
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
    if (stopwords_.contains(word.word)) {
      continue;
    }
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
  // Use English stopwords by default.
  std::string stopword_option = "english";
  if (auto option = config.find("stopwords"); option != config.end()) {
    stopword_option = std::move(option->second);
    config.erase(option);
  }
  // Use SQLite's unicode61 tokenizer by default.
  std::string name = "unicode61";
  if (auto option = config.find("tokenizer"); option != config.end()) {
    name = std::move(option->second);
    config.erase(option);
  }

  static const std::unordered_set<std::string> builtin_names = {
      "unicode61", "ascii", "porter", "trigram"};
  const auto base_name = name.substr(0, name.find(' '));

  std::shared_ptr<FTSTokenizer> tokenizer;
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
  tokenizer->LoadStopwords(stopword_option);
  return tokenizer;
}

}  // namespace neug::fts_ext
