// Minimal dynamic-size bitset, a drop-in replacement for the small subset of
// boost::dynamic_bitset<> used by the vendored DAF backtracking code
// (failing-set propagation). Supports exactly the operations DAF needs:
// default construction, resize (preserve-on-grow, zero-fill new bits, matching
// boost), set(i), reset() (clear all), test(i), copy assignment, |=, and |.
// Header-only and dependency-free so the pattern_matching extension no longer
// requires Boost.
#ifndef NEUG_PATTERN_MATCHING_UTIL_DYNAMIC_BITSET_H_
#define NEUG_PATTERN_MATCHING_UTIL_DYNAMIC_BITSET_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace neug {
namespace pm {

class DynamicBitset {
 public:
  DynamicBitset() = default;
  explicit DynamicBitset(std::size_t num_bits) { resize(num_bits); }

  // Grows/shrinks to `num_bits`. Existing bits are preserved and any newly
  // exposed bits are zero, matching boost::dynamic_bitset::resize default.
  void resize(std::size_t num_bits) {
    words_.resize(WordCount(num_bits), 0ULL);
    num_bits_ = num_bits;
  }

  void set(std::size_t i) { words_[i >> 6] |= (1ULL << (i & 63)); }

  // No-argument reset clears every bit (boost semantics).
  void reset() { std::fill(words_.begin(), words_.end(), 0ULL); }

  bool test(std::size_t i) const { return (words_[i >> 6] >> (i & 63)) & 1ULL; }

  std::size_t size() const { return num_bits_; }

  DynamicBitset& operator|=(const DynamicBitset& other) {
    if (other.words_.size() > words_.size()) {
      words_.resize(other.words_.size(), 0ULL);
      num_bits_ = std::max(num_bits_, other.num_bits_);
    }
    for (std::size_t i = 0; i < other.words_.size(); ++i) {
      words_[i] |= other.words_[i];
    }
    return *this;
  }

  friend DynamicBitset operator|(DynamicBitset lhs, const DynamicBitset& rhs) {
    lhs |= rhs;
    return lhs;
  }

 private:
  static std::size_t WordCount(std::size_t num_bits) {
    return (num_bits + 63) / 64;
  }

  std::size_t num_bits_ = 0;
  std::vector<uint64_t> words_;
};

}  // namespace pm
}  // namespace neug

#endif  // NEUG_PATTERN_MATCHING_UTIL_DYNAMIC_BITSET_H_
