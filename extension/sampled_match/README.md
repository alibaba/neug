# sampled_match Extension

This extension implements subgraph matching cardinality estimation based on the FaSTest algorithm described in:

**Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach** (VLDB 2024)

See the original project note in `include/fastest_lib/README.md`:
- `Fastest [VLDB 2024]`
- `Cardinality Estimation of Subgraph Matching: A Filtering-Sampling Approach`

## Build Test

From your build directory, compile tests with:

```sh
mkdir build && cd build
cmake .. -DBUILD_EXTENSIONS="sampled_match" -DBUILD_TEST=ON
make -j8 sampled_match_test
./extension/sampled_match/tests/sampled_match_test
```
