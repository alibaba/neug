/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include "leiden_algorithm.h"

using namespace neug::leiden;

// ---------------------------------------------------------------------------
// Test: Empty graph
// ---------------------------------------------------------------------------
TEST(LeidenTest, EmptyGraph) {
  LeidenGraph g;
  auto result = RunLeiden(g);
  EXPECT_EQ(result.communities.size(), 0u);
  EXPECT_EQ(result.num_communities, 0);
}

// ---------------------------------------------------------------------------
// Test: Single vertex
// ---------------------------------------------------------------------------
TEST(LeidenTest, SingleVertex) {
  LeidenGraph g;
  g.AddVertex(0);
  auto result = RunLeiden(g);
  EXPECT_EQ(result.communities.size(), 1u);
  EXPECT_EQ(result.num_communities, 1);
}

// ---------------------------------------------------------------------------
// Test: Two disconnected vertices
// ---------------------------------------------------------------------------
TEST(LeidenTest, TwoDisconnectedVertices) {
  LeidenGraph g;
  g.AddVertex(0);
  g.AddVertex(1);
  auto result = RunLeiden(g);
  EXPECT_EQ(result.communities.size(), 2u);
  EXPECT_NE(result.communities[0], result.communities[1]);
}

// ---------------------------------------------------------------------------
// Test: Two connected vertices (undirected)
// ---------------------------------------------------------------------------
TEST(LeidenTest, TwoConnectedUndirected) {
  LeidenGraph g;
  g.AddEdge(0, 1, 1.0, /*directed=*/false);
  auto result = RunLeiden(g, /*directed=*/false);
  EXPECT_EQ(result.communities.size(), 2u);
  EXPECT_EQ(result.communities[0], result.communities[1]);
}

// ---------------------------------------------------------------------------
// Test: Simple triangle (undirected)
// ---------------------------------------------------------------------------
TEST(LeidenTest, TriangleUndirected) {
  LeidenGraph g;
  g.AddEdge(0, 1, 1.0, false);
  g.AddEdge(1, 2, 1.0, false);
  g.AddEdge(2, 0, 1.0, false);
  auto result = RunLeiden(g, false);
  EXPECT_EQ(result.communities.size(), 3u);
  EXPECT_EQ(result.communities[0], result.communities[1]);
  EXPECT_EQ(result.communities[1], result.communities[2]);
}

// ---------------------------------------------------------------------------
// Test: Two cliques connected by a single edge (undirected)
// ---------------------------------------------------------------------------
TEST(LeidenTest, TwoCliquesUndirected) {
  LeidenGraph g;
  // Clique 1: vertices 0-3
  g.AddEdge(0, 1, 1.0, false);
  g.AddEdge(0, 2, 1.0, false);
  g.AddEdge(0, 3, 1.0, false);
  g.AddEdge(1, 2, 1.0, false);
  g.AddEdge(1, 3, 1.0, false);
  g.AddEdge(2, 3, 1.0, false);
  // Clique 2: vertices 4-7
  g.AddEdge(4, 5, 1.0, false);
  g.AddEdge(4, 6, 1.0, false);
  g.AddEdge(4, 7, 1.0, false);
  g.AddEdge(5, 6, 1.0, false);
  g.AddEdge(5, 7, 1.0, false);
  g.AddEdge(6, 7, 1.0, false);
  // Bridge
  g.AddEdge(3, 4, 1.0, false);

  auto result = RunLeiden(g, false);
  EXPECT_EQ(result.communities.size(), 8u);

  int64_t com0 = result.communities[0];
  EXPECT_EQ(result.communities[1], com0);
  EXPECT_EQ(result.communities[2], com0);
  EXPECT_EQ(result.communities[3], com0);

  int64_t com4 = result.communities[4];
  EXPECT_EQ(result.communities[5], com4);
  EXPECT_EQ(result.communities[6], com4);
  EXPECT_EQ(result.communities[7], com4);

  EXPECT_NE(com0, com4);
  EXPECT_EQ(result.num_communities, 2);
}

// ---------------------------------------------------------------------------
// Test: Directed graph - two strongly connected components
// ---------------------------------------------------------------------------
TEST(LeidenTest, DirectedTwoSCCs) {
  LeidenGraph g;
  // SCC 1: 0->1->2->0
  g.AddEdge(0, 1, 1.0, true);
  g.AddEdge(1, 2, 1.0, true);
  g.AddEdge(2, 0, 1.0, true);
  // SCC 2: 3->4->5->3
  g.AddEdge(3, 4, 1.0, true);
  g.AddEdge(4, 5, 1.0, true);
  g.AddEdge(5, 3, 1.0, true);
  // Bridge: 2->3
  g.AddEdge(2, 3, 1.0, true);

  auto result = RunLeiden(g, true);
  EXPECT_EQ(result.communities.size(), 6u);

  int64_t com0 = result.communities[0];
  EXPECT_EQ(result.communities[1], com0);
  EXPECT_EQ(result.communities[2], com0);

  int64_t com3 = result.communities[3];
  EXPECT_EQ(result.communities[4], com3);
  EXPECT_EQ(result.communities[5], com3);

  EXPECT_NE(com0, com3);
}

// ---------------------------------------------------------------------------
// Test: Weighted edges
// ---------------------------------------------------------------------------
TEST(LeidenTest, WeightedUndirected) {
  LeidenGraph g;
  // Strong clique 1
  g.AddEdge(0, 1, 10.0, false);
  g.AddEdge(0, 2, 10.0, false);
  g.AddEdge(1, 2, 10.0, false);
  // Strong clique 2
  g.AddEdge(3, 4, 10.0, false);
  g.AddEdge(3, 5, 10.0, false);
  g.AddEdge(4, 5, 10.0, false);
  // Weak bridge
  g.AddEdge(2, 3, 0.1, false);

  auto result = RunLeiden(g, false);
  EXPECT_EQ(result.communities.size(), 6u);
  EXPECT_EQ(result.num_communities, 2);

  int64_t com0 = result.communities[0];
  EXPECT_EQ(result.communities[1], com0);
  EXPECT_EQ(result.communities[2], com0);

  int64_t com3 = result.communities[3];
  EXPECT_EQ(result.communities[4], com3);
  EXPECT_EQ(result.communities[5], com3);
  EXPECT_NE(com0, com3);
}

// ---------------------------------------------------------------------------
// Test: Resolution parameter
// ---------------------------------------------------------------------------
TEST(LeidenTest, ResolutionParameter) {
  LeidenGraph g;
  g.AddEdge(0, 1, 1.0, false);
  g.AddEdge(0, 2, 1.0, false);
  g.AddEdge(1, 2, 1.0, false);
  g.AddEdge(3, 4, 1.0, false);
  g.AddEdge(3, 5, 1.0, false);
  g.AddEdge(4, 5, 1.0, false);
  g.AddEdge(2, 3, 1.0, false);

  auto result_low = RunLeiden(g, false, 0.1);
  auto result_high = RunLeiden(g, false, 10.0);

  EXPECT_GE(result_high.num_communities, result_low.num_communities);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
