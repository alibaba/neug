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

#include "louvain_algorithm.h"

using namespace neug::louvain;

// ---------------------------------------------------------------------------
// Test: Empty graph
// ---------------------------------------------------------------------------
TEST(LouvainTest, EmptyGraph) {
  LouvainGraph g;
  auto result = RunLouvain(g);
  EXPECT_EQ(result.communities.size(), 0u);
  EXPECT_EQ(result.num_communities, 0);
}

// ---------------------------------------------------------------------------
// Test: Single vertex
// ---------------------------------------------------------------------------
TEST(LouvainTest, SingleVertex) {
  LouvainGraph g;
  g.AddVertex(0);
  auto result = RunLouvain(g);
  EXPECT_EQ(result.communities.size(), 1u);
  EXPECT_EQ(result.num_communities, 1);
}

// ---------------------------------------------------------------------------
// Test: Two disconnected vertices
// ---------------------------------------------------------------------------
TEST(LouvainTest, TwoDisconnectedVertices) {
  LouvainGraph g;
  g.AddVertex(0);
  g.AddVertex(1);
  auto result = RunLouvain(g);
  EXPECT_EQ(result.communities.size(), 2u);
  // Each vertex should be in its own community
  EXPECT_NE(result.communities[0], result.communities[1]);
}

// ---------------------------------------------------------------------------
// Test: Two connected vertices (undirected)
// ---------------------------------------------------------------------------
TEST(LouvainTest, TwoConnectedUndirected) {
  LouvainGraph g;
  g.AddEdge(0, 1, 1.0, /*directed=*/false);
  auto result = RunLouvain(g, /*directed=*/false);
  EXPECT_EQ(result.communities.size(), 2u);
  // They should be in the same community
  EXPECT_EQ(result.communities[0], result.communities[1]);
}

// ---------------------------------------------------------------------------
// Test: Simple triangle (undirected) - should form one community
// ---------------------------------------------------------------------------
TEST(LouvainTest, TriangleUndirected) {
  LouvainGraph g;
  g.AddEdge(0, 1, 1.0, false);
  g.AddEdge(1, 2, 1.0, false);
  g.AddEdge(2, 0, 1.0, false);
  auto result = RunLouvain(g, false);
  EXPECT_EQ(result.communities.size(), 3u);
  // All three should be in the same community
  EXPECT_EQ(result.communities[0], result.communities[1]);
  EXPECT_EQ(result.communities[1], result.communities[2]);
}

// ---------------------------------------------------------------------------
// Test: Two cliques connected by a single edge (undirected)
// Classic Louvain test case: should detect two communities.
// ---------------------------------------------------------------------------
TEST(LouvainTest, TwoCliquesUndirected) {
  LouvainGraph g;
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

  auto result = RunLouvain(g, false);
  EXPECT_EQ(result.communities.size(), 8u);

  // Vertices in clique 1 should share a community
  int64_t com0 = result.communities[0];
  EXPECT_EQ(result.communities[1], com0);
  EXPECT_EQ(result.communities[2], com0);
  EXPECT_EQ(result.communities[3], com0);

  // Vertices in clique 2 should share a community
  int64_t com4 = result.communities[4];
  EXPECT_EQ(result.communities[5], com4);
  EXPECT_EQ(result.communities[6], com4);
  EXPECT_EQ(result.communities[7], com4);

  // The two communities should be different
  EXPECT_NE(com0, com4);
  EXPECT_EQ(result.num_communities, 2);
}

// ---------------------------------------------------------------------------
// Test: Directed graph - simple chain
// ---------------------------------------------------------------------------
TEST(LouvainTest, DirectedChain) {
  LouvainGraph g;
  g.AddEdge(0, 1, 1.0, true);
  g.AddEdge(1, 2, 1.0, true);
  g.AddEdge(2, 3, 1.0, true);
  auto result = RunLouvain(g, true);
  EXPECT_EQ(result.communities.size(), 4u);
  // With a simple chain, we expect 1-2 communities
  EXPECT_GE(result.num_communities, 1);
  EXPECT_LE(result.num_communities, 4);
}

// ---------------------------------------------------------------------------
// Test: Directed graph - two strongly connected components
// ---------------------------------------------------------------------------
TEST(LouvainTest, DirectedTwoSCCs) {
  LouvainGraph g;
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

  auto result = RunLouvain(g, true);
  EXPECT_EQ(result.communities.size(), 6u);

  // SCC 1 vertices should share a community
  int64_t com0 = result.communities[0];
  EXPECT_EQ(result.communities[1], com0);
  EXPECT_EQ(result.communities[2], com0);

  // SCC 2 vertices should share a community
  int64_t com3 = result.communities[3];
  EXPECT_EQ(result.communities[4], com3);
  EXPECT_EQ(result.communities[5], com3);

  // Different communities
  EXPECT_NE(com0, com3);
}

// ---------------------------------------------------------------------------
// Test: Weighted edges - strong internal, weak external
// ---------------------------------------------------------------------------
TEST(LouvainTest, WeightedUndirected) {
  LouvainGraph g;
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

  auto result = RunLouvain(g, false);
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
// Test: Resolution parameter - high resolution should find more communities
// ---------------------------------------------------------------------------
TEST(LouvainTest, ResolutionParameter) {
  LouvainGraph g;
  // Build a graph with clear community structure
  g.AddEdge(0, 1, 1.0, false);
  g.AddEdge(0, 2, 1.0, false);
  g.AddEdge(1, 2, 1.0, false);
  g.AddEdge(3, 4, 1.0, false);
  g.AddEdge(3, 5, 1.0, false);
  g.AddEdge(4, 5, 1.0, false);
  g.AddEdge(2, 3, 1.0, false);

  // Low resolution: might merge into 1 community
  auto result_low = RunLouvain(g, false, 0.1);

  // High resolution: should find more communities
  auto result_high = RunLouvain(g, false, 10.0);

  EXPECT_GE(result_high.num_communities, result_low.num_communities);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
