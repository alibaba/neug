# Performance Debugging with EXPLAIN and PROFILE

EXPLAIN and PROFILE are Cypher query execution commands that allow you to inspect query execution plans and collect performance metrics during query execution.

## Quick Reference

- **EXPLAIN**: View the execution plan without executing the query
- **PROFILE**: Execute the query and collect per-operator timing and row count statistics

## Overview

### EXPLAIN Mode

**EXPLAIN** shows the physical execution plan of your query without actually executing it. This is useful for:
- Understanding how the query optimizer plans your query
- Identifying suboptimal query structures before execution
- Debugging complex joins and aggregations

**Results**: Returns the operator tree structure with 0 data rows.

### PROFILE Mode

**PROFILE** executes your query and collects detailed per-operator metrics. This is useful for:
- Identifying performance bottlenecks in slow queries
- Comparing execution times across different query structures
- Understanding data distribution impact on execution

**Results**:
1. **Query data rows** - The actual result set from your query
2. **Operator tree structure** - The execution plan (same as EXPLAIN)
3. **Per-operator metrics** - Execution time and row count for each operator

## Examples

The examples below use the **ldbc_sample** dataset.

### PROFILE Examples

#### 1. Which posts attract the most discussion?

Find the top 10 posts by direct comment volume and identify their authors.

```cypher
PROFILE MATCH (c:comment)-[:commentReplyOfPost]->(p:post)-[:postHasCreator]->(author:person)
RETURN p.id,
       author.firstName,
       author.lastName,
       COUNT(c.id) AS direct_reply_count
ORDER BY direct_reply_count DESC
LIMIT 10;
```

Output:
```
+---------------+--------------------------+----------------------+----------------------+
|       _2_p.id | _6_author.firstName      | _6_author.lastName   |   direct_reply_count |
+===============+==========================+======================+======================+
| 1030792245903 | Bona Pinder Yayumayalolo | Collinet             |                   19 |
+---------------+--------------------------+----------------------+----------------------+
|  343597401905 | Abby                     | Hassan               |                   18 |
+---------------+--------------------------+----------------------+----------------------+
|  618475479931 | Hao                      | Liu                  |                   18 |
+---------------+--------------------------+----------------------+----------------------+
| 1030792175730 | Yang                     | Zhang                |                   17 |
+---------------+--------------------------+----------------------+----------------------+
|  343597520846 | Abdul-Malik              | Binalshibh           |                   17 |
+---------------+--------------------------+----------------------+----------------------+
|  893353316266 | Emperor of Brazil        | Dom Pedro II         |                   17 |
+---------------+--------------------------+----------------------+----------------------+
|  549755856528 | Camila                   | Alves                |                   17 |
+---------------+--------------------------+----------------------+----------------------+
|  274877914558 | Pol                      | Dara                 |                   17 |
+---------------+--------------------------+----------------------+----------------------+
|  893353214109 | Dumitru                  | David                |                   16 |
+---------------+--------------------------+----------------------+----------------------+
|  412316913317 | Isabel                   | Fernandez            |                   16 |
+---------------+--------------------------+----------------------+----------------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 10
Total elapsed time: 52.99 ms

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│  time: 12.75ms | rows: 76830 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│  time: 12.16ms | rows: 38044 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│  time: 10.52ms | rows: 38044 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 9.57ms | rows: 38044 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              GroupByOpr               │
├───────────────────────────────────────┤
│   time: 7.88ms | rows:  7325 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│         ProjectOrderByOprBeta         │
├───────────────────────────────────────┤
│   time: 0.10ms | rows:    10 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:    10 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:    10 tuples   │
└───────────────────────────────────────┘
```

What to observe in this profile:

1. **Scan dominates early stages**: `ScanWithGPredOpr` (scans nodes matching predicates) accounts for 12.75ms out of 52.99ms total. This is where all 76,830 comments are loaded into memory—a necessary baseline cost.

2. **Edge expansion is a significant bottleneck**: The two `EdgeExpandVOpr` (expands along relationships to produce new rows) operators combined take 22.68ms (43% of total time), even though the output stabilizes at 38,044 rows. This tells us that edge lookups are relatively expensive in this case; if this query is slow, consider indexing the comment-to-post and post-to-person relationships.

3. **Aggregation is efficient**: `GroupByOpr` (groups rows by key and applies aggregate functions) collapses 38,044 rows down to 7,325 groups in only 7.88ms. This shows that grouping is fast compared to the earlier edge traversal.

4. **Optimizer fuses ORDER + LIMIT**: `ProjectOrderByOprBeta` (fused operator: sorts rows and applies LIMIT in one pass) handles sorting and limiting in just 0.10ms, reducing 7,325 rows to 10 output rows. This demonstrates that the optimizer intelligently merges ORDER BY and LIMIT to avoid sorting the entire intermediate result.

5. **LIMIT does not reduce upstream work**: Notice that all 38,044 comment rows flow through to GroupByOpr before being filtered down. The `LIMIT` clause shortens output, not execution—this is why focusing on early filtering (e.g., date range on comments) would be more impactful than changing LIMIT.

#### 2. Which forum members are liking posts inside the forums they joined?

This is a practical engagement query: it correlates community membership with actual in-forum activity.

```cypher
PROFILE MATCH (f:forum)-[:forumHasMember]->(member:person),
              (f)-[:forumContainerOf]->(p:post),
              (member)-[:personLikesPost]->(p)
RETURN f.title,
       member.firstName,
       member.lastName,
       COUNT(DISTINCT p.id) AS liked_posts_in_forum;
```

Output:
```
+----------------------------+-----------------------+----------------------+------------------------+
| _0_f.title                 | _2_member.firstName   | _2_member.lastName   | liked_posts_in_forum   |
+============================+=======================+======================+========================+
| Album 1 of Mahinda Perera  | Dumitru               | David                | 2                      |
+----------------------------+-----------------------+----------------------+------------------------+
| Album 1 of Mahinda Perera  | Walter                | Becker               | 1                      |
+----------------------------+-----------------------+----------------------+------------------------+
| Album 3 of Mahinda Perera  | Dumitru               | David                | 1                      |
+----------------------------+-----------------------+----------------------+------------------------+
| Album 9 of Mahinda Perera  | Walter                | Becker               | 2                      |
+----------------------------+-----------------------+----------------------+------------------------+
| Album 9 of Mahinda Perera  | Anh                   | Nguyen               | 1                      |
+----------------------------+-----------------------+----------------------+------------------------+
| Album 10 of Mahinda Perera | Dumitru               | David                | 1                      |
+----------------------------+-----------------------+----------------------+------------------------+
| ...                        | ...                   | ...                  | ...                    |
+----------------------------+-----------------------+----------------------+------------------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 16254
Total elapsed time: 96.16 ms

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│  time: 26.20ms | rows: 78976 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│  time: 24.70ms | rows: 21636 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│          IntersectOprMultip           │
├───────────────────────────────────────┤
│  time: 23.41ms | rows: 21636 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│  time: 12.27ms | rows: 21636 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              GroupByOpr               │
├───────────────────────────────────────┤
│   time: 9.58ms | rows: 16254 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.00ms | rows: 16254 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.00ms | rows: 16254 tuples   │
└───────────────────────────────────────┘
```

What to observe in this profile:

1. **Scan cost is substantial**: ScanWithGPredOpr takes 26.20ms (27% of total time) scanning 78,976 forums. For engagement queries, starting with the larger collection (forums rather than members) may impact early performance.

2. **Edge expansion is comparable to Example 1**: The EdgeExpandVOpr consumes 24.70ms exploring forum-to-member relationships, reducing to 21,636 rows. This is similar in cost to Example 1, showing that edge lookup overhead is consistent across different query patterns.

3. **IntersectOprMultip: The multi-path correlation bottleneck**: `IntersectOprMultip` (correlates multiple independent relationship paths to find matches) represents aligning three independent paths (23.41ms, 24% of total time):
   - Forum → Member (forumHasMember)
   - Forum → Post (forumContainerOf)  
   - Member → Post (personLikesPost)
   
   The high cost reflects the computational overhead of finding all valid (forum, member, post) tuples where the member joined the forum AND likes a post inside that forum. The row count stays constant at 21,636, indicating no data explosion, just expensive correlation work.

4. **GroupByOpr: Moderate overhead for counting distinct values**: GroupByOpr reduces 21,636 rows to 16,254 groups in 9.58ms. The reduction suggests that many (forum, member) pairs like multiple posts within the same forum.

5. **Optimization insights**: If this query is slow, the bottleneck is the IntersectOprMultip correlation step (23.41ms), not aggregation (9.58ms). Consider:
   - Indexing the forumHasMember and forumContainerOf relationships
   - Rewriting the query to reduce the number of correlation candidates before the intersection (e.g., filtering forums by size first)

#### 3. Are users liking content that matches their declared interests?

This is a realistic personalization or recommendation-analysis query.

```cypher
PROFILE MATCH (person:person)-[:personHasInterest]->(tag:tag),
              (person)-[:personLikesPost]->(post:post)-[:postHasTag]->(tag)
RETURN person.firstName,
       person.lastName,
       tag.name,
       COUNT(DISTINCT post.id) AS matching_liked_posts;
```

Output:
```
+----------------------------+----------------------+----------------------------+------------------------+
| _0_person.firstName        | _0_person.lastName   | _2_tag.name                | matching_liked_posts   |
+============================+======================+============================+========================+
| Ali                        | Diori                | Henry_Wadsworth_Longfellow | 1                      |
+----------------------------+----------------------+----------------------------+------------------------+
| Antonio                    | Alvarez              | Wolfgang_Amadeus_Mozart    | 1                      |
+----------------------------+----------------------+----------------------------+------------------------+
| Ken                        | Yamada               | Franz_Kafka                | 1                      |
+----------------------------+----------------------+----------------------------+------------------------+
| Rahul                      | Nair                 | Hamid_Karzai               | 2                      |
+----------------------------+----------------------+----------------------------+------------------------+
| Fritz                      | Richter              | Orson_Welles               | 1                      |
+----------------------------+----------------------+----------------------------+------------------------+
| Walter                     | Schmidt              | United_Arab_Emirates       | 1                      |
+----------------------------+----------------------+----------------------------+------------------------+
| ...                        | ...                  | ...                        | ...                    |
+----------------------------+----------------------+----------------------------+------------------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 503
Total elapsed time: 26.17 ms

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 9.23ms | rows: 78976 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│   time: 8.40ms | rows: 21636 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│          IntersectOprMultip           │
├───────────────────────────────────────┤
│   time: 6.95ms | rows:   715 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 1.05ms | rows:   715 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              GroupByOpr               │
├───────────────────────────────────────┤
│   time: 0.54ms | rows:   503 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:   503 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:   503 tuples   │
└───────────────────────────────────────┘
```

What to observe in this profile:

1. **Scan establishes the baseline**: ScanWithGPredOpr takes 9.23ms to establish the initial working set for pattern matching. This is the foundational cost before any graph traversal.

2. **Edge expansion shows consistent overhead**: EdgeExpandVOpr consumes 8.40ms exploring personHasInterest relationships, producing 21,636 rows. The cost is comparable to the initial scan (9.23ms), demonstrating that edge lookup carries a consistent per-operation overhead in this query.

3. **IntersectOprMultip is highly selective**: This operator takes 6.95ms and dramatically reduces 21,636 rows down to 715 rows (97% reduction). It represents the core pattern: finding (person, tag) pairs where the person both declares interest in the tag AND has liked a post tagged with that tag. The selectivity reflects that most interests don't overlap with liked posts in the dataset.

4. **Dramatic downstream efficiency**: With only 715 candidate rows after intersection, the remaining operations are fast:
   - ProjectOpr: 1.05ms
   - GroupByOpr: 0.54ms (reducing 715 rows to 503 distinct person-tag pairs)

#### 4. What is the company footprint by location and hiring period?

This query combines a work relationship, organisation location, and aggregation over edge properties.

```cypher
PROFILE MATCH (person:person)-[w:personWorkAt]->(org:organisation)-[:organisationIsLocatedIn]->(place:place)
RETURN place.name,
       org.name,
       COUNT(DISTINCT person.id) AS employee_count,
       MIN(w.workFrom) AS earliest_work_from,
       MAX(w.workFrom) AS latest_work_from;
```

Output:
```
+-----------------+---------------------------------------+------------------+----------------------+--------------------+
| _6_place.name   | _2_org.name                           | employee_count   | earliest_work_from   | latest_work_from   |
+=================+=======================================+==================+======================+====================+
| Sri_Lanka       | SriLankan_Airlines                    | 2                | 2007                 | 2013               |
+-----------------+---------------------------------------+------------------+----------------------+--------------------+
| Sri_Lanka       | Aero_Lanka                            | 4                | 2003                 | 2013               |
+-----------------+---------------------------------------+------------------+----------------------+--------------------+
| Morocco         | Atlas_Blue                            | 5                | 2002                 | 2012               |
+-----------------+---------------------------------------+------------------+----------------------+--------------------+
| China           | Spring_Airlines                       | 10               | 1999                 | 2010               |
+-----------------+---------------------------------------+------------------+----------------------+--------------------+
| China           | Sichuan_Airlines                      | 11               | 2004                 | 2011               |
+-----------------+---------------------------------------+------------------+----------------------+--------------------+
| Netherlands     | Quick_Airways_Holland                 | 3                | 2005                 | 2006               |
+-----------------+---------------------------------------+------------------+----------------------+--------------------+
| Germany         | Luftfahrtgesellschaft_Walter          | 2                | 2005                 | 2007               |
+-----------------+---------------------------------------+------------------+----------------------+--------------------+
| ...             | ...                                   | ...              | ...                  | ...                |
+-----------------+---------------------------------------+------------------+----------------------+--------------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 796
Total elapsed time: 10.73 ms

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 2.64ms | rows:   903 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandEOpr             │
├───────────────────────────────────────┤
│   time: 2.56ms | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│           GetVFromEdgesOpr            │
├───────────────────────────────────────┤
│   time: 2.02ms | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│   time: 1.38ms | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 1.34ms | rows:  1953 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              GroupByOpr               │
├───────────────────────────────────────┤
│   time: 0.78ms | rows:   796 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:   796 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:   796 tuples   │
└───────────────────────────────────────┘
```

What to observe in this profile:

1. **Scan operates on a small collection**: ScanWithGPredOpr takes 2.64ms scanning 903 persons. Starting with the smallest entity in the pattern (person nodes vs. posts or comments) makes early scan operations fast.

2. **Multi-hop traversal remains efficient at scale**: The sequence of operators handles the two-hop traversal (person → organisation → place):
   - `EdgeExpandEOpr` (expands edges to gather edge+node pairs): 2.56ms (person to their work relationships)
   - `GetVFromEdgesOpr` (extracts target nodes from edge results): 2.02ms (extracting target organisation nodes)
   - `EdgeExpandVOpr`: 1.38ms (organisation to locations)
   
   The total overhead for multi-hop traversal is ~6ms, modest considering three relationship lookups are performed.

3. **Row counts stay manageable throughout**: The working set grows to 1,953 rows (person-organization pairs) and stays at that cardinality through to aggregation. This small cardinality is the key to fast execution—no intermediate explosion of rows.

4. **Aggregation with edge property functions is efficient**: GroupByOpr reduces 1,953 rows to 796 groups (place × organisation combinations) in just 0.78ms while computing COUNT(DISTINCT person.id), MIN(workFrom), and MAX(workFrom). This shows that aggregating edge properties alongside counting distinct entities is fast when row counts are reasonable.

5. **Total execution demonstrates query efficiency**: At 10.73ms, this query illustrates that multi-hop traversal queries can be very fast when:
   - You start with a smaller node collection (903 persons)
   - Traversals don't cause row explosion
   - Aggregation operates on a compact working set

#### 5. How quickly does a friend-of-a-friend expansion grow?

This query is useful when debugging recommendation-style graph expansion.

```cypher
PROFILE MATCH (p1:person)-[:personKnows]->(p2:person)-[:personKnows]->(p3:person)
WHERE p1.id <> p3.id
RETURN p1.firstName,
       p1.lastName,
       p2.firstName,
       p2.lastName,
       p3.firstName,
       p3.lastName
LIMIT 10;
```

Output:
```
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| _0_p1.firstName   | _0_p1.lastName   | _2_p2.firstName   | _2_p2.lastName   | _6_p3.firstName   | _6_p3.lastName   |
+===================+==================+===================+==================+===================+==================+
| Mahinda           | Perera           | Dumitru           | David            | Nicolae           | Antonescu        |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Dumitru           | David            | Amit              | Sharma           |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Dumitru           | David            | Jie               | Wang             |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Dumitru           | David            | Hans              | Berg             |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Dumitru           | David            | Mihai             | David            |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Dumitru           | David            | Jan               | Zakrzewski       |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Dumitru           | David            | Victor            | Antonescu        |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Dumitru           | David            | Chris             | Hall             |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Dumitru           | David            | Alexei            | Goma             |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+
| Mahinda           | Perera           | Dumitru           | David            | Alec              | Cheng            |
+-------------------+------------------+-------------------+------------------+-------------------+------------------+


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 10
Total elapsed time: 68.13 ms

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│  time: 15.04ms | rows:   903 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│  time: 14.95ms | rows:  6626 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│  time: 14.82ms | rows: 92285 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│               SelectOpr               │
├───────────────────────────────────────┤
│  time: 13.89ms | rows: 92285 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 8.34ms | rows: 92285 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│               LimitOpr                │
├───────────────────────────────────────┤
│   time: 1.09ms | rows:    10 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:    10 tuples   │
└───────────────────────────────────────┘
```

What to observe in this profile:

1. **First hop produces modest expansion**: The first EdgeExpandVOpr scans 903 persons and explores their personKnows relationships, producing 6,626 rows (7x expansion). The cost is 14.95ms—a stable edge lookup overhead.

2. **Second hop causes explosive row growth**: The second EdgeExpandVOpr is the critical point. Taking 14.82ms, it expands 6,626 rows (person → friend) to 92,285 rows (person → friend → friend-of-friend). This represents a **14x expansion**, demonstrating that multi-hop friend-of-friend expansion grows exponentially in social networks where each person has many connections.

3. **WHERE filter is highly selective or unnecessary**: `SelectOpr` (filters rows based on WHERE conditions) processes the `p1.id <> p3.id` condition in 13.89ms. The output remains at 92,285 rows, suggesting either:
   - The dataset contains no or very few self-loop FOF paths (where p1 == p3)
   - The social network structure makes such loops rare
   
This demonstrates an important principle: not all WHERE conditions produce significant filtering. Understanding your data distribution is crucial for optimization.

4. **Projection overhead on massive row set**: ProjectOpr takes 8.34ms to extract and format the six output columns (p1/p2/p3 names) from 92,285 rows. This moderate per-row cost scales linearly with the row count.

5. **LIMIT does not prevent upstream work**: LimitOpr finally reduces 92,285 rows down to just 10 output rows in only 1.09ms. However, all 92,285 rows had to flow through scan, two expansions, filtering, and projection before the limit took effect. The query's total cost is dominated by this upstream work, not the final output limit.

### EXPLAIN Example

EXPLAIN uses the same formatted report layout as PROFILE, but it does not execute the query. As a result, all elapsed times are `0.00 ms` and all output row counts are `0`.

```cypher
EXPLAIN MATCH (c:comment)-[:commentReplyOfPost]->(p:post)-[:postHasCreator]->(author:person)
RETURN p.id,
       author.firstName,
       author.lastName,
       COUNT(c.id) AS direct_reply_count
ORDER BY direct_reply_count DESC
LIMIT 10;
```

Sample output:
```
No results (total records: 0)


╔════════════════════════════════════════╗
║         PROFILE REPORT                 ║
╚════════════════════════════════════════╝
Total output tuples: 0
Total elapsed time: 0.00 ms

┌───────────────────────────────────────┐
│           ScanWithGPredOpr            │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│            EdgeExpandVOpr             │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              GroupByOpr               │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│         ProjectOrderByOprBeta         │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│              ProjectOpr               │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:     0 tuples   │
└───────────────────────────────────────┘
┌───────────────────────────────────────┐
│                SinkOpr                │
├───────────────────────────────────────┤
│   time: 0.00ms | rows:     0 tuples   │
└───────────────────────────────────────┘
```

This makes EXPLAIN useful when you want to validate plan first, and only run PROFILE after the plan looks reasonable.
