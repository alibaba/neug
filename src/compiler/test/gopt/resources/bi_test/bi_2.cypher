MATCH (tag:TAG)-[:HASTYPE]->(:TAGCLASS {name: "ChristianBishop"})
OPTIONAL MATCH (tag:TAG)<-[:HASTAG]-(message:POST:COMMENT)
WITH
  tag,
  CASE
    WHEN message.creationDate <  TIMESTAMP("2010-09-16 00:00:00")
  AND message.creationDate >= TIMESTAMP("2010-06-08 00:00:00") THEN 1
    ELSE                                     0
    END AS count1,
  CASE
    WHEN message.creationDate <  TIMESTAMP("2010-12-26 00:00:00")
  AND message.creationDate >= TIMESTAMP("2010-06-08 00:00:00") THEN 1
    ELSE                                     0
    END AS count2
WITH
  tag,
  sum(count1) AS countWindow1,
  sum(count2) AS countWindow2
RETURN
  tag.name as name,
  countWindow1,
  countWindow2,
  CASE WHEN countWindow1 > countWindow2 THEN countWindow1 - countWindow2
  ELSE countWindow2 - countWindow1 END AS diff
ORDER BY
diff DESC,
name ASC
LIMIT 100