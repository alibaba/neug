MATCH
  (tag:TAG {name: $tag})<-[:HASTAG]-(message:POST|COMMENT),
  (message)<-[:REPLYOF]-(comment:COMMENT)
WHERE NOT (comment:COMMENT)-[:HASTAG]->(tag:TAG {name: $tag})
MATCH (comment:COMMENT)-[:HASTAG]->(relatedTag:TAG)
RETURN
  relatedTag.name as name,
  count(DISTINCT comment) AS count
ORDER BY
  count DESC,
  name ASC
LIMIT 100