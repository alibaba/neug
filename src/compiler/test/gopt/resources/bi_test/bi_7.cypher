MATCH
  (tag:TAG {name: "Slovenia"})<-[:HASTAG]-(message:COMMENT:POST),
  (message:COMMENT:POST)<-[:REPLYOF]-(comment:COMMENT)
WHERE NOT (comment:COMMENT)-[:HASTAG]->(:TAG {name: "Slovenia"})
WITH distinct comment
Match (comment:COMMENT)-[:HASTAG]->(relatedTag:TAG)
RETURN
  relatedTag.name AS name,
  count(DISTINCT comment) AS count
ORDER BY
  count DESC,
  name ASC
LIMIT 100