Match (tag:TAG {name: $tag})<-[:HASTAG]-(message:POST|COMMENT)
WITH DISTINCT message
OPTIONAL MATCH (message)<-[:LIKES]-(liker:PERSON)
WITH message, count(liker) as likeCount
OPTIONAL MATCH (message)<-[:REPLYOF]-(comment:COMMENT)
WITH message, likeCount, count(comment) as replyCount
MATCH (message)-[:HASCREATOR]->(person:PERSON)
WITH
  person.id AS id,
  sum(replyCount) as replyCount,
  sum(likeCount) as likeCount,
  count(message) as messageCount
RETURN
  id,
  replyCount,
  likeCount,
  messageCount,
  1*messageCount + 2*replyCount + 10*likeCount AS score
ORDER BY
  score DESC,
  id ASC
LIMIT 100;