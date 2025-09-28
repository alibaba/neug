MATCH (person:PERSON)
OPTIONAL MATCH (person)<-[:HASCREATOR]-(message:COMMENT:POST)
WHERE message.length > 115 AND message.creationDate > DATE("2012-06-05")
MATCH (message)-[:REPLYOF * 0..]->(post:POST)
WHERE post.language = 'a' OR post.language = 'b'

WITH
  person,
  count(message) AS messageCount
RETURN
  messageCount,
  count(person) AS personCount
  ORDER BY
  personCount DESC,
  messageCount DESC
