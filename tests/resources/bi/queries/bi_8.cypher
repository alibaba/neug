MATCH (tag:TAG {name: $tag})
CALL {
  MATCH (tag)<-[interest:HASINTEREST]-(person:PERSON)
  RETURN person, count(tag) as cnt1, 0L as cnt2
}
UNION
CALL {
  MATCH (tag)<-[:HASTAG]-(message:POST|COMMENT)
  MATCH (message)-[:HASCREATOR]->(person:PERSON)
  WHERE $startDate < message.creationDate AND message.creationDate < $endDate
  RETURN person, 0L as cnt1, count(tag) as cnt2
}
WITH person, sum(cnt1) * 100L + sum(cnt2) as score
CALL {
  MATCH (person)-[:KNOWS]-(person2:PERSON)
  RETURN person2 as person, 0L as score, sum(score) as friendScore
}
UNION
CALL {
  RETURN person, score, 0L as friendScore
}
WITH person.id as id, sum(score) as score, sum(friendScore) as friendScore
WHERE score > 0L
RETURN id, score, friendScore
ORDER BY
  score + friendScore DESC,
  id ASC
LIMIT 100