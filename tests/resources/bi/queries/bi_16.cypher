CALL {
  MATCH (person1:PERSON) <-[:HASCREATOR]-(msg:COMMENT|POST)-[:HASTAG]->(tag:TAG {name: $tagA})
  WHERE  gs.function.date32(msg.creationDate) = $dateA
  OPTIONAL MATCH (person1)-[:KNOWS]-(person2:PERSON)<-[:HASCREATOR]-(msg2:POST|COMMENT)-[:HASTAG]->(tag)
  WHERE gs.function.date32(msg2.creationDate) = $dateA
  WITH person1, count(DISTINCT msg) AS cm, count(DISTINCT person2) AS cp2
  WHERE cp2 <= $maxKnowsLimit
  // return count
  RETURN person1, cm, 0L as cm2
}
UNION
CALL {
  MATCH (person1:PERSON) <-[:HASCREATOR]-(msg:COMMENT|POST)-[:HASTAG]->(tag:TAG {name: $tagB})
  WHERE  gs.function.date32(msg.creationDate) = $dateB
  OPTIONAL MATCH (person1)-[:KNOWS]-(person2:PERSON)<-[:HASCREATOR]-(msg2:POST|COMMENT)-[:HASTAG]->(tag)
  WHERE gs.function.date32(msg2.creationDate) = $dateB
  WITH person1, count(DISTINCT msg) AS cm2, count(DISTINCT person2) AS cp2
  WHERE cp2 <= $maxKnowsLimit
  // return count
  RETURN person1, 0L as cm, cm2
}

WITH person1, sum(cm) as msg1Cnt , sum(cm2) as msg2Cnt
WHERE msg1Cnt > 0 AND msg2Cnt > 0
RETURN person1.id as personId, msg1Cnt, msg2Cnt
ORDER BY msg1Cnt + msg2Cnt DESC, personId ASC
LIMIT 20

  
