MATCH (person1: PERSON)
WITH person1
CALL (person1) {
  MATCH (person1)<-[:HASCREATOR]-(msg:COMMENT:POST)
  WHERE msg.creationDate = $dateA
  MATCH (msg) -[:HASTAG]->(tag:TAG)
  WHERE tag.name = "Diosdado_Macapagal"
  OPTIONAL MATCH (person1)-[:KNOWS]-(other_person:PERSON)<-[:HASCREATOR]-(o_msg:COMMENT:POST)-[:HASTAG]->(tag:TAG)
  WHERE o_msg.creationDate =  $dateA
  RETURN person1, other_person, tag

  UNION ALL
  MATCH (person1)<-[:HASCREATOR]-(msg:COMMENT:POST)
  WHERE msg.creationDate = $dateB 
  MATCH (msg) -[:HASTAG]->(tag:TAG)
  WHERE tag.name = "Thailand"
  OPTIONAL MATCH (person1)-[:KNOWS]-(other_person:PERSON)<-[:HASCREATOR]-(o_msg:COMMENT:POST)-[:HASTAG]->(tag:TAG)
  WHERE o_msg.creationDate = $dateB 
  RETURN person1, other_person, tag
}
WITH
  person1,
  other_person,
  CASE WHEN tag.name = "Diosdado_Macapagal" THEN 1
    ELSE 0
    END as msg1,
  CASE WHEN  tag.name =  "Thailand" THEN 1
    ELSE 0
    END as msg2
WITH
  person1,
  count(other_person) as p2Cnt,
  count(DISTINCT msg1) as msg1Cnt,
  count(DISTINCT msg2) as msg2Cnt
  WHERE p2Cnt <= 6 AND msg1Cnt > 0 AND msg2Cnt > 0
RETURN
  person1.id as personId,
  msg1Cnt,
  msg2Cnt 
  ORDER BY
  msg1Cnt + msg2Cnt DESC,
  personId ASC
  LIMIT 20;
