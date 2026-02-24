// compute sub pattern of <tag, message1, forum1, comment>
MATCH
  (message1:COMMENT:POST)-[:REPLYOF*0..]->(post1:POST)<-[:CONTAINEROF]-(forum1:FORUM),
  (message1:COMMENT:POST)-[:HASTAG]->(tag:TAG {name: "WOW_Hits_2008"}),
  (forum1:FORUM)-[:HASMEMBER]-(person2:PERSON)<-[:HASCREATOR]-(comment:COMMENT)-[:HASTAG]->(tag:TAG)
WITH DISTINCT message1, forum1, comment
// add message2 to the sub pattern
MATCH (comment)-[:REPLYOF]->(message2:POST:COMMENT)-[:HASTAG]->(tag:TAG {name: "WOW_Hits_2008"})
WHERE message1.creationDate + INTERVAL('2 hours') < message2.creationDate
WITH DISTINCT message1, forum1 as m1_forum1, collect(message2) as message2_fold
// compute forum2 of the aggregated message1
MATCH (message1)-[:HASCREATOR]->(person1:PERSON)<-[:HASMEMBER]-(forum2:FORUM)
WITH message1, m1_forum1, message2_fold, collect(forum2) as m1_forum2
UNWIND message2_fold as message2
WITH message1, m1_forum1, m1_forum2, message2
// compute forum1 of message2
MATCH (message2:COMMENT:POST)-[:REPLYOF*0..]->(post2:POST)<-[:CONTAINEROF]-(m2_forum1:FORUM)
WITH message1, m1_forum1, m1_forum2, message2, m2_forum1
// compute forum2 of message2
MATCH (message2)-[:HASCREATOR]->(person3:PERSON)<-[:HASMEMBER]-(m2_forum2:FORUM)
WHERE m1_forum1 = m2_forum2
    AND NOT (m2_forum1 IN m1_forum2)
WITH DISTINCT message1, message2
MATCH (message1)-[HASCREATOR]->(person1:PERSON)
RETURN person1.id AS id, count(DISTINCT message2) AS messageCount
ORDER BY messageCount DESC, id ASC
LIMIT 10