MATCH
    (person1:PERSON)<-[:HASCREATOR]-(message1:COMMENT:POST)-[:REPLYOF*0..]->(post1:POST)<-[:CONTAINEROF]-(forum1:FORUM),
    (message1:COMMENT:POST)-[:HASTAG]->(tag:TAG {name: "WOW_Hits_2008"}),
    (forum1:FORUM)-[:HASMEMBER]-(person2:PERSON)<-[:HASCREATOR]-(comment:COMMENT)-[:HASTAG]->(tag:TAG),
    (forum1:FORUM)-[:HASMEMBER]-(person3:PERSON)<-[:HASCREATOR]-(message2:COMMENT:POST),
    (comment:COMMENT)-[:REPLYOF]->(message2:COMMENT:POST)-[:REPLYOF*0..]->(post2:POST)<-[:CONTAINEROF]-(forum2:FORUM),
    (message2:COMMENT:POST)-[:HASTAG]->(tag:TAG)
WHERE forum1 <> forum2
AND message2.creationDate > message1.creationDate + interval('5 days')
AND NOT (forum2)-[:HASMEMBER]->(person1)
RETURN person1.id AS id, count(DISTINCT message2) AS messageCount
  ORDER BY messageCount DESC, id ASC
  LIMIT 10
  