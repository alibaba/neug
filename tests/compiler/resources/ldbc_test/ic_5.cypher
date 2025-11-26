MATCH (person:PERSON { id: 124 })-[:KNOWS*1..3]-(friend)
WITH friend
WHERE friend.id <> 124
CALL (friend) {
  MATCH (friend)<-[membership:HASMEMBER]-(forum)
  RETURN forum, 0 AS postCount
  ORDER BY forum.id ASC
  LIMIT 20

UNION ALL 

  MATCH (friend)<-[membership2:HASMEMBER]-(forum)
  WITH friend, collect(distinct forum) as forums
  
  MATCH (friend)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)
  WITH forum, count(post) AS postCount
  RETURN forum, postCount
  ORDER BY postCount DESC, forum.id ASC
  LIMIT 20
}
WITH forum, max(postCount) AS postCount
ORDER BY postCount DESC, forum.id ASC
LIMIT 20

RETURN forum.title as name, postCount