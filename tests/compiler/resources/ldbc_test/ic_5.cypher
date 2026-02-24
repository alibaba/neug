MATCH (person:PERSON { id: $personId })-[:KNOWS*1..2]-(friend)
WITH DISTINCT friend
WHERE friend.id <> $personId
CALL (friend) {
  MATCH (friend)<-[membership:HASMEMBER]-(forum)
  WHERE membership.joinDate > $minDate
  WITH distinct forum
  ORDER BY forum.id ASC
  LIMIT 20
  RETURN forum, 0 AS postCount
UNION ALL
  MATCH (friend)<-[membership:HASMEMBER]-(forum)
  WHERE membership.joinDate > $minDate
  WITH friend, collect(distinct forum) AS forums
  MATCH (friend)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)
  WHERE forum IN forums
  WITH forum, count(post) AS postCount
  RETURN forum, postCount
  ORDER BY postCount DESC, forum.id ASC
  LIMIT 20
}
WITH forum, max(postCount) AS postCount
ORDER BY postCount DESC, forum.id ASC
LIMIT 20

RETURN forum.title as name, postCount