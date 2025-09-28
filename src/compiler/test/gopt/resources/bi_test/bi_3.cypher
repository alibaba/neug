MATCH
  (country:PLACE {name: "India"})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-
  (person:PERSON)<-[:HASMODERATOR]-(forum:FORUM)-[:CONTAINEROF]->
  (post:Post)<-[:REPLYOF*0..]-(message)-[:HASTAG]->(:TAG)-[:HASTYPE]->(:TAGCLASS {name: "Saint"})
RETURN
  forum.id as id,
  forum.title,
  forum.creationDate,
  person.id as personId,
  count(DISTINCT message) AS messageCount
  ORDER BY
  messageCount DESC,
  id ASC
  LIMIT 20