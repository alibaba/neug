MATCH (person:person{ id: 123 })
WITH person
CALL (person) {

  MATCH (person)-[k:knows]->(friend)
  WHERE k.weight > 1.0
  Return friend

UNION ALL 
 
 Match (person)-[k:knows]->(friend)
 WHERE k.weight < 1.0
 Return friend
}
Return friend.id, friend.name