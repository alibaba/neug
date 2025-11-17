MATCH (tag:TAG {name: "Pope_Paul_VI"})<-[:HASINTEREST]-(person1:PERSON)-[:KNOWS]-(mutualFriend:PERSON),
      (mutualFriend:PERSON)-[:KNOWS]-(person2:PERSON)-[:HASINTEREST]->(tag)
WHERE NOT (person1)-[:KNOWS]-(person2) AND person1 <> person2
RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT mutualFriend) AS mutualFriendCount
  ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
  LIMIT 20