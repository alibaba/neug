MATCH (person1: PERSON {id: $person1Id})
WITH person1
CALL (person1) {
  MATCH (person1: PERSON)-[k:KNOWS* SHORTEST 1..]-(person2: PERSON {id: $person2Id})
  RETURN length(k) as len

  UNION ALL
  RETURN -1 as len
}
RETURN max(len) as len