MATCH (forum:FORUM)
WHERE forum.creationDate > $creationDate
WITH forum
ORDER BY
  forum.popularity DESC
LIMIT 100
WITH collect(forum) AS topForums
CALL {
  UNWIND topForums AS topForums1
  MATCH (topForums1:FORUM)-[:CONTAINEROF]->(post:POST)<-[:REPLYOF*0..10]-(message:POST|COMMENT)-[:HASCREATOR]->(person:PERSON)
  WITH message, person, topForums
  UNWIND topForums AS topForums2
  MATCH (person)<-[:HASMEMBER]-(topForums2:FORUM)
  RETURN
    person,
    count(DISTINCT message) AS messageCount
  ORDER BY
    messageCount DESC,
    person.id ASC
  LIMIT 100
}
UNION
CALL {
  // Ensure that people who are members of top forums but have 0 messages are also returned.
  // To this end, we return each person with a 0 messageCount
  UNWIND topForums AS topForum1
  MATCH (person:PERSON)<-[:HASMEMBER]-(topForum1:FORUM)
  RETURN person, 0L AS messageCount
  ORDER BY
    person.id ASC
  LIMIT 100
}
RETURN
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  sum(messageCount) AS messageCount
ORDER BY
  messageCount DESC,
  personId ASC
LIMIT 100