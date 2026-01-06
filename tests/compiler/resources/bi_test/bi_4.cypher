MATCH (country:PLACE)<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(person:PERSON)<-[:HASMEMBER]-(forum:FORUM)
WHERE forum.creationDate > $date
WITH country, forum, count(person) AS numberOfMembers
ORDER BY numberOfMembers DESC, forum.id ASC, country.id
LIMIT 100
WITH collect(DISTINCT forum) AS topForums
UNWIND topForums as topForum1
WITH topForum1, topForums

// CALL can only be used after WITH
CALL (topForum1, topForums) {
  // MATCH should be first in each call union branch
  MATCH (topForum1)-[:CONTAINEROF]->(post:POST)<-[:REPLYOF*0..]-(message:POST:COMMENT)-[:HASCREATOR]->(person:PERSON)<-[:HASMEMBER]-(topForum2:FORUM)
  WITH person, message, topForum2, topForums
  WHERE topForum2 IN topForums
  RETURN person, count(DISTINCT message) AS messageCount
UNION ALL
  // Ensure that people who are members of top forums but have 0 messages are also returned.
  // To this end, we return each person with a 0 messageCount
  MATCH (person:PERSON)<-[:HASMEMBER]-(topForum1:FORUM)
  RETURN person, 0 AS messageCount
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

