MATCH (c:PLACE {id: $cityId})
CREATE (p:PERSON {
  id: $personId,
  firstName: $personFirstName,
  lastName: $personLastName,
  gender: $gender,
  birthday: $birthday,
  creationDate: $creationDate,
  locationIP: $locationIP,
  browserUsed: $browserUsed,
  language: $languages,
  email: $emails
})-[:ISLOCATEDIN]->(c)
WITH distinct p
UNWIND CAST($tagIds, 'INT64[]') AS tagId
MATCH (t:TAG {id: tagId})
CREATE (p)-[:HASINTEREST]->(t)
WITH distinct p
UNWIND CAST($studyAt, 'INT64[][]') AS studyAt
WITH p, studyAt[0] as studyAt_0, CAST(studyAt[1], 'INT32') as studyAt_1
MATCH(u:ORGANISATION {id: studyAt_0})
CREATE (p)-[:STUDYAT {classYear:studyAt_1}]->(u)
WITH distinct p
UNWIND CAST($workAt, 'INT64[][]') AS workAt
WITH p, workAt[0] as workAt_0, CAST(workAt[1], 'INT32') as workAt_1
MATCH(comp:ORGANISATION {id: workAt_0})
CREATE (p)-[:WORKAT {workFrom: workAt_1}]->(comp)