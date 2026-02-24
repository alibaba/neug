WITH
  $personId as personId,
  $personFirstName as firstName,
  $personLastName as lastName,
  $gender as gender,
  $birthday as birthday,
  $creationDate as creationDate,
  $locationIP as locationIP,
  $browserUsed as browserUsed,
  $languages as languages,
  $emails as emails,
  $cityId as cityId,
  $tagIds as tagIds,
  $studyAt as studyAts,
  $workAt as workAts
CREATE (p:PERSON {
  id: personId,
  firstName: firstName,
  lastName: lastName,
  gender: gender,
  birthday: birthday,
  creationDate: creationDate,
  locationIP: locationIP,
  browserUsed: browserUsed,
  language: languages,
  email: emails
})
CREATE (p:PERSON {id: personId})-[:ISLOCATEDIN]->(c:PLACE {id: cityId})
UNWIND tagIds AS tagId
CREATE (p:PERSON {id: personId})-[:HASINTEREST]->(t:TAG {id: tagId})
WITH distinct personId, studyAts, workAts
UNWIND studyAts AS studyAt
WITH personId, workAts, gs.function.first(studyAt) as studyAt_0, gs.function.second(studyAt) as studyAt_1
CREATE (p:PERSON {id:personId})-[:STUDYAT {classYear:studyAt_1}]->(u:ORGANISATION {id:studyAt_0})
WITH distinct personId, workAts
UNWIND workAts AS workAt
WITH personId, gs.function.first(workAt) as workAt_0, gs.function.second(workAt) as workAt_1
CREATE (p:PERSON {id: personId})-[:WORKAT {workFrom: workAt_1}]->(comp:ORGANISATION {id: workAt_0})