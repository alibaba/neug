WITH 
  $moderatorPersonId as personId,
  $forumId as forumId,
  $forumTitle as forumTitle,
  $creationDate as creationDate,
  $tagIds as tagIds
CREATE (f:FORUM {id: forumId, title: forumTitle, creationDate: creationDate})
CREATE (f:FORUM {id: forumId})-[:HASMODERATOR]->(p:PERSON {id: personId})
UNWIND tagIds AS tagId
CREATE (f:FORUM {id: forumId})-[:HASTAG]->(t:TAG {id: tagId})