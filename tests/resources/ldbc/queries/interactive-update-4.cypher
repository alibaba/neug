MATCH (p:PERSON {id: $moderatorPersonId})
CREATE (f:FORUM {id: $forumId, title: $forumTitle, creationDate: $creationDate})-[:HASMODERATOR]->(p)
WITH f
UNWIND CAST($tagIds, 'INT64[]') AS tagId
MATCH (t:TAG {id: tagId} )
CREATE (f)-[:HASTAG]->(t)