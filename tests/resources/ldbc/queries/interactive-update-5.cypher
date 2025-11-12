WITH $forumId as forumId, $personId as personId, $joinDate as joinDate
CREATE (f:FORUM {id: forumId})-[:HASMEMBER {joinDate: joinDate}]->(p:PERSON {id: personId})