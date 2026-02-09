MATCH (m:POST : COMMENT {id: $messageId })-[:REPLYOF*0..]->(p:POST)<-[:CONTAINEROF]-(f:FORUM)-[:HASMODERATOR]->(mod:PERSON)
RETURN
    f.id AS forumId,
    f.title AS forumTitle,
    mod.id AS moderatorId,
    mod.firstName AS moderatorFirstName,
    mod.lastName AS moderatorLastName