MATCH (author:PERSON {id: $authorPersonId}),
       (country:PLACE {id: $countryId})
CREATE (author)<-[:HASCREATOR]- (c:COMMENT {
    id: $commentId,
    creationDate: $creationDate,
    locationIP: $locationIP,
    browserUsed: $browserUsed,
    content: $content,
    length: $length
})-[:ISLOCATEDIN]->(country)
WITH c
CALL (c){
MATCH  (comment: COMMENT {id: $replyToCommentId})
CREATE (c)-[:REPLYOF]->(comment)
RETURN c

UNION ALL

MATCH (post: POST {id :$replyToPostId})
CREATE (c)-[:REPLYOF]->(post)
RETURN c
}
WITH c
UNWIND CAST($tagIds, 'INT64[]') AS tagId
MATCH (t:TAG {id: tagId})
CREATE (c)-[:HASTAG]->(t)