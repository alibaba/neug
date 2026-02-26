MATCH (author:PERSON {id: $authorPersonId}),
       (country:PLACE {id: $countryId})
CREATE (author)<-[:HASCREATOR {creationDate: $creationDate}]- (c:COMMENT {
    id: $commentId,
    creationDate: $creationDate,
    locationIP: $locationIP,
    browserUsed: $browserUsed,
    content: $content,
    length: $length
})-[:ISLOCATEDIN]->(country)
WITH c
OPTIONAL MATCH(comment :COMMENT {id: $replyToCommentId})
OPTIONAL MATCH(post:POST {id: $replyToPostId})
WITH c, comment, post
CALL (c, comment, post)  {
WITH c, comment, post
WHERE comment IS NOT NULL
CREATE (c)-[:REPLYOF]->(comment: COMMENT)
RETURN c

UNION ALL

WITH c, comment, post
WHERE post IS NOT NULL
CREATE (c)-[:REPLYOF]->(post: POST)
RETURN c
}
WITH c
UNWIND CAST($tagIds, 'INT64[]') AS tagId
MATCH (t:TAG {id: tagId})
CREATE (c)-[:HASTAG]->(t)