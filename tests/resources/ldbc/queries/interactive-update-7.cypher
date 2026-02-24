WITH
  $authorPersonId as personId,
  $countryId as countryId,
  $replyToPostId as replyToPostId,
  $replyToCommentId as replyToCommentId,
  $commentId as commentId,
  $creationDate as creationDate,
  $locationIP as locationIP,
  $browserUsed as browserUsed,
  $content as content,
  $length as length,
  $tagIds as tagIds
CREATE (c:COMMENT {
    id: commentId,
    creationDate: creationDate,
    locationIP: locationIP,
    browserUsed: browserUsed,
    content: content,
    length: length
  })
CREATE (author:PERSON {id: personId})<-[:HASCREATOR {creationDate: creationDate}]-(c:COMMENT {id: commentId})
CREATE (c:COMMENT {id: commentId})-[:REPLYOF]->(message:POST {id: replyToPostId})
CREATE (c:COMMENT {id: commentId})-[:REPLYOF]->(message:COMMENT {id: replyToCommentId})
CREATE (c:COMMENT {id: commentId})-[:ISLOCATEDIN]->(country:PLACE {id: countryId})
UNWIND tagIds AS tagId
CREATE (c:COMMENT {id: commentId})-[:HASTAG]->(t:TAG {id: tagId})