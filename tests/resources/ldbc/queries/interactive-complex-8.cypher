MATCH(p:PERSON {id: $personId}) <-[:HASCREATOR] -(msg : POST : COMMENT) <- [:REPLYOF] - (cmt: COMMENT) - [:HASCREATOR] -> (author : PERSON)
WITH
   cmt, author, cmt.creationDate as cmtCreationDate , cmt.id as cmtId
ORDER BY 
    cmtCreationDate DESC, 
    cmtId ASC 
LIMIT 20 
RETURN
    author.id, 
    author.firstName, 
    author.lastName, 
    cmtCreationDate,
    cmtId,
    cmt.content
