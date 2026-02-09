MATCH (m:POST:COMMENT {id: $messageId})
RETURN
    m.creationDate as messageCreationDate,
    m.content as messageContent,
    m.imageFile as messageImageFile;