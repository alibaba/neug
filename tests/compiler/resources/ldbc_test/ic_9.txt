MATCH(person: PERSON { id: 124 }) - [: KNOWS * 2..3] - (friend: PERSON)
WHERE NOT friend.id <> person.id
WITH friend
MATCH (friend)<-[:HASCREATOR]-(post:POST)
WITH friend,  count(post) as postCount

CALL (friend, postCount) {

    MATCH(friend: PERSON) < -[: HASCREATOR] - (post: POST) -[: HASTAG] -> (tag:TAG)
    WITH friend, postCount, post, tag
    MATCH(tag: TAG) < -[: HASINTEREST] - (person: PERSON { id: 124 })
    WITH friend, postCount, count(distinct post) as commonPostCount
    return friend, commonPostCount - (postCount - commonPostCount) AS commonInterestScore
        ORDER BY commonInterestScore DESC, friend.id ASC
        LIMIT 10

UNION ALL

    return friend, postCount AS commonInterestScore
        ORDER BY commonInterestScore DESC, friend.id ASC
        LIMIT 10
}
WITH friend, max(commonInterestScore) AS score
ORDER BY score DESC, friend.id ASC
LIMIT 10
MATCH(friend: PERSON) - [: ISLOCATEDIN] -> (city:PLACE)
RETURN friend.id,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    score,
    friend.gender AS personGender,
    city.name AS personCityName