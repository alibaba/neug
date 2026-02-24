MATCH (pA:Person)-[knows:KNOWS]-(pB:Person)
OPTIONAL MATCH (pA)<-[:HASCREATOR]-(m1:POST:COMMENT)-[r:REPLYOF]-(m2:POST:COMMENT)-[:HASCREATOR]->(pB)
OPTIONAL MATCH (m1)-[:REPLYOF*0..]->(:Post)<-[:CONTAINEROF]-(forum:Forum)
        WHERE forum.creationDate >= timestamp('2012-01-01 00:00:00')
        AND forum.creationDate <= timestamp('2012-10-01 00:00:00')
WITH pA, pB, knows,
    sum(CASE forum IS NOT NULL
        WHEN true THEN
            CASE (labels(m1) = 'Post' OR labels(m2) = 'Post') WHEN true THEN 1.0
            ELSE 0.5 END
        ELSE 0.0 END
    ) AS w
SET knows.score = 1/(w+1)