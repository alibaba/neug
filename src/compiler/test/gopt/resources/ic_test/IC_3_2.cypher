MATCH (p1:person)-[k1:knows]-(:person)-[:knows]-(p2:person)<-[:comment_hascreator]-(m1:comment)-[:comment_islocatedin]->(pl1:place)
MATCH (p2)<-[:comment_hascreator]-(m2:comment)-[:comment_islocatedin]->(pl2:place)
WHERE p1.p_personid = 26388279067479 
AND m1.m_creationdate >= 1296242799012 
AND m1.m_creationdate < 1301426799012 
AND m2.m_creationdate >= 1296242799012 
AND m2.m_creationdate < 1301426799012 
AND pl1.pl_name = "Germany" 
AND pl2.pl_name = "Germany" 
RETURN p2.p_personid as p2_id, p2.p_firstname as p2_firstname, p2.p_lastname as p2_lastname
