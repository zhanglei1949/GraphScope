MATCH  (p1:PERSON {id:$personId})-[:PERSON_KNOWS_PERSON]-(p2:PERSON)<-[fp:FORUM_HASMEMBER_PERSON]-(f:FORUM)-[:FORUM_CONTAINEROF_POST]->(m:POST), (m)-[:POST_HASCREATOR_PERSON]->(p2)
WHERE  fp.joinDate >= $minDate
RETURN f.title;