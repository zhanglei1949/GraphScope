MATCH (p:PERSON {id: $personId})-[k:KNOWS*1..3]-(other:PERSON)<-[hasMem:HASMEMBER]-(f:FORUM), 
    (f:FORUM)-[:CONTAINEROF]->(po:POST)-[:HASCREATOR]->(other:PERSON) 
WHERE 
    p <> other
    AND hasMem.joinDate > $minDate 
WITH 
    f as f, 
    count(distinct po) AS postCount 
ORDER BY 
    postCount DESC, 
    f.id ASC 
LIMIT 20 
RETURN 
    f.title as title, 
    postCount;