MATCH (p:PERSON {id: 15393162790207})-[k:KNOWS*1..3]-(other:PERSON)<-[hasMem:HASMEMBER]-(f:FORUM), 
(f:FORUM)-[:CONTAINEROF]->(po:POST)-[:HASCREATOR]->(other:PERSON) WHERE hasMem.joinDate > 1344643200000 
WITH f as f, count(distinct po) AS postCount ORDER BY postCount DESC, f.id ASC LIMIT 20  RETURN f.title as title, postCount;

MATCH (person:PERSON { id: 15393162790207 })-[:KNOWS*1..3]-(otherPerson)
WHERE
    person <> otherPerson
WITH DISTINCT otherPerson
MATCH (otherPerson)<-[membership:HASMEMBER]-(forum)
WHERE
    membership.joinDate > 1288612800000
WITH forum, otherPerson
OPTIONAL MATCH (otherPerson)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)
RETURN forum.title as title, forum.id as id, count(distinct post) AS postCount
ORDER BY
    postCount DESC,
    id ASC
LIMIT 20