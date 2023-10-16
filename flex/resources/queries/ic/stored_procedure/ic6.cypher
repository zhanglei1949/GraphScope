MATCH (p_:PERSON {id:$personId})-[:KNOWS*1..3]-(other:PERSON)<-[:HASCREATOR]-(p:POST)
with p as p

MATCH(tag: TAG {name : $tagName})<-[:HASTAG]-(p:POST)-[:HASTAG]->(otherTag:TAG)
WHERE otherTag.name <> $tagName
RETURN otherTag.name as name, count(distinct p) as postCnt
ORDER BY postCnt desc, name asc LIMIT 10;