MATCH (p_:person {id:$personId})-[:knows*1..3]-(other:person)<-[:hasCreator]-(p:post)-[:hasTag]->(t:tag {name:$tagName}),(p:post)-[:hasTag]->(otherTag:tag) WHERE otherTag <> t RETURN otherTag.name as name, count(distinct p) as postCnt ORDER BY postCnt desc, name asc LIMIT 10