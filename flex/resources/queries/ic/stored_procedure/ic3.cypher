MATCH (countryX:PLACE {name: $countryXName})<-[:ISLOCATEDIN]-(messageX : POST | COMMENT)-[:HASCREATOR]->(otherP:PERSON),
(countryY:PLACE {name: $countryYName})<-[:ISLOCATEDIN]-(messageY: POST | COMMENT)-[:HASCREATOR]->(otherP:PERSON),
(otherP:PERSON)-[:ISLOCATEDIN]->(city:PLACE)-[:ISPARTOF]->(countryCity:PLACE),
(p:PERSON {id:$personId})-[:KNOWS*1..3]-(otherP:PERSON) 
WHERE messageX.creationDate >= $startDate and messageX.creationDate < $endDate AND messageY.creationDate >= $startDate 
and messageY.creationDate < $endDate AND  countryCity.name <> $countryXName and countryCity.name <> $countryYName WITH otherP,
count(messageX) as xCount, count(messageY) as yCount 
RETURN otherP.id as id,otherP.firstName as firstName, otherP.lastName as lastName, xCount, yCount, xCount + yCount as total 
ORDER BY total DESC, id ASC LIMIT 20;
