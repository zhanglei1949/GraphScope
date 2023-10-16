MATCh(p : PERSON {id:$personId})-[:KNOWS*1..3]-(otherP:PERSON)-[:ISLOCATEDIN]->(city:PLACE)-[:ISPARTOF]->(countryCity:PLACE)
WHERE countryCity.name <> $countryXName AND countryCity.name <> $countryYName
WITH distinct otherP AS otherP

MATCH(otherP: PERSON) <-[:HASCREATOR]-(messageX: POST | COMMENT)-[:ISLOCATEDIN]->(countryX: PLACE)
WHERE countryX.name = $countryXName AND messageX.creationDate >= $startDate AND messageX.creationDate < $endDate
with otherP AS otherP, COUNT(messageX) as xCount

MATCH(otherP: PERSON)<-[:HASCREATOR]-(messageY: POST | COMMENT)-[:ISLOCATEDIN]->(countryY: PLACE)
where countryY.name = $countryYName AND messageY.creationDate >= $startDate AND messageY.creationDate < $endDate
with otherP AS otherP, xCount as xCount, COUNT(messageY) as yCount
RETURN otherP.id as id,otherP.firstName as firstName, otherP.lastName as lastName, xCount, yCount, xCount + yCount as total 
ORDER BY total DESC, id ASC LIMIT 20;
