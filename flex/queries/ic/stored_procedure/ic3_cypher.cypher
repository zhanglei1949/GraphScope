MATCH (countryX:place {name: $countryXName})<-[:isLocatedIn]-(messageX : post | comment)-[:hasCreator]->(otherP:person),(countryY:place {name: $countryYName})<-[:isLocatedIn]-(messageY: post | comment)-[:hasCreator]->(otherP:person),(otherP:person)-[:isLocatedIn]->(city:place)-[:isPartOf]->(countryCity:place),(p:person {id:$personId})-[:knows*1..3]-(otherP:person) WHERE messageX.creationDate >= $startDate and messageX.creationDate < $endDate AND messageY.creationDate >= $startDate and messageY.creationDate < $endDate AND  countryCity.name <> $countryXName and countryCity.name <> $countryYName WITH otherP, count(messageX) as xCount, count(messageY) as yCount RETURN otherP.id as id,otherP.firstName as firstName, otherP.lastName as lastName, xCount, yCount, xCount + yCount as total ORDER BY total DESC, id ASC LIMIT 20