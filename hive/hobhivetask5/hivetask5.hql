ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE gorskajael;

DROP TABLE IF EXISTS receipt_types;
CREATE TABLE receipt_types
AS SELECT content.userInn as user, LEAD(kkt_transactions_1.subType) 
    OVER(PARTITION BY kkt_transactions_1.content.userInn ORDER BY content.dateTime.ddate) AS nextType,
    kkt_transactions_1.subType as subType,
    LAG(kkt_transactions_1.subType) 
    OVER(PARTITION BY kkt_transactions_1.content.userInn ORDER BY content.dateTime.ddate) AS prevType
FROM kkt_transactions_1
WHERE subType in ("openShift", "closeShift", "receipt");

SELECT DISTINCT receipt_types.user
FROM receipt_types
WHERE (receipt_types.nextType LIKE "openShift" OR receipt_types.prevType LIKE "closeShift") AND receipt_types.subType LIKE "receipt"
SORT BY user
LIMIT 50;