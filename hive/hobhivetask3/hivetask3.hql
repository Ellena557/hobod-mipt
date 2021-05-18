ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE gorskajael;

DROP TABLE IF EXISTS kkt_sums;
CREATE TABLE kkt_sums 
AS SELECT content.userInn as user, DAY(from_unixtime(CAST(content.dateTime.ddate/1000 as BIGINT), 'yyyy-MM-dd')) as date, COALESCE(content.totalSum, 0) as userSum FROM kkt_transactions_1
SORT BY user, date;

DROP TABLE IF EXISTS day_sums;
CREATE TABLE day_sums 
AS SELECT user, date, sum(userSum) as daySum FROM kkt_sums 
GROUP BY user, date;

DROP TABLE IF EXISTS max_sums;
CREATE TABLE max_sums 
AS SELECT user, max(daySum) as maxSum FROM day_sums
GROUP BY user;

SELECT day_sums.user, day_sums.date, day_sums.daySum
FROM day_sums INNER JOIN max_sums
ON day_sums.user = max_sums.user AND day_sums.daySum = max_sums.maxSum;
