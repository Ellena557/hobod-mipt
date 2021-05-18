ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE gorskajael;

SELECT avg_evening_sums.user, round(avg_morning_sums.avgMorningSum) as avgMorningSum, round(avg_evening_sums.avgEveningSum) as avgEveningSum
FROM (
    SELECT content.userInn as user, AVG(COALESCE(content.totalSum, 0)) as avgEveningSum FROM kkt_transactions_1
    WHERE HOUR(from_unixtime(CAST(content.dateTime.ddate/1000 as BIGINT))) >= 13
    GROUP BY content.userInn
) avg_evening_sums INNER JOIN (
    SELECT content.userInn as user, AVG(COALESCE(content.totalSum, 0)) as avgMorningSum FROM kkt_transactions_1
    WHERE HOUR(from_unixtime(CAST(content.dateTime.ddate/1000 as BIGINT))) < 13
    GROUP BY content.userInn
) avg_morning_sums
ON avg_evening_sums.user = avg_morning_sums.user
WHERE avg_morning_sums.avgMorningSum > avg_evening_sums.avgEveningSum
SORT BY avgMorningSum
LIMIT 50;
