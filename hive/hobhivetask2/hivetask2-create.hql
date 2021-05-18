ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;

SET mapred.input.dir.recursive=true;

SET hive.mapred.supports.subdirectories=true;

USE gorskajael;

DROP TABLE IF EXISTS kkt_transactions_text;
CREATE TABLE kkt_transactions_text 
STORED AS TEXTFILE
AS SELECT content.userInn as user, content.totalSum as userSum FROM kkt_transactions_1 WHERE subtype = "receipt";

DROP TABLE IF EXISTS kkt_transactions_orc;
CREATE TABLE kkt_transactions_orc 
STORED AS ORC
AS SELECT content.userInn as user, content.totalSum as userSum FROM kkt_transactions_1 WHERE subtype = "receipt";

DROP TABLE IF EXISTS kkt_transactions_parquet;
CREATE TABLE kkt_transactions_parquet 
STORED AS PARQUET
AS SELECT content.userInn as user, content.totalSum as userSum FROM kkt_transactions_1 WHERE subtype = "receipt";

DROP TABLE IF EXISTS kkt_transactions_results;
CREATE external TABLE kkt_transactions_results (
	formatType STRING,
	time STRING
);
INSERT INTO TABLE kkt_transactions_results
    VALUES ('Text', '44.35s'), ('ORC', '43.51s'), ('Parquet', '49.39s');
