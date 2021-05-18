ADD jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;

SET hive.cli.print.header=false;

SET mapred.input.dir.recursive=true;

SET hive.mapred.supports.subdirectories=true;

USE gorskajael;

DROP TABLE IF EXISTS kkt_transactions_1;

CREATE EXTERNAL TABLE kkt_transactions_1 (  
	fsId STRING,
    kktRegId STRING,
	subtype STRING,
	receiveDate STRUCT<
		ddate : BIGINT
	>,
	protocolVersion INT,
	ofdId STRING,
	protocolSubversion INT,
	content STRUCT<
		receiptCode: TINYINT,
		bsoCode: TINYINT,
		user: STRING,
		userInn: STRING,
		requestNumber: INT,
		dateTime: STRUCT<
			ddate : BIGINT
		>,
		shiftNumber: INT,
		operationType: INT,
		taxationType: INT,
		operator: STRING,
		fiscalDriveNumber: STRING,
		retailPlaceAddress: STRING,
		buyerAddress: STRING,
		senderAddress: STRING,
		addressToCheckFiscalSign: STRING,
		items: ARRAY<STRING>,
		stornoItems: ARRAY<STRING>,
		paymentAgentRemuneration: BIGINT,
		paymentAgentPhone: STRING,
		paymentSubagentPhone: STRING,
		operatorPhoneToReceive: STRING,
		operatorPhoneToTransfer: STRING,
		bankAgentPhone: STRING,
		bankSubagentPhone: STRING,
		bankAgentOperation: STRING,
		bankSubagentOperation: STRING,
		bankAgentRemuneration: BIGINT,
		operatorName: STRING,
		operatorAddress: STRING,
		operatorInn: STRING,
		modifiers: ARRAY<STRING>,
		nds18: BIGINT,
		nds10: BIGINT,
		nds0: BIGINT,
		ndsNo: BIGINT,
		ndsCalculated18: BIGINT,
		ndsCalculated10: BIGINT,
		totalSum: BIGINT,
		cashTotalSum: BIGINT,
		ecashTotalSum: BIGINT,
		fiscalDocumentNumber: INT,
		fiscalSign: BIGINT,
		properties: ARRAY<STRING>,
		rawData: STRING
	>,
	documentId INT
)

ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
	"ignore.malformed.json" = "true", 
	"mapping.ddate" = "$date"
)

LOCATION '/data/hive/fns2';

SELECT * FROM kkt_transactions_1 LIMIT 50;
