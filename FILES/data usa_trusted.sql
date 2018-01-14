CREATE EXTERNAL TABLE DATAUSA_HIGH_SCHOOL (
  year int,
  geo_name string,
  geo string,
  high_school_graduation decimal(4,3)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION "/kcbda/RAW/DATAUSA_HIGH_SCHOOL"
tblproperties("skip.header.line.count"="1");


CREATE EXTERNAL TABLE DATAUSA_AVG (
  year int,
  geo_name string,
  geo string,
  avg_wage decimal(6,1)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION "/kcbda/RAW/DATAUSA_AVG"
tblproperties("skip.header.line.count"="1");


CREATE EXTERNAL TABLE DATAUSA (
  high_school_graduation decimal(4,3),
  avg_wage decimal(6,1)
)
PARTITIONED by (DE_ESTADO string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "|"
STORED AS ORC
LOCATION "/kcbda/TRUSTED/DATAUSA";

INSERT INTO TABLE DATAUSA PARTITION (DE_ESTADO)
SELECT
DHS.high_school_graduation,
DAVG.avg_wage,
UPPER(DS.DE_STATE) AS DE_ESTADO
FROM kcbda_raw.DATAUSA_STATE AS DS
INNER JOIN kcbda_raw.DATAUSA_HIGH_SCHOOL DHS ON DS.ID_STATE = DHS.geo
INNER JOIN kcbda_raw.DATAUSA_AVG DAVG ON DS.ID_STATE = DAVG.geo;

