--DROP TABLE IF EXISTS tableMath;
--DROP TABLE IF EXISTS tableEng;
DROP TABLE IF EXISTS tableCombine;

CREATE EXTERNAL TABLE IF NOT EXISTS tableMath (rowkey STRING, math INT, name STRING) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES("hbase.columns.mapping" = "grade:math,grade:name")
TBLPROPERTIES("hbase.table.name" = "100060006_math");

CREATE EXTERNAL TABLE IF NOT EXISTS tableEng (rowkey STRING, eng INT, name STRING) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES("hbase.columns.mapping" = "grade:eng,grade:name")
TBLPROPERTIES("hbase.table.name" = "100060006_eng");

CREATE TABLE tableCombine AS SELECT tableMath.name, tableMath.math, tableEng.eng FROM tableMath JOIN tableEng ON tableMath.name = tableEng.name;
ALTER TABLE tableCombine ADD COLUMNS (avg DOUBLE);
UPDATE tableCombine SET avg=((eng+math)/2);

SELECT * FROM tableCombine;
