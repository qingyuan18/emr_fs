CREATE EXTERNAL TABLE customer_features( \
  `_hoodie_commit_time` string, \
  `_hoodie_commit_seqno` string, \
  `_hoodie_record_key` string,  \
  `_hoodie_partition_path` string,\
  `_hoodie_file_name` string, \
  'customer_id' string, \
  'city_code'	string, \
  'state_code'  string , \
  'country_code' string,  \
  'dt' timestamp \
   )  \
PARTITIONED BY ( \
  'city_code') \
ROW FORMAT SERDE \
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \
STORED AS INPUTFORMAT \
  'org.apache.hudi.hadoop.HoodieParquetInputFormat' \
OUTPUTFORMAT \
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' \
TBLPROPERTIES ('feature_unique_key'='customer_id','feature_eventtime_key'='dt')