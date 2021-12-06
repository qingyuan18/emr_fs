#
#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

import json
import datetime
import importlib.util
import numpy as np
import pandas as pd

# in case importing in %%local
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.rdd import RDD
    from pyspark.sql.column import Column, _to_java_column
    from pyspark.sql.functions import struct, concat, col, lit
except ImportError:
    pass
from emr_fs.exceptions import FeatureStoreException
from emr_fs.engine import hudi_engine
from emr_fs.constructor import query


class FeatureStoreSparkEngine:

    APPEND = "append"
    OVERWRITE = "overwrite"

    def __init__(self):
        self._spark_session = SparkSession.builder.appName("emr_feature_store app").enableHiveSupport().getOrCreate()
        self._spark_context = self._spark_session.sparkContext
        self._jvm = self._spark_context._jvm

        self._spark_session.conf.set("hive.exec.dynamic.partition", "true")
        self._spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self._spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")


    def create_feature_store(name, desc, location):
        sql="create database if not exists @emr_feature_store@ comment 'emr_feature_store for sagemaker' location @DBLocation@;".replace("@emr_feature_store@",name).replace("@DBLocation@",location)
        if description  is not None:
           sql.replace("emr_feature_store for sagemaker",desc)
        self._spark_session.sql(sql)
        self.logger.info("created emr feature store: "+name)

    def get_feature_group(feature_store_name,feature_group_name):
        sql = "show create table "+feature_store_name+ "."+feature_group_name+";"
        df=self._spark_session.sql(sql)
        feature_group_info = ""
        for line in df.collect():
            feature_group_info = feature_group_info+line + "\n"
        self.logger.info("get feature groups:"+feature_group_info)
        return feature_group_info

    def register_feature_group(self,
                                 feature_store_name,feature_group_name, desc,
                                 feature_unique_key,
                                 feature_eventtime_key):
        self._spark_session.sql("use "+feature_store_name+";")
        sql = "alter table  @feature_group_nm@ set tblproperties ('feature_unique_key'='@feature_unique_key@')".replace("@feature_group_nm@",feature_group_name).replace("@feature_unique_key@",feature_unique_key)
        self._spark_session.sql(sql)
        sql = "alter table  @feature_group_nm@ set tblproperties ('feature_eventtime_key'='@feature_eventtime_key@')".replace("@feature_group_nm@",feature_group_name).replace("@feature_eventtime_key@",feature_eventtime_key)
        df=self._spark_session.sql(sql)
        self.logger.info("register emr feature group "+feature_group_name + "in "+ feature_store_name+" result:")
        for line in df.collect():
            self.logger.info(line)


    def create_feature_group(self,
                                 feature_store_name,feature_group_name, desc,
                                 feature_unique_key,
                                 feature_eventtime_key,
                                 features):

        self._spark_session.sql("use "+feature_store_name+";")
        sql="""CREATE EXTERNAL TABLE @feature_group_nm@(
          _hoodie_commit_time string,
          _hoodie_commit_seqno string,
          _hoodie_record_key string,
          _hoodie_partition_path string,
          _hoodie_file_name string,
          "@features@)
        PARTITIONED BY (
          @feature_partitions@)
        ROW FORMAT SERDE
          'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT
          'org.apache.hudi.hadoop.HoodieParquetInputFormat'
        OUTPUTFORMAT
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        TBLPROPERTIES (@tableProps@)"""

        tableProps="'feature_unique_key'='"+feature_unique_key"',"
        tableProps=tableProps+"'feature_partition_key='"+feature_partition_key+"'"
        partition_keys=feature_eventtime_key+" "+feature_eventtime_key_type
        columns=""
        for feature in features:
           columns.append(feature[0]+" "+feature[1]+",\n")
        sql=sql.replace("@feature_normal_keys@",columns)
        sql=sql.replace("@tableProps@",tableProps)
        df=self._spark_session.sql(sql)
        self.logger.info("create emr feature group "+feature_group_name + "in "+ feature_store_name+" result:")
        for result in df.collect():
           self.logger.info(result)


    def show(full_query,lines):
        if lines != 0:
           self._spark_session.sql(full_query).show(lines)
        else :
           self._spark_session.sql(full_query).show()

    def query(full_query):
        return self._spark_session.sql(full_query)


    def append_features(self, feature_store_name, feature_group_name, new_feature_key,new_feature_key_type):
        sql = "alter table "+feature_store_name+"."+feature_group_name+" add column ('"+new_feature_key+","+new_feature_key_type+"');"
        try:
           df=self._spark_session.sql(sql)
           self.logger.info("add new_feature :"+new_feature_key+":"+new_feature_key_type+" in "+feature_group_name+"."+fe
        exceptions Exception as e:
           raise FeatureStoreException(
                           "Error add new feature:" + str(e)
                       )

    def save_s3_dataset(
            self,
            feature_group_name,
            feature_group_location,
            source_s3_location,
            operation,
            feature_unique_key,
            feature_eventtime_key
        ):
            hudi_engine = HudiEngine(feature_group,self._spark_context,self._spark_session)
            hudi_options = hudi_engine._setup_hudi_write_opts(operation, primary_key=feature_unique_key,partition_key=feature_eventtime_key,pre_combine_key=feature_eventtime_key)
            dataframe = _spark_session.read.format("org.apache.hudi").load(source_s3_location)
            try:
                dataframe.write.format("hudi"). \
                     	options(**hudi_options). \
                     	mode("append"). \
                     	save(feature_group_location)
            exceptions Exception as e::
                raise FeatureStoreException(
                    "Error writing to offline feature group :" + str(e)
                )


    def save_dataframe(
        self,
        feature_group_name,
        feature_group_location,
        dataframe,
        operation,
        feature_unique_key,
        feature_eventtime_key
    ):
        hudi_engine = HudiEngine(feature_group,self._spark_context,self._spark_session)
        hudi_options = hudi_engine._setup_hudi_write_opts(operation, primary_key=feature_unique_key,partition_key=feature_eventtime_key,pre_combine_key=feature_eventtime_key)
        try:
            dataframe.write.format("hudi"). \
                 	options(**hudi_options). \
                 	mode("append"). \
                 	save(feature_group_location)
        exceptions Exception as e::
            raise FeatureStoreException(
                "Error writing to offline feature group :" + str(e)
            )



    #def _sychronize_online_feature_group(self, feature_group_name, dataframe, sagemaker_fs):
    #   pass





