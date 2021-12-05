import warnings

from emr_fs import engine, client, util, exceptions
from emr_fs import feature_group as fg
from emr_fs.engine.feature_group_base_engine import FeatureBaseEngine
from pyhive import hive
from util import *
import pandas as pd
from emr_fs.logger import Log



class FeatureStoreHiveEngine(feature_group_base_engine.FeatureBaseEngine):
    def __init__(self,emr_master_node):
        self.logger = Log("file")
        self._master_node = emr_master_node
        super().__init__()

    def __enter__(self):
        self._con = hive.Connection(host=self._master_node, port='10000', username='hive')

    def __exit__(self):
        self._con.close()


    def register_feature_group(self,
                             feature_store_name,feature_group_name, desc,
                             feature_unique_key,
                             feature_eventtime_key):
        cursor = self._con.cursor()
        cursor.execute("use "+feature_store_name+";")
        sql = "alter table  @feature_group_nm@ set tblproperties ('feature_unique_key'='@feature_unique_key@')".replace("@feature_group_nm@",feature_group_name).replace("@feature_unique_key@",feature_unique_key)
        cursor.execute(sql)
        sql = "alter table  @feature_group_nm@ set tblproperties ('feature_eventtime_key'='@feature_eventtime_key@')".replace("@feature_group_nm@",feature_group_name).replace("@feature_eventtime_key@",feature_eventtime_key)
        cursor.execute(sql)
        self.logger.info("register emr feature group "+feature_group_name + "in "+ feature_store_name+" result:")
        for result in cursor.fetchall():
            self.logger.info(result)


    def create_feature_group(self,
                             feature_store_name,feature_group_name, desc,
                             feature_unique_key,
                             feature_eventtime_key,
                             features):
        cursor = self._con.cursor()
        cursor.execute("use "+feature_store_name+";")
        sql="CREATE EXTERNAL TABLE @feature_group_nm@( \n"+
          "`_hoodie_commit_time` string,\n"+
          "`_hoodie_commit_seqno` string,\n"+
          "`_hoodie_record_key` string,\n"+
          "`_hoodie_partition_path` string,\n"+
          "`_hoodie_file_name` string,\n"+
          "@features@)\n"+
        "PARTITIONED BY (\n"+
        "  @feature_partitions@)\n"+
        "ROW FORMAT SERDE\n"+
        "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n"+
        "STORED AS INPUTFORMAT\n"+
        "  'org.apache.hudi.hadoop.HoodieParquetInputFormat' \n"+
        "OUTPUTFORMAT \n"+
        "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' \n"+
        "TBLPROPERTIES (@tableProps@)"

        tableProps="'feature_unique_key'='"+feature_unique_key"',"
        tableProps=tableProps+"'feature_partition_key='"+feature_partition_key+"'"
        partition_keys=feature_eventtime_key+" "+feature_eventtime_key_type
        columns=""
        for feature in features:
           columns.append(feature[0]+" "+feature[1]+",\n")
        sql=sql.replace("@feature_normal_keys@",columns)
        sql=sql.replace("@tableProps@",tableProps)
        cursor.execute(sql)
        self.logger.info("create emr feature group "+feature_group_name + "in "+ feature_store_name+" result:")
        for result in cursor.fetchall():
           self.logger.info(result)


    def create_feature_store(self, name,desc,location):
        cursor=self._con.cursor()
        sql="create database if not exists @emr_feature_store@ comment 'emr_feature_store for sagemaker' location @DBLocation@;".replace("@emr_feature_store@",name).replace("@DBLocation@",s3_store_path)
        if description  is not None:
           sql.replace("emr_feature_store for sagemaker",description)
        cursor.execute(sql)
        #data=pd.DataFrame(cursor.fetchall())
        self.logger.info("created emr feature store: "+name)


    def get_feature_group(feature_store_name,feature_group_name):
        cursor = self._con.cursor()
        sql = "show create table "+feature_store_name+ "."+feature_group_name+";"
        cursor.execute(sql)
        results = cursor.fetchall()
        self.logger.info("get feature groups:"+results)
        return results




    def append_features(self, feature_store_name, feature_group_name, new_feature_key,new_feature_key_type):
        cursor = self._con.cursor()
        sql = "alter table "+feature_store_name+"."+feature_group_name+" add column ('"+new_feature_key+","+new_feature_key_type+"');"
        cursor.execute(sql)
        results = cursor.fetchall()
        self.logger.info("add new_feature :"+new_feature_key+":"+new_feature_key_type+" in "+feature_group_name+"."+feature_group_name)



