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
                             feature_unique_key,feature_unique_key_type,
                             feature_eventtime_key,feature_eventtime_key_type,
                             feature_normal_keys):
        cursor = self._con.cursor()
        cursor.execute("use "+feature_store_name+";")
        sql = "alter table  @feature_group_nm@ set tblproperties ('feature_unique_key'='@feature_unique_key_pairs@')".replace("@feature_group_nm@",feature_group_name).replace("@feature_unique_key_pairs@",feature_unique_key+":"+feature_unique_key_type)
        cursor.execute(sql)
        sql = "alter table  @feature_group_nm@ set tblproperties ('feature_eventtime_key'='@feature_eventtime_key_pairs@')".replace("@feature_group_nm@",feature_group_name).replace("@feature_eventtime_key_pairs@",feature_eventtime_key+":"+feature_eventtime_key_type)
        cursor.execute(sql)
        normal_keys=""
        for feature_key in feature_normal_keys:
            normal_keys.append(feature_key[0]+":"+feature_key[1]+",\n")
        sql = "alter table  @feature_group_nm@ set tblproperties ('feature_normal_keys'='@feature_normal_key_pairs@')".replace("@feature_group_nm@",feature_group_name).replace("@feature_normal_key_pairs@",normal_keys)
        cursor.execute(sql)
        self.logger.info("register emr feature group "+feature_group_name + "in "+ feature_store_name+" result:")
        for result in cursor.fetchall():
            self.logger.info(result)


    def create_feature_group(self,
                             feature_store_name,feature_group_name, desc,
                             feature_unique_key,feature_unique_key_type,
                             feature_eventtime_key,feature_eventtime_key_type,
                             feature_normal_keys):
        cursor = self._con.cursor()
        cursor.execute("use "+feature_store_name+";")
        sql="CREATE EXTERNAL TABLE @feature_group_nm@( \n"+
          "`_hoodie_commit_time` string,\n"+
          "`_hoodie_commit_seqno` string,\n"+
          "`_hoodie_record_key` string,\n"+
          "`_hoodie_partition_path` string,\n"+
          "`_hoodie_file_name` string,\n"+
          "@feature_unique_key@,\n"+
          "@feature_normal_keys@)\n"+
        "PARTITIONED BY (\n"+
        "  @feature_partitions@)\n"+
        "ROW FORMAT SERDE\n"+
        "  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n"+
        "STORED AS INPUTFORMAT\n"+
        "  'org.apache.hudi.hadoop.HoodieParquetInputFormat' \n"+
        "OUTPUTFORMAT \n"+
        "  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' \n"+
        "TBLPROPERTIES (@tableProps@)"

        tableProps="'feature_unique_key'='"+feature_unique_key+":"+feature_unique_key_type+"',"
        tableProps=tableProps+"'feature_partition_key='"+feature_partition_key+":"+feature_partition_key_type+"',"
        partition_keys="("+feature_eventtime_key+" "+feature_eventtime_key_type+")"
        normal_keys=""
        tableProps=tableProps+"'feature_normal_keys='("
        for feature_key in feature_normal_keys:
           normal_keys.append(feature_key[0]+" "+feature_key[1]+",\n")
           tableProps.append(feature_key[0]+":"+feature_key[1]+",")
        tableProps.append("')")
        sql=sql.replace("@feature_partitions@",partition_keys)
        sql=sql.replace("@feature_unique_key@",feature_unique_key+" "+feature_unique_key_type)
        sql=sql.replace("@feature_normal_keys@",normal_keys)
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


    def get_feature_groups(feature_store_name,feature_group_name):
        cmd = "line=$(hive -e \"use @db_name@; show tables\") && echo $line".replace("@db_name@",feature_store_name)
        feature_groups = exec_command(cmd,60).split(" ")
        ret_feature_groups=[]
        for feature_group in feature_groups:
            if feature_group.contains(feature_group_name):
               ret_feature_groups.append(feature_group)
        return ret_feature_groups


    def inspect_feature_group(feature_store_name,feature_group_name):
        cmd = "emr_fs_exec.sh inspect "+feature_store_name + " feature_group_name"
        features = exec_command(cmd,60).split(" ")
        return features

    def delete(self, feature_group):
        self._feature_group_api.delete(feature_group)



    def sql(self, query, feature_store_name, dataframe_type, online, read_options):
        if online and self._online_conn is None:
            self._online_conn = self._storage_connector_api.get_online_connector()
        return engine.get_instance().sql(
            query,
            feature_store_name,
            self._online_conn if online else None,
            dataframe_type,
            read_options,
        )

    def append_features(self, feature_group, new_features):
        """Appends features to a feature group."""
        # first get empty dataframe of current version and append new feature
        # necessary to write empty df to the table in order for the parquet schema
        # which is used by hudi to be updated
        df = engine.get_instance().get_empty_appended_dataframe(
            feature_group.read(), new_features
        )

        # perform changes on copy in case the update fails, so we don't leave
        # the user object in corrupted state
        copy_feature_group = fg.FeatureGroup(
            None,
            None,
            None,
            None,
            id=feature_group.id,
            features=feature_group.features + new_features,
        )
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "updateMetadata"
        )

        # write empty dataframe to update parquet schema
        engine.get_instance().save_empty_dataframe(feature_group, df)



