#
#   Copyright 2020 Logical Clocks AB
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

import warnings

from emr_fs import engine, client, util, exceptions
from emr_fs import feature_group as fg
from emr_fs.engine import FeatureBaseEngine
from pyhive import hive
import pandas as pd


class FeatureStoreEngine(feature_group_base_engine.FeatureBaseEngine):
    def __init__(self,emr_cluster_id, feature_store_name):
        super().__init__(feature_store_id)
        # cache online feature store connector

    def create_feature_store(self, name,description,s3_store_path):
        con=hive.Connection(host='localhost', port='10000', username='hive')
        cursor=con.cursor()
        sql="create database if not exists @emr_feature_store@ comment 'emr_feature_store for sagemaker' location @DBLocation@;".replace("@emr_feature_store@",name).replace("@DBLocation@",s3_store_path)
        if description  is not None:
           sql.replace("emr_feature_store for sagemaker",description)
        cursor.execute(sql)
        data=pd.DataFrame(cursor.fetchall())
        print(data.head())
        return true



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

    def update_description(self, feature_group, description):
        """Updates the description of a feature group."""
        copy_feature_group = fg.FeatureGroup(
            None,
            None,
            description,
            None,
            id=feature_group.id,
            features=feature_group.features,
        )
        self._feature_group_api.update_metadata(
            feature_group, copy_feature_group, "updateMetadata"
        )


