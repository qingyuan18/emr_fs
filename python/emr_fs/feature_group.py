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


import pandas as pd
import numpy as np
import avro.schema

from emr_fs import  engine, feature, user, storage_connector as sc
from emr_fs.engine.hive.feature_store_hive_engine import FeatureStoreHiveEngine
from emr_fs.engine.spark.feature_store_spark_engine import FeatureStoreSparkEngine



class FeatureGroup:
    def __init__(self, feature_store,feature_group_name,feature_group_desc,feature_unique_key,feature_eventtime_key,features):
        self._feature_store = feature_store
        self._feature_group_name = feature_group_name
        self._feature_group_desc = feature_group_desc
        self._feature_unique_key = feature_unique_key
        self._feature_eventtime_key = feature_eventtime_key
        self._features = features
        self._query = None


    def delete(self):
        with FeatureStoreSparkEngine(emr_master_node) as engine:
             engine.delete(self.feature_group_name)

    def select_all(self):
        """Select all features in the feature group and return a query object.
        The query can be used to construct joins of feature groups or create a
        training dataset immediately.
        # Returns
            `Query`. A query object with all features of the feature group.
        """

           self._query.select_all()
        return self._query

    def timeQuery(self,beginTimeStamp,endTimeStamp):
        if self._query is None:
           self._query = Query(self._feature_store.get_feature_store_name(),self,'spark',None)
           self._query.timeQuery(beginTimeStamp,endTimeStamp)
        return self._query

    def select(self, features:= []):
        """Select a subset of features of the feature group and return a query object.
        """
       if self._query is None:
          self._query = Query(self.feature_store,self,'spark',None)
       return self.query.select(features)

    def ingestion(self,dataframe):
       """use spark engine(which will use hudi engine internal) to ingest  into feature group"""
       feature_group_location = self._feature_store.s3_store_path()
       with FeatureStoreSparkEngine() as engine:
            engine.save_dataframe(
                           self._feature_group_name,
                           feature_group_location,
                           dataframe,
                           "append",
                           self._feature_unique_key,
                           self._feature_eventtime_key)

    def ingestion(self,source_dataset_location):
           """use spark engine(which will use hudi engine internal) to ingest  into feature group"""
       feature_group_location = self._feature_store.s3_store_path()+"/"+self._feature_group_name+"/"
       with FeatureStoreSparkEngine() as engine:
            engine.save_s3_dataset(
                           self._feature_group_name,
                           feature_group_location,
                           source_dataset_location,
                           "overwrite",
                           self._feature_unique_key,
                           self._feature_eventtime_key)


    def create_training_dataset(self,
            name ,
            data_format = "tfrecord",
            startDt,
            endDt,
            outputLoc ):
        query = Query(self.feature_store.get_feature_store_name(),self,'spark',None)
        query.create_training_dataset(name,data_format,startDt,endDt,outputLoc)


    def get_feature(self, name: str):
        """Retrieve a `Feature` object from the schema of the feature group.
        Returns:
            [type]: [description]
        """
        try:
            return self._features.__getitem__(name)
        except KeyError:
            raise FeatureStoreException(
                f"'FeatureGroup' object has no feature called '{name}'."
            )




    def __getattr__(self, name):
        try:
            return self.__getitem__(name)
        except KeyError:
            raise AttributeError(
                f"'FeatureGroup' object has no attribute '{name}'. "
                "If you are trying to access a feature, fall back on "
                "using the `get_feature` method."
            )

    def __getitem__(self, name):
        if not isinstance(name, str):
            raise TypeError(
                f"Expected type `str`, got `{type(name)}`. "
                "Features are accessible by name."
            )
        feature = [f for f in self.__getattribute__("_features") if f.name == name]
        if len(feature) == 1:
            return feature[0]
        else:
            raise KeyError(f"'FeatureGroup' object has no feature called '{name}'.")


    @property
    def primary_key(self):
        """List of features building the primary key."""
        return self._primary_key

    @primary_key.setter
    def primary_key(self, new_primary_key):
        self._primary_key = [pk.lower() for pk in new_primary_key]








