import json
import humps
from typing import Optional, List, Union

from emr_fs import util, engine
from emr_fs.core import query_constructor_api, storage_connector_api
from emr_fs.constructor import join, filter


class Query:
    def __init__(
        self,
        feature_store=None,
        feature_group=None,
        engine_type='spark',
        emr_master_node=None
    ):
        self._feature_store = feature_store
        self._feature_group = feature_group
        self._join_feature_groups = []
        self._join_feature_group_keys = {}
        self._features = []
        self._sqlSelect = "select "
        self._sqlJoin = ""
        self._sqlWhere = ""
        self._engine = SparkEngine()
        if engine.get_type() == "spark":
           self._engine = FeatureStoreSparkEngine()
        else
           self._engine=FeatureStoreHiveEngine(emr_master_node)

    def select_all(self):
         """select all the feature group dataset.
         """
         self._features.append(self.feature_group._features)
         return self

    def select(self,features=[]):
         """select subset of the feature group dataset.
         """
         self._features.append(features)
         return self


    def pareseSql(self):
        full_query="select "
        for feature in self._features:
           full_query = full_query + feature.get_feature_group+"."+feature.feature_name + ","
        full_query = full_query + " from " + self._feature_group.get_feature_group_name()
        for  join_feature_group in self._join_feature_groups
            full_query = full_query+ " left join "+join_feature_group.get_feature_group_name() +
                                     " on "+self._feature_group.get_feature_group_name + "." + self._feature_group.get_feature_unique_key +"="+self._join_feature_group_keys[join_feature_group.get_feature_group_name()]
            full_query = full_query + ","
        full_query=" where "+self._sqlWhere
        return full_query

    def show(self,lines:int):
         """Show the first N rows of the Query.
        # Arguments
            n: Number of rows to show.
            online: Show from online storage. Defaults to `False`.
        """
        full_query = self.pareseSql()
        return self._engine.show(full_query,lines)



    def create_training_dataset(self,
            name = “userProfile dataset",
            data_format = "tfrecord",
            startDt="2020-10-20 07:31:38",
            endDt= "2020-10-20 07:34:11“,
            outputLoc = "s3://emr_fs/output/userProfile"):
        timeQuery(startDt,engine)
        full_query = self.pareseSql()
        df = self._engine.query(full_query)
        transfer = TransformationFunction(self,data_format,outputLoc)
        transfer.save(data_format,df,None)



    def join(self,Query query,join_key=None):
        """join other query
        # Arguments
            query: another query instance.
            join_key:feature key to join each other.
        """
        self._join_feature_groups.append(query.get_feature_group)
        self._join_feature_group_keys[query.get_feature_group]=join_key
        if query._sqlWhere is not None:
           self._sqlSelect = " and " + self._sqlWhere
        return self

    def timeQuery(self,begin_timestamp,end_timestamp):
        self._sqlWhere = " "+self._feature_group+"._hoodie_commit_time>='"+begin_timestamp + "' and _hoodie_commit_time <='"+end_timestamp+"'"
        return self



