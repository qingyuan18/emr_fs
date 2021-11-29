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
        join_feature_groups=[],
        join_feature_group_keys={},
        features=[],
        engine_type='spark',
        emr_master_node=None
    ):
        self._feature_store = feature_store
        self._feature_group = feature_group
        self._join_feature_groups = join_feature_groups
        self._join_feature_group_keys = join_feature_group_keys
        self._features = features
        self._sqlSelect = "select "
        self._sqlJoin = ""
        self._sqlWhere = ""
        self._engine = SparkEngine()
        if engine.get_type() == "spark":
           self._engine = FeatureStoreSparkEngine()
        else
           self._engine=FeatureStoreHiveEngine(emr_master_node)

    def show(self,lines:int):
         """Show the first N rows of the Query.
        # Arguments
            n: Number of rows to show.
            online: Show from online storage. Defaults to `False`.
        """
        full_query="select "
        for feature in self._features:
           full_query = full_query + feature.get_feature_group+"."+feature.feature_name + ","
        full_query = full_query + " from " + self._feature_group.get_feature_group_name()
        for  join_feature_group in self._join_feature_groups
            full_query = full_query+ " left join "+join_feature_group.get_feature_group_name() + " on "+self._feature_group.get_feature_group_name + "." + self._feature_group.get_feature_unique_key +"="+self._join_feature_group_keys[join_feature_group.get_feature_group_name()]
            full_query = full_query + ", "
        full_query=" where "+self._sqlWhere
        return self._engine.query(full_query,lines)

    def join(Query query,join_key=""):
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

    def timeQuery(begin_timestamp,end_timestamp):
        self._sql = " "+self._feature_group+"._hoodie_commit_time>='"+begin_timestamp + "' and _hoodie_commit_time <='"+end_timestamp+"'"
        return self



    def show(self, n: int, online: Optional[bool] = False):
        """Show the first N rows of the Query.

        # Arguments
            n: Number of rows to show.
            online: Show from online storage. Defaults to `False`.
        """
        sql_query, online_conn = self._prep_read(online, {})

        return engine.get_instance().show(
            sql_query, self._feature_store_name, n, online_conn
        )

