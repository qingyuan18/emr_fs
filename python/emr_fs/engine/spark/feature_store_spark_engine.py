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
        self._spark_session = SparkSession.builder.getOrCreate()
        self._spark_context = self._spark_session.sparkContext
        self._jvm = self._spark_context._jvm

        self._spark_session.conf.set("hive.exec.dynamic.partition", "true")
        self._spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self._spark_session.conf.set("spark.sql.hive.convertMetastoreParquet", "false")


    def show(full_query,lines):
        if lines != 0:
           self._spark_context.sql(full_query).show(lines)
        else :
           self._spark_context.sql(full_query).show()

    def query(full_query):
        return self._spark_context.sql(full_query)



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
        exceptions:
            raise FeatureStoreException(
                "Error writing to offline feature group."
            )



    #def _sychronize_online_feature_group(self, feature_group_name, dataframe, sagemaker_fs):
    #   pass





