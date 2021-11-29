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

        if importlib.util.find_spec("pydoop"):
            # If we are on Databricks don't setup Pydoop as it's not available and cannot be easily installed.
            util.setup_pydoop()

    def query(full_query,lines):
        self._spark_context.sql(full_query).show(lines)


    def register_on_demand_temporary_table(self, on_demand_fg, alias):
        on_demand_dataset = on_demand_fg.storage_connector.read(
            on_demand_fg.query,
            on_demand_fg.data_format,
            on_demand_fg.options,
            on_demand_fg.storage_connector._get_path(on_demand_fg.path),
        )

        on_demand_dataset.createOrReplaceTempView(alias)
        return on_demand_dataset

    def register_hudi_temporary_table(
        self, hudi_fg_alias, feature_store_id, feature_store_name, read_options
    ):
        hudi_engine_instance = hudi_engine.HudiEngine(
            feature_store_id,
            feature_store_name,
            hudi_fg_alias.feature_group,
            self._spark_context,
            self._spark_session,
        )
        hudi_engine_instance.register_temporary_table(
            hudi_fg_alias.alias,
            hudi_fg_alias.left_feature_group_start_timestamp,
            hudi_fg_alias.left_feature_group_end_timestamp,
            read_options,
        )

    def save_dataframe(
        self,
        feature_group,
        dataframe,
        operation,
        online_enabled,
        storage,
        offline_write_options,
        online_write_options,
        validation_id=None,
    ):

        if storage == "offline" or not online_enabled:
            self._save_offline_dataframe(
                feature_group,
                dataframe,
                operation,
                offline_write_options,
                validation_id,
            )
        elif storage == "online":
            self._save_online_dataframe(feature_group, dataframe, online_write_options)
        elif online_enabled and storage is None:
            self._save_offline_dataframe(
                feature_group,
                dataframe,
                operation,
                offline_write_options,
            )
            self._save_online_dataframe(feature_group, dataframe, online_write_options)
        else:
            raise FeatureStoreException(
                "Error writing to offline and online feature store."
            )

    def save_stream_dataframe(
        self,
        feature_group,
        dataframe,
        query_name,
        output_mode,
        await_termination,
        timeout,
        write_options,
    ):
        serialized_df = self._online_fg_to_avro(
            feature_group, self._encode_complex_features(feature_group, dataframe)
        )
        if query_name is None:
            query_name = (
                "insert_stream_"
                + feature_group._online_topic_name
                + "_"
                + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            )

        query = (
            serialized_df.writeStream.outputMode(output_mode)
            .format(self.KAFKA_FORMAT)
            .option(
                "checkpointLocation",
                "/Projects/"
                + client.get_instance()._project_name
                + "/Resources/"
                + query_name
                + "-checkpoint",
            )
            .options(**write_options)
            .option("topic", feature_group._online_topic_name)
            .queryName(query_name)
            .start()
        )

        if await_termination:
            query.awaitTermination(timeout)

        return query

    def _sychronize_online_feature_group(self, feature_group_name, dataframe, sagemaker_fs):
       pass



    def write_options(self, data_format, provided_options):
        if data_format.lower() == "tfrecords":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "tfrecord":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "csv":
            options = dict(delimiter=",", header="true")
            options.update(provided_options)
        elif data_format.lower() == "tsv":
            options = dict(delimiter="\t", header="true")
            options.update(provided_options)
        else:
            options = {}
            options.update(provided_options)
        return options

    def read_options(self, data_format, provided_options):
        if data_format.lower() == "tfrecords":
            options = dict(recordType="Example", **provided_options)
            options.update(provided_options)
        elif data_format.lower() == "tfrecord":
            options = dict(recordType="Example")
            options.update(provided_options)
        elif data_format.lower() == "csv":
            options = dict(delimiter=",", header="true", inferSchema="true")
            options.update(provided_options)
        elif data_format.lower() == "tsv":
            options = dict(delimiter="\t", header="true", inferSchema="true")
            options.update(provided_options)
        else:
            options = {}
            options.update(provided_options)
        return options

    def parse_schema_feature_group(self, dataframe):
        return [
            feature.Feature(
                feat.name.lower(),
                feat.dataType.simpleString(),
                feat.metadata.get("description", ""),
            )
            for feat in dataframe.schema
        ]



    def training_dataset_schema_match(self, dataframe, schema):
        schema_sorted = sorted(schema, key=lambda f: f.index)
        insert_schema = dataframe.schema
        if len(schema_sorted) != len(insert_schema):
            raise SchemaError(
                "Schemas do not match. Expected {} features, the dataframe contains {} features".format(
                    len(schema_sorted), len(insert_schema)
                )
            )

        i = 0
        for feat in schema_sorted:
            if feat.name != insert_schema[i].name.lower():
                raise SchemaError(
                    "Schemas do not match, expected feature {} in position {}, found {}".format(
                        feat.name, str(i), insert_schema[i].name
                    )
                )

            i += 1



    def is_spark_dataframe(self, dataframe):
        if isinstance(dataframe, DataFrame):
            return True
        return False

    def get_empty_appended_dataframe(self, dataframe, new_features):
        dataframe = dataframe.limit(0)
        for f in new_features:
            dataframe = dataframe.withColumn(f.name, lit(None).cast(f.type))
        return dataframe

    def save_empty_dataframe(self, feature_group, dataframe):
        """Wrapper around save_dataframe in order to provide no-op in hive engine."""
        self.save_dataframe(
            feature_group,
            dataframe,
            "upsert",
            feature_group.online_enabled,
            "offline",
            {},
            {},
        )



