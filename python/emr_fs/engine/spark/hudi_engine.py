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

from emr_fs import util
from emr_fs.core import feature_group_api


class HudiEngine:
    HUDI_SPARK_FORMAT = "org.apache.hudi"
    HUDI_TABLE_NAME = "hoodie.table.name"
    HUDI_TABLE_STORAGE_TYPE = "hoodie.datasource.write.storage.type"
    HUDI_TABLE_OPERATION = "hoodie.datasource.write.operation"
    HUDI_RECORD_KEY = "hoodie.datasource.write.recordkey.field"
    HUDI_PARTITION_FIELD = "hoodie.datasource.write.partitionpath.field"
    HUDI_PRECOMBINE_FIELD = "hoodie.datasource.write.precombine.field"

    HUDI_HIVE_SYNC_ENABLE = "hoodie.datasource.hive_sync.enable"
    HUDI_HIVE_SYNC_TABLE = "hoodie.datasource.hive_sync.table"
    HUDI_HIVE_SYNC_DB = "hoodie.datasource.hive_sync.database"
    HUDI_HIVE_SYNC_JDBC_URL = "hoodie.datasource.hive_sync.jdbcurl"
    HUDI_HIVE_SYNC_PARTITION_FIELDS = "hoodie.datasource.hive_sync.partition_fields"
    HUDI_HIVE_SYNC_SUPPORT_TIMESTAMP = "hoodie.datasource.hive_sync.support_timestamp"

    HUDI_KEY_GENERATOR_OPT_KEY = "hoodie.datasource.write.keygenerator.class"
    HUDI_COMPLEX_KEY_GENERATOR_OPT_VAL = "org.apache.hudi.keygen.CustomKeyGenerator"
    HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY = (
        "hoodie.datasource.hive_sync.partition_extractor_class"
    )
    DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL = (
        "org.apache.hudi.hive.MultiPartKeysValueExtractor"
    )
    HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL = (
        "org.apache.hudi.hive.NonPartitionedExtractor"
    )
    HUDI_COPY_ON_WRITE = "COPY_ON_WRITE"
    HUDI_BULK_INSERT = "bulk_insert"
    HUDI_INSERT = "insert"
    HUDI_UPSERT = "upsert"
    HUDI_QUERY_TYPE_OPT_KEY = "hoodie.datasource.query.type"
    HUDI_QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
    HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"
    HUDI_BEGIN_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.begin.instanttime"
    HUDI_END_INSTANTTIME_OPT_KEY = "hoodie.datasource.read.end.instanttime"
    PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class"
    PAYLOAD_CLASS_OPT_VAL = "org.apache.hudi.common.model.EmptyHoodieRecordPayload"
    HUDI_WRITE_INSERT_DROP_DUPLICATES = "hoodie.datasource.write.insert.drop.duplicates"
    HUDI_SCHEMA_MERGE = "hoodie.mergeSchema"

    def __init__(
        self,
        feature_group,
        spark_context,
        spark_session,
    ):
        self.
        self._feature_group = feature_group
        self._spark_context = spark_context
        self._spark_session = spark_session

        self._partition_key = (
            ",".join(feature_group._feature_eventtime_key)
            if len(feature_group._feature_eventtime_key) >= 1
            else ""
        )

        self._pre_combine_key = (
            feature_group._feature_eventtime_key
            if feature_group._feature_eventtime_key
            else feature_group.primary_key[0]
        )

        self._record_key = feature_group._feature_unique_key




    def _setup_hudi_write_opts(self, operation):
        _jdbc_url = self._get_conn_str()
        hudi_options = {
            self.HUDI_PRECOMBINE_FIELD: self._pre_combine_key[0]
            self.HUDI_RECORD_KEY: self._primary_key,
            self.HUDI_PARTITION_FIELD: self._partition_path,
            self.HUDI_TABLE_NAME: self._feature_group._feature_group_name,
            self.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY: self.DEFAULT_HIVE_PARTITION_EXTRACTOR_CLASS_OPT_VAL
            if len(self._partition_key) >= 1
            else self.HIVE_NON_PARTITION_EXTRACTOR_CLASS_OPT_VAL,
            self.HUDI_HIVE_SYNC_ENABLE: "true",
            self.HUDI_HIVE_SYNC_TABLE: self._table_name,
            self.HUDI_HIVE_SYNC_JDBC_URL: _jdbc_url,
            self.HUDI_HIVE_SYNC_DB: self._feature_store_name,
            self.HUDI_HIVE_SYNC_PARTITION_FIELDS: self._partition_key,
            self.HUDI_TABLE_OPERATION: operation,
            self.HUDI_SCHEMA_MERGE:"true"
        }

        if operation.lower() in [self.HUDI_BULK_INSERT, self.HUDI_INSERT]:
            hudi_options[self.HUDI_WRITE_INSERT_DROP_DUPLICATES] = "true"

        if write_options:
            hudi_options.update(write_options)
        return hudi_options

    def _setup_hudi_read_opts(self, start_timestamp, end_timestamp):

        hudi_options = {
            self.HUDI_QUERY_TYPE_OPT_KEY: self.HUDI_QUERY_TYPE_INCREMENTAL_OPT_VAL,
            self.HUDI_BEGIN_INSTANTTIME_OPT_KEY: start_timestamp,
            self.HUDI_END_INSTANTTIME_OPT_KEY: end_timestamp,
        }
        return hudi_options



    def _get_conn_str(self):
        credentials = {
            "sslTrustStore": client.get_instance()._get_jks_trust_store_path(),
            "trustStorePassword": client.get_instance()._cert_key,
            "sslKeyStore": client.get_instance()._get_jks_key_store_path(),
            "keyStorePassword": client.get_instance()._cert_key,
        }

        return self._connstr + ";".join(
            ["{}={}".format(option[0], option[1]) for option in credentials.items()]
        )
