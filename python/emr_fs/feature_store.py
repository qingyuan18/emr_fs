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

import numpy
import datetime
from typing import Optional, Union, List, Dict, TypeVar
from emr_fs.transformation_function import TransformationFunction
from emr_fs.engine.hive import FeatureStoreEngine
from emr_fs.statistics_config import StatisticsConfig


class FeatureStore:
    def __init__(
        self,
        emr_cluster_id,
        emr_master_node,
        featurestore_name,
        s3_store_path,
        featurestore_description,
    ):
        self._emr_cluster_id = emr_cluster_id
        self._emr_master_node= emr_master_node
        self._name = featurestore_name
        self._description = featurestore_description
        self._s3_store_path = s3_store_path
        self._feature_store_engine = FeatureStoreEngine(self._name)
        self._feature_group_engine = feature_group_engine.FeatureGroupEngine(self._name)
        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(self._name)
        )



    def get_feature_groups(self, name: str):
        """Get a list of all feature group entity from the feature store.
        # Arguments
            name: Name of the feature group to get.
        # Returns
            `FeatureGroup`: List of feature group metadata objects.
        # Raises
            `FeatureStoreException`: If unable to retrieve feature group from the feature store.
        """
        return self._feature_store_engine.get_feature_groups(name)




    def create_feature_store(self):
            """Create a feature store db in hive metadata."""
        self._feature_store_engine.create_feature_store(
             master_node=self._emr_master_node,
             name=self._name,
             desc=self._description,
             location=self._s3_store_path)



    def create_feature_group(
        self,
        name: str,
        description: Optional[str] = "",
        partition_key: Optional[List[str]] = [],
        primary_key: Optional[List[str]] = [],
        hudi_precombine_key: Optional[str] = None,
        features: Optional[List[feature.Feature]] = [],
    ):
        """Create a feature group metadata object.
        # Returns
            `FeatureGroup`. The feature group metadata object.
        """
        return feature_group.FeatureGroup(
            name=name,
            version=version,
            description=description,
            online_enabled=online_enabled,
            time_travel_format=time_travel_format,
            partition_key=partition_key,
            primary_key=primary_key,
            hudi_precombine_key=hudi_precombine_key,
            featurestore_id=self._id,
            featurestore_name=self._name,
            features=features,
            statistics_config=statistics_config,
            validation_type=validation_type,
            expectations=expectations,
            event_time=event_time,
        )



    def create_training_dataset(
        self,
        name: str,
        version: Optional[int] = None,
        description: Optional[str] = "",
        data_format: Optional[str] = "tfrecords",
        coalesce: Optional[bool] = False,
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        splits: Optional[Dict[str, float]] = {},
        location: Optional[str] = "",
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        label: Optional[List[str]] = [],
        transformation_functions: Optional[Dict[str, TransformationFunction]] = {},
    ):
        """Create a training dataset metadata object.

        !!! info "Data Formats"
            The feature store currently supports the following data formats for
            training datasets:

            1. tfrecord
            2. csv
            3. tsv
            4. parquet
            5. avro
            6. orc

            Currently not supported petastorm, hdf5 and npy file formats.
        # Returns:
            `TrainingDataset`: The training dataset metadata object.
        """
        return training_dataset.TrainingDataset(
            name=name,
            version=version,
            description=description,
            data_format=data_format,
            storage_connector=storage_connector,
            location=location,
            featurestore_id=self._id,
            splits=splits,
            seed=seed,
            statistics_config=statistics_config,
            label=label,
            coalesce=coalesce,
            transformation_functions=transformation_functions,
        )

    @property
    def id(self):
        """Id of the feature store."""
        return self._id

    @property
    def name(self):
        """Name of the feature store."""
        return self._name



    @property
    def description(self):
        """Description of the feature store."""
        return self._description

    @property
    def online_featurestore_name(self):
        """Name of the online feature store database."""
        return self._online_feature_store_name



    @property
    def hive_endpoint(self):
        """Hive endpoint for the offline feature store."""
        return self._hive_endpoint

    @property
    def offline_featurestore_name(self):
        """Name of the offline feature store database."""
        return self._offline_feature_store_name
