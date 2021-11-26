import numpy
import datetime
from typing import Optional, Union, List, Dict, TypeVar
from emr_fs.transformation_function import TransformationFunction
from emr_fs.engine.hive import FeatureStoreHiveEngine
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



    def get_feature_groups(self, name: str):
        with FeatureStoreHiveEngine(emr_master_node) as engine:
            return engine.get_feature_groups(name)




    def create_feature_store(self):
            """Create a feature store db in hive metadata."""
        with FeatureStoreHiveEngine(emr_master_node) as engine:
           engine.create_feature_store(
                master_node=self._emr_master_node,
                name=self._name,
                desc=self._description,
                location=self._s3_store_path)


    def register_feature_group(
        self,
        feature_group_name: str,
        desc: str = "",
        feature_unique_key: str,
        feature_unique_key_type: str = "string",
        feature_eventtime_key: str,
        feature_eventtime_key_type: str = "string",
        feature_normal_keys:  = []
    ):
       """register a feature group metadata object in a exsiting hudi table.
            # Returns
                `FeatureGroup`. The feature group metadata object.
       """
       with FeatureStoreHiveEngine(emr_master_node) as engine:
             engine.register_feature_group(feature_store_name=self.feature_store_name,feature_group_name, desc,
                  feature_unique_key,feature_unique_key_type,
                  feature_eventtime_key,feature_eventtime_key_type,
                  feature_normal_keys)
                   )



    def create_feature_group(
        self,
        feature_group_name: str,
        desc: str = "",
        feature_unique_key: str,
        feature_unique_key_type: str = "string",
        feature_eventtime_key: str,
        feature_eventtime_key_type: str = "string",
        feature_normal_keys:  = []
    ):
        """Create a feature group metadata object.
        # Returns
            `FeatureGroup`. The feature group metadata object.
        """
        with FeatureStoreHiveEngine(emr_master_node) as engine:
             engine.create_feature_group(feature_store_name=self.feature_store_name,feature_group_name, desc,
                   feature_unique_key,feature_unique_key_type,
                   feature_eventtime_key,feature_eventtime_key_type,
                   feature_normal_keys)
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
            3. libsvm

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
