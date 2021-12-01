import numpy
import datetime
import re
from typing import Optional, Union, List, Dict, TypeVar
from emr_fs.transformation_function import TransformationFunction
from emr_fs.engine.hive import FeatureStoreHiveEngine
from emr_fs.statistics_config import StatisticsConfig


class FeatureStore:
    def __init__(
        self,
        emr_master_node,
        featurestore_name,
        s3_store_path,
        featurestore_description,
    ):
        self._emr_master_node= emr_master_node
        self._name = featurestore_name
        self._description = featurestore_description
        self._s3_store_path = s3_store_path



    def get_feature_group(self, name: str):
        with FeatureStoreHiveEngine(emr_master_node) as engine:
            feature_group_info =  engine.get_feature_group(name)
            feature_unique_key = ""
            feature_eventtime_key = ""
            features = []

            tablePros = re.findMatch(feature_group_info,"TBLPROPERTIES(*)").split(",")
            for property in tablePros:
                if property.contains('feature_unique_key'):
                   feature_unique_key=property.split("=")[1]
                else property.contains("feature_eventtime_key"):
                   feature_eventtime_key=property.split("=")[1]

            tableColumns = re.findMatch(feature_group_info,name+"(*)")
            for column in tableColumns:
                name = column.split(" ")[0]
                type = column.split(" ")[1]
                feature = Feature(name,type)
                features.append(feature)

            feature_group = FeatureGroup(self,name,"",feature_unique_key,feature_eventtime_key,features)
            return feature_group




    def create_feature_store(self):
            """Create a feature store db in hive metadata."""
        with FeatureStoreHiveEngine(emr_master_node) as engine:
           engine.create_feature_store(
                master_node=self._emr_master_node,
                name=self._name,
                desc=self._description,
                location=self._s3_store_path)
        return FeatureStore()


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
        feature_eventtime_key: str,
        feature_keys:  = {}
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
        features = pares_features(feature_keys)
        return FeatureGroup(feature_group_name,desc,feature_unique_key,feature_eventtime_key,features)




    def create_training_dataset(
        self,
        Query,
        data_format: Optional[str] = "tfrecords",
        target_s3_path: str = None
    ):
        """Create a training dataset metadata object.
        !!! info "Data Formats"
            The feature store currently supports the following data formats for
            training datasets:

            1. tfrecord
            2. csv
            3. libsvm
        # Returns:
            Sink The training dataset into S3 target path.
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
    def emr_master_node(self):
        """Id of the feature store."""
        return self._emr_master_node

    @property
    def featurestore_name(self):
        """Name of the feature store."""
        return self._featurestore_name


    @property
    def s3_store_path(self):
        """Description of the feature store."""
        return self._s3_store_path


