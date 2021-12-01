import numpy
import datetime
from emr_fs.transformation_function import TransformationFunction
from feature import Feature
from feature_group import FeatureGroup
from feature_store import FeatureStore

class Client(object):
    _instance = None

    def __new__(cls, *args, **kw):
         if cls._instance is None:
            cls._instance = object.__new__(cls, *args, **kw)
         return cls._instance

    def __init__(self):
        self._feature_group = None
        self._feature_store = None
        self.logger = Log("console")

    def create_feature_store(emr_master_node,name,s3_location_path,desc):
        self._feature_store = FeatureStore(emr_master_node,name,s3_location_path,desc)
        self._feature_store.create_feature_store()

    def connect_to_feature_store(emr_master_node,name):
        self._feature_store = FeatureStore(emr_master_node,name,None,None)

    def get_feature_group(name):
        if self._feature_store is None:
           self.logger.error("not connected to a feature_store")
           return None
        else:
           return self._feature_store.get_feature_group(name)

    def create_feature_group(name,desc,feature_unique_key,feature_eventtime_key,features):
         if self._feature_store is None:
            self.logger.error("not connected to a feature_store")
            return None
         else:
            return self._feature_store.create_feature_group(name,feature_unique_key,feature_eventtime_key,features)
