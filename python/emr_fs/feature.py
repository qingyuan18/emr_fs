import json
import re
from emr_fs import util


class Feature:
    """Metadata object representing a feature in a feature group in the Feature Store.

    See Training Dataset Feature for the
    feature representation of training dataset schemas.
    """

    COMPLEX_TYPES = ["MAP", "ARRAY", "STRUCT", "UNIONTYPE"]

    def __init__(
        self,
        name,
        type=None,
        primary=None,
        partition=None,
        hudi_precombine_key=None,
        default_value=None,
        feature_group=None,
    ):
        self._name = name.lower()
        self._type = type
        self._primary = primary or False
        self._partition = partition or False
        self._hudi_precombine_key = hudi_precombine_key or False
        self._default_value = default_value
        self._feature_group = feature_group

    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type,
            "partition": self._partition,
            "hudiPrecombineKey": self._hudi_precombine_key,
            "primary": self._primary,
            "defaultValue": self._default_value,
            "featureGroup": self._feature_group,
        }


    def parse_to_features(self,tableDes):
        retFeatures = []
        tlbColGrgs=re.findall("(.+)",tableDes)
        for tlbColGrg in tlbColGrgs:
            if tlbColGrg.contains("partition"):
               ###primary key #####
               retFeatures.append(self.getPkFeature(tlbColGrg))
            else if tlbColGrg.contains("tableproperities"):
               continue
            else:
               ####other parse_to_features #######
               retFeatures.append(self.getNormalFeature(tlbColGrg))
        return retFeatures

    def  getNormalFeature(self):
        pass

    def getPkFeature(self):
        return json.dumps(self, cls=util.FeatureStoreEncoder)

    def is_complex(self):
        """Returns true if the feature has a complex type."""
        return any(map(self._type.upper().startswith, self.COMPLEX_TYPES))

    @property
    def name(self):
        """Name of the feature."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def type(self):
        """Data type of the feature in the offline feature store.

        !!! danger "Not a Python type"
            This type property is not to be confused with Python types.
            The type property represents the actual data type of the feature in
            the feature store.
        """
        return self._type

    @type.setter
    def type(self, type):
        self._type = type


    @property
    def primary(self):
        """Whether the feature is part of the primary key of the feature group."""
        return self._primary

    @primary.setter
    def primary(self, primary):
        self._primary = primary

    @property
    def partition(self):
        """Whether the feature is part of the partition key of the feature group."""
        return self._partition

    @partition.setter
    def partition(self, partition):
        self._partition = partition

    @property
    def hudi_precombine_key(self):
        """Whether the feature is part of the hudi precombine key of the feature group."""
        return self._hudi_precombine_key

    @hudi_precombine_key.setter
    def hudi_precombine_key(self, hudi_precombine_key):
        self._hudi_precombine_key = hudi_precombine_key

    @property
    def default_value(self):
        """Default value of the feature as string, if the feature was appended to the
        feature group."""
        return self._default_value

    @default_value.setter
    def default_value(self, default_value):
        self._default_value = default_value

    def __str__(self):
        return self.json()

