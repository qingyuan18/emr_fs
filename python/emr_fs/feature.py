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
        type=None
    ):
        self._name = name.lower()
        self._type = type


    def to_dict(self):
        return {
            "name": self._name,
            "type": self._type
        }



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




