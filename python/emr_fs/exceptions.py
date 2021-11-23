class UnknownSecretStorageError(Exception):
    """This exception will be raised if an unused secrets storage is passed as a parameter."""


class FeatureStoreException(Exception):
    """Generic feature store exception"""


class ExternalClientError(TypeError):
    """Raised when external client cannot be initialized due to missing arguments."""

    def __init__(self, missing_argument):
        message = (
            "{0} cannot be of type NoneType, {0} is a non-optional "
            "argument to connect to hopsworks from an external environment."
        ).format(missing_argument)
        super().__init__(message)
