from pyspark.mllib.util import MLUtils
from emr_fs import util
from pyspark.context import SparkContext, SparkConf


class TransformationFunction:
    def __init__(
        self,
        featurestore_id,
        query,
        s3_target_path
   ):
        self._id = id
        self._featurestore_id = featurestore_id
        self._query = query
        self._s3_target_path = s3_target_path
        self._sc = SparkContext()



   def __init__(self,emr_master_node):
        self.logger = Log("file")
        self._master_node = emr_master_node
        super().__init__()

   def __enter__(self):
        self._con = hive.Connection(host=self._master_node, port='10000', username='hive')

   def __exit__(self):
        self._con.close()

   def save(self, df, format='tfrecords',path="s3://"):
        """
            transformate the feature store dataset into output format and landing to target location.
        """
        if path is not None:
           path = self._s3_target_path

        if format == 'tfrecords':
            df.write.format("tfrecords").mode("overwrite").save(path)
        else if format == 'libsvm':
            convDf = df.rdd.map(lambda line: LabeledPoint(line[0],[line[1:]]))
            MLUtils.saveAsLibSVMFile(convDf, path)
        else if format == 'csv':
            df.write.format("csv").mode("overwrite").save(path)


    @property
    def id(self):
        """Training dataset id."""
        return self._id

    @property
    def query(self):
        return self._query

    @property
    def s3_target_path(self):
        return self._s3_target_path



    @featurestore_id.setter
    def featurestore_id(self, featurestore_id):
        self._featurestore_id = featurestore_id

    @s3_target_path.setter
    def s3_target_path(self, s3_target_path):
        self._s3_target_path = s3_target_path

    @query.setter
    def query(self, query):
        self._query = query
