from Datalake.utils.readers.AbstractBatchReader import AbstractBatchReader
from Datalake.utils.readers.BatchReaderManager import BatchReaderConfig
from pyspark.sql import SparkSession

class NetezzaBatchReaderLogger(AbstractBatchReader):
  def __init__(self,batch_config: BatchReaderConfig, sparkSession: SparkSession):