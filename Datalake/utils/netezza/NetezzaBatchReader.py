from Datalake.utils.readers.AbstractBatchReader import AbstractBatchReader
from Datalake.utils.BatchTransferManager import DateRangeBatchConfig
from pyspark.sql import SparkSession

class NetezzaBatchReaderLogger(AbstractBatchReader):
  def __init__(self,batch_config: DateRangeBatchConfig, sparkSession: SparkSession):