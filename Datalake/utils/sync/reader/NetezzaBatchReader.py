from pyspark.sql import SparkSession

from Datalake.utils.sync.BatchManager import DateRangeBatchConfig
from Datalake.utils.sync.reader.AbstractBatchReader import AbstractBatchReader


class NetezzaBatchReaderLogger(AbstractBatchReader):
    def __init__(self, batch_config: DateRangeBatchConfig, sparkSession: SparkSession):
        pass
