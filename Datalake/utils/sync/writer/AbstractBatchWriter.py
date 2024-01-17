from abc import ABC

from delta.tables import DataFrame, SparkSession
from pyspark.sql import DataFrame, SparkSession
from Datalake.utils.sync.batch.BatchMemento import BatchMemento

from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig


class AbstractBatchWriter(ABC):
    def __init__(self, config: BatchMemento):
        self.config = config

    def write(self, df: DataFrame) -> None:
        pass
