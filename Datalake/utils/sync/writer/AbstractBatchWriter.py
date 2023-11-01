from abc import ABC

from delta.tables import DataFrame, SparkSession
from pyspark.sql import DataFrame, SparkSession

from Datalake.utils.sync.BatchManager import DateRangeBatchConfig


class AbstractBatchWriter(ABC):
    def __init__(self, config: DateRangeBatchConfig, spark: SparkSession):
        self.config = config
        self.spark = spark

    def write(self, df: DataFrame) -> None:
        pass
