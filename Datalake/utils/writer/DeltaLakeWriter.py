from abc import ABC
from pyspark.sql import SparkSession, DataFrame
from delta.tables import *
from Datalake.utils.BatchTransferManager import DateRangeBatchConfig


class AbstractBatchWriter(ABC):
    def __init__(self, config: DateRangeBatchConfig, spark: SparkSession):
        self.config = config
        self.spark = spark

    def write(self, df: DataFrame) -> None:
        pass


class DataLakeBatchWriter(AbstractBatchWriter):
    def __init__(self, config: DateRangeBatchConfig):
        super().__init__(config=config)

    def write(self, df: DataFrame) -> None:
        target = DeltaTable.forName(self.spark,)
