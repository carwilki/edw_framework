from delta.tables import DataFrame, DeltaTable
from pyspark.sql import DataFrame, SparkSession

from Datalake.utils.sync.BatchManager import DateRangeBatchConfig
from Datalake.utils.sync.writer.AbstractBatchWriter import AbstractBatchWriter


class SparkDeltaLakeBatchWriter(AbstractBatchWriter):
    def __init__(self, config: DateRangeBatchConfig, spark: SparkSession):
        super().__init__(config=config)

    def write(self, df: DataFrame) -> None:
        target = DeltaTable.forName(self.spark)
