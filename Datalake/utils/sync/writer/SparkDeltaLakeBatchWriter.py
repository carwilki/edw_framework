from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig
from Datalake.utils.sync.writer.AbstractBatchWriter import AbstractBatchWriter


class SparkDeltaLakeBatchWriter(AbstractBatchWriter):
    def __init__(self, config: DateRangeBatchConfig, spark: SparkSession):
        super().__init__(config=config)
        self.spark = spark

    def _build_merge_key(self, source: str, target: str) -> str:
        key = ""
        for col in self.config.keys():
            if len(key) == 0:
                key += f"{source}.{col}={target}.{col} "
            else:
                key += f" and {source}.{col}={target}.{col}"

        return key

    def write(self, source: DataFrame) -> None:
        print(
            f"SparkDeltaLakeBatchWriter::write::merging to {self.config.target_table_fqn}"
        )
        target: DeltaTable = DeltaTable.forName(
            self.spark, self.config.target_table_fqn
        )
        target.merge(
            source, self._build_merge_key("source", "target")
        ).whenMatchedUpdateAll().whenNotMatchedInsert().execute()
        print(
            f"SparkDeltaLakeBatchWriter::write::compacting {self.config.target_table_fqn}"
        )
        target.optimize().executeCompaction()
        print("SparkDeltaLakeBatchWriter::write::done")
