from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from Datalake.utils.genericUtilities import getEnvPrefix

from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig
from Datalake.utils.sync.writer.AbstractBatchWriter import AbstractBatchWriter


class SparkDeltaLakeBatchWriter(AbstractBatchWriter):
    def __init__(self, config: DateRangeBatchConfig, spark: SparkSession):
        super().__init__(config=config)
        self.spark = spark

        self._setup_writer()

    def _setup_writer(self):
        parts = self.config.source_table_fqn.strip().split(".")
        if len(parts) == 2:
            print(
                "SparkDeltaLakeBatchWriter::_setup_writer::schema.table format detected"
            )
            dl_schema = parts[0].strip()
            dl_schema = dl_schema + getEnvPrefix(self.config.env)
            dl_table = parts[1].strip()
            dl_catalog = None

        elif len(parts) == 3:
            print(
                "SparkDeltaLakeBatchWriter::_setup_writer::catalog.schema.table format detected"
            )
            dl_catalog = parts[0].strip()
            dl_schema = parts[1].strip()
            dl_schema = getEnvPrefix(self.env) + dl_schema

            dl_table = parts[2].strip()
        else:
            raise ValueError(
                f"""Invalid target table FQN: {self.config.target_table_fqn}
                table must follow one of the following formats:
                schema.table or catalog.schema.table"""
            )
        if dl_catalog is not None:
            self.config.target_table_fqn = f"{dl_catalog}.{dl_schema}.{dl_table}"
        else:
            self.config.target_table_fqn = f"{dl_schema}.{dl_table}"

        print(
            f"SparkDeltaLakeBatchWriter::_setup_writer::target_table_fqn: {self.config.target_table_fqn}"
        )

    def _build_merge_key(self, source: str, target: str) -> str:
        key = ""
        for col in self.config.keys:
            if len(key) == 0:
                key += f"{source}.{col}={target}.{col} "
            else:
                key += f" and {source}.{col}={target}.{col}"
        print(f"SparkDeltaLakeBatchWriter::_build_merge_key::key={key}")
        return key

    def _create_and_write(self, source: DataFrame) -> None:
        if not self.spark.catalog.tableExists(self.config.target_table_fqn):
            print(
                f"SparkDeltaLakeBatchWriter::_create_and_write::creating {self.config.target_table_fqn}"
            )

            source.write.format("delta").mode("overwrite").save(
                self.config.target_table_fqn
            )

    def _upsert(self, source: DataFrame) -> None:
        print(
            f"SparkDeltaLakeBatchWriter::_upsert::merging to {self.config.target_table_fqn}"
        )

        target: DeltaTable = DeltaTable.forName(
            self.spark, self.config.target_table_fqn
        )
        keys = self._build_merge_key("source", "target")
        target.alias("target").merge(
            source.alias("source"), keys
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(
            f"SparkDeltaLakeBatchWriter::_upsert::compacting {self.config.target_table_fqn}"
        )
        target.optimize().executeCompaction()
        print("SparkDeltaLakeBatchWriter::_upsert::done")

    def write(self, df: DataFrame) -> None:
        if self.spark.catalog.tableExists(self.config.target_table_fqn):
            self._upsert(df)
        else:
            print("SparkDeltaLakeBatchWriter::write::target table not found...creating")
            self._create_and_write(df)
