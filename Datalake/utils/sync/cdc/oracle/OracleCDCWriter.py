from typing import Optional
from pyspark.sql import SparkSession
from Datalake.utils.logger import getLogger

from Datalake.utils.sync.cdc.CDCWriter import CDCWriter

class OracleCDCWriter(CDCWriter):
    def __init__(
        self,
        env: str,
        spark: SparkSession,
        source_schema: str,
        source_table: str,
        target_database: str,
        target_schema: str,
        target_table: str,
        source_catalog: str = None,
        primary_keys: str = None,
        update_excl_columns=[],
    ):
        super().__init__(
            env=env,
            spark=spark,
            source_schema=source_schema,
            source_table=source_table,
            target_schema=target_schema,
            target_table=target_database,
            target_catalog=target_table,
            source_catalog=source_catalog,
            primary_keys=primary_keys,
            update_excl_columns=update_excl_columns,
        )
        self.logger = getLogger()