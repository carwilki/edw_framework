from delta import DeltaTable
from pyspark.sql import DataFrame


from abc import ABC, abstractmethod
from typing import Optional
from Datalake.utils.genericUtilities import getEnvPrefix
from Datalake.utils.logger import getLogger

from Datalake.utils.sync.cdc.CDCLogger import CDCLogger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class CDCWriter(ABC):
    """Provide abstract base class for all cdc
    :param ABC: _description_
    :type ABC: _type_
    """

    def __init__(
        self,
        env: str,
        spark: SparkSession,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        target_catalog: str = None,
        source_catalog: str = None,
        primary_keys: str = None,
        update_excl_columns: list[str] = [],
        write_mode: str | None = None,
    ):
        self.cdc_logger = CDCLogger(
            env=env,
            spark=spark,
            source_catalog=source_catalog,
            source_schema=getEnvPrefix(env) + source_schema,
            source_table=source_table,
            target_catalog=target_catalog,
            target_schema=target_schema,
            target_table=target_table,
        )
        self.spark = spark
        self.env = env.strip()
        self.source_catalog = (
            source_catalog.strip() if source_catalog is not None else None
        )

        self.target_catalog = (
            target_catalog.strip() if target_catalog is not None else None
        )

        if write_mode not in ["overwrite", "append", "merge"]:
            raise ValueError(f"update_type {write_mode} is not supported")

        self.source_schema = getEnvPrefix(self.env) + source_schema.strip()
        self.source_table = source_table.strip()
        self.update_excl_columns = [x.lower() for x in update_excl_columns]
        self.target_schema = target_schema.strip()
        self.target_table = target_table.strip()
        self.primary_keys = primary_keys
        self.write_mode = write_mode
        self.log_table = self.cdc_logger._get_metadata_table()
        self.logger = getLogger()

    @abstractmethod
    def _push(self, df: DataFrame) -> Optional[int]:
        pass

    def _getTargetFQN(self) -> str:
        if self.target_catalog is not None:
            return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"
        else:
            return f"{self.target_schema}.{self.target_table}"

    def _getSourceFQN(self) -> str:
        if self.source_catalog is not None:
            return f"{self.source_catalog}.{self.source_schema}.{self.source_table}"
        else:
            return f"{self.target_schema}.{self.target_table}"

    def _get_source_for_overwrite(self) -> DataFrame:
        return DeltaTable.forName(self.spark, self._getTargetFQN())

    def _get_source_for_append(self) -> DataFrame:
        lastSeenVersion = self.cdc_logger.getLastSeenVersion()

        if lastSeenVersion is None:
            lastSeenVersion = 0

        changes = (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", lastSeenVersion)
            .load()
            .filter(col("_change_type") == "insert")
            .drop(cols="_change_type,_commit_version,_commit_timestamp")
        )
        return changes

    def _get_source_for_merge(self) -> DataFrame:
        lastSeenVersion = self.cdc_logger.getLastSeenVersion()

        if lastSeenVersion is None:
            lastSeenVersion = 0

        changes = (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", lastSeenVersion)
            .load()
        )
        return changes

    def _get_source(self):
        if self.write_mode is None:
            return self._get_source_for_overwrite()
        elif self.write_mode.lower() == "overwrite":
            return self._get_source_for_overwrite()
        elif self.write_mode.lower() == "append":
            return self._get_source_for_append()
        elif self.write_mode.lower() == "merge":
            return self._get_source_for_merge()
        else:
            raise ValueError(f"update_option {self.write_mode} is not supported")

    def push(self) -> Optional[int]:
        """Push the cdc data to target that is configured for this writer from the
        datalake table that has been configured for this writer"""

        df = self._get_source()

        count = df.count()

        if count > 0:
            lastSeenVersion = self._push(df)
            self.cdc_logger.logLastSeenVersion(lastSeenVersion)
            return count
        else:
            print(
                f"""SnowflakeCDCWriter::push_cdc 
                  No changes found for {self._getSourceFQN()}"""
            )
            return None
