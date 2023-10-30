from datetime import datetime
from enum import Enum
import json
from typing import Optional
from dataclasses import dataclass, field
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, desc, row_number, when
from pyspark.sql.window import Window
import pickle

from Datalake.utils import secrets
from Datalake.utils.Snowflake.SnowflakeBatchReader import SnowflakeBatchReader
from Datalake.utils.genericUtilities import getEnvPrefix, getLogger
from Datalake.utils.Snowflake.vars import (
    cdc_metadata_catalog,
    cdc_metadata_schema,
    cdc_metadata_table,
)
from Datalake.utils.netezza.NetezzaBatchReader import NetezzaBatchReaderLogger
from Datalake.utils.readers.AbstractBatchReader import AbstractBatchReader


class BatchReaderSourceType(Enum):
    """
    This enum defines the different types of source systems that can be read from by the script.
    """

    SNOWFLAKE = "snowflake"
    NETEZZA = "netezza"


class BatchReaderManagerException(Exception):
    """
    This exception is used to handle any exceptions that may occur during the execution of the script.
    """

    def __init__(self, message: str):
        super().__init__(message)


@dataclass
class BatchMemento(object):
    batch_id: str
    env: str
    source_table: str
    source_schema: str
    target_schema: str
    target_table: str
    source_type: BatchReaderSourceType
    source_filter: str | None = None
    source_catalog: str | None = None
    target_catalog: str | None = None
    excluded_columns: list[str] = field(default_factory=list)
    date_columns: list[str] = field(default_factory=list)
    start_dt: datetime
    end_dt: datetime
    current_dt: datetime

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__ = d

    def to_config(self):
        return DateRangeBatchConfig(
            batch_id=self.batch_id,
            env=self.env,
            source_table=self.source_table,
            source_schema=self.source_schema,
            target_schema=self.target_schema,
            target_table=self.target_table,
            source_type=self.source_type,
            source_filter=self.source_filter,
            source_catalog=self.source_catalog,
            target_catalog=self.target_catalog,
            excluded_columns=self.excluded_columns,
            date_columns=self.date_columns,
            start_dt=self.start_dt,
            end_dt=self.end_dt,
        )


@dataclass(keyword_only=True)
class DateRangeBatchConfig(object):
    """
    This dataclass is used to store the configuration information for the script.
    The configuration information includes the name of the table to be read from the source system,
    the name of the table to be loaded into the target system, and the type of source system being read from.
    """

    batch_id: str
    env: str
    source_table: str
    source_schema: str
    target_schema: str
    target_table: str
    source_type: BatchReaderSourceType
    source_filter: str | None = None
    source_catalog: str | None = None
    target_catalog: str | None = None
    excluded_columns: list[str] = field(default_factory=list)
    date_columns: list[str] = field(default_factory=list)
    start_dt: datetime
    end_dt: datetime

    def to_memento_for_dt(self, current: datetime) -> BatchMemento:
        """turns this config into a BatchReaderMemento wich can be used to create a BatchReaderManager

        Args:
            current (datetime): that date time that should be between the start_dt and the end_dt
            that indicates what day is currently being processed

        Returns:
            BatchReaderMemento: BatchReaderMemento that can be used to create a BatchReaderManager
        """
        return BatchMemento(
            batch_id=self.batch_id,
            env=self.env,
            source_table=self.source_table,
            source_schema=self.source_schema,
            target_schema=self.target_schema,
            target_table=self.target_table,
            source_type=self.source_type,
            source_filter=self.source_filter,
            source_catalog=self.source_catalog,
            target_catalog=self.target_catalog,
            excluded_columns=self.excluded_columns,
            date_columns=self.date_columns,
            start_dt=self.start_dt,
            end_dt=self.end_dt,
            current_dt=current,
        )


class BatchReaderManager(object):
    """Snowflake CDC Logger class. This class is used to write the metadata about successfull execution
    cdc to the snowflake database. This class will create the table to store the metadata in if it does
    not already exist.
    dl_catalog, dl_schema, dl_table, sf_database, sf_schema, sf_table are used to identity the table
    that will be logged.
    :param env: The environment variable used to identify the environment.
    :param spark: The spark session.
    :param dl_schema: The schema of the datalake table.
    :param dl_table: The table of the datalake table.
    :param sf_database: The snowflake database.
    :param sf_schema: The snowflake schema.
    :param sf_table: The snowflake table.
    :param dl_catalog: The datalake catalog. Optional.
    :param primary_keys: The primary keys of the table.
    :param update_excl_columns: Colunms that should be excluded from the update.
    """

    def __init__(self, env: str, spark: SparkSession, batchConfig: DateRangeBatchConfig):
        """Initializes the BatchReaderManager class. This class will create the table to store the metadata in if it
        does not already exist"""
        self.env = env
        self.spark = spark
        self.log_table = f"{getEnvPrefix(env)}raw.batch_reader_state"
        self._createLogTable()
        m = self._loadMemento(batchConfig.batch_id)
        if m is not None:
            getLogger().info(f"found mememento for batch_id:{batchConfig.batch_id}")
            self.state = m
        else:
            getLogger().info(
                f"""memento not found for batch_id:{batchConfig.batch_id}.
                    creating new memento"""
            )
            self.state = batchConfig.to_memento_for_dt(batchConfig.start_dt)

    def _loadMemento(
        self,
        batch_id: str,
    ) -> BatchMemento | None:
        df = self.spark.sql(
            f"select value from {self.log_table} where lower(batch_id) = '{batch_id.lower()}'"
        )
        try:
            s = str(df.collect()[0][0])
        except IndexError:
            getLogger().warning(f"""no memento found for batch_id:{batch_id}""")
            return None

        return pickle.loads(s)

    def _saveMemento(self, memento: BatchMemento) -> None:
        self.spark.sql(
            f"""insert into {self._get_metadata_table()}
                (batch_id, value) values ('{memento.batch_id}', '{pickle.dumps(memento)}')"""
        ).collect()

    def _createLogTable(self):
        """Creates the metadata table for the batch reader if it does not exist"""
        self.spark.sql(
            f"""create table if not exists {self.log_table}(
                batch_id string,
                value string)"""
        ).collect()

    def _build_source(self) -> AbstractBatchReader:
        if self.state.source_type == BatchReaderSourceType.SNOWFLAKE:
            return SnowflakeBatchReader(self.state.to_config(), self.spark)
        elif self.state.source_type == BatchReaderSourceType.NETEZZA:
            return NetezzaBatchReaderLogger(self.state.to_config(), self.spark)
    
    def _build_target(self) -> AbstractBatchReader:

    def process_batch(self):
        source = self._build_source()
        target = 
