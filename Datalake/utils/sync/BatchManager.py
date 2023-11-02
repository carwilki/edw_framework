import pickle
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from pyspark.sql import SparkSession
from Datalake.utils.genericUtilities import getEnvPrefix
from Datalake.utils.sync.reader.AbstractBatchReader import AbstractBatchReader
from Datalake.utils.sync.reader.NetezzaBatchReader import NetezzaBatchReaderLogger
from Datalake.utils.sync.reader.SnowflakeBatchReader import SnowflakeBatchReader
from Datalake.utils.sync.writer.AbstractBatchWriter import AbstractBatchWriter
from Datalake.utils.sync.writer.SparkDeltaLakeBatchWriter import (
    SparkDeltaLakeBatchWriter,
)


class BatchReaderSourceType(Enum):
    """
    This enum defines the different types of source systems that can be read from by the script.
    """

    SNOWFLAKE = "snowflake"
    NETEZZA = "netezza"


class BatchManagerException(Exception):
    """
    This exception is used to handle any exceptions that may occur during the execution of the script.
    """

    def __init__(self, message: str):
        super().__init__(message)


@dataclass
class BatchMemento(object):
    batch_id: str
    env: str
    source_table_fqn: str
    target_table_fqn: str
    source_type: BatchReaderSourceType
    source_filter: str | None = None
    keys: list[str] = field(default_factory=list)
    excluded_columns: list[str] = field(default_factory=list)
    date_columns: list[str] = field(default_factory=list)
    start_dt: datetime
    end_dt: datetime
    current_dt: datetime
    interval: timedelta = field(default_factory=lambda: timedelta(weeks=1))

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__ = d

    def to_config(self):
        return DateRangeBatchConfig(
            batch_id=self.batch_id,
            env=self.env,
            source_type=self.source_type,
            source_table_fqn=self.source_table_fqn,
            target_table_fqn=self.target_table_fqn,
            source_filter=self.source_filter,
            excluded_columns=self.excluded_columns,
            date_columns=self.date_columns,
            start_dt=self.start_dt,
            end_dt=self.end_dt,
            current_dt=self.current_dt,
            interval=self.interval,
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
    source_type: BatchReaderSourceType
    source_table_fqn: str
    target_table_fqn: str
    source_filter: str | None = None
    keys: list[str] = field(default_factory=list)
    excluded_columns: list[str] = field(default_factory=list)
    date_columns: list[str] = field(default_factory=list)
    start_dt: datetime
    end_dt: datetime
    current_dt: datetime
    interval: timedelta = field(default_factory=lambda: timedelta(weeks=1))

    def to_memento(self) -> BatchMemento:
        """turns this config into a BatchMemento wich can be used to create a BatchReaderManager

        Returns:
            BatchMemento: BatchReaderMemento that can be used to create a BatchReaderManager
        """
        return BatchMemento(
            batch_id=self.batch_id,
            env=self.env,
            source_type=self.source_type,
            source_table_fqn=self.source_table_fqn,
            target_table_fqn=self.target_table_fqn,
            source_filter=self.source_filter,
            excluded_columns=self.excluded_columns,
            date_columns=self.date_columns,
            start_dt=self.start_dt,
            end_dt=self.end_dt,
            current_dt=self.current_dt,
            interval=self.interval,
        )


class BatchManager(object):
    """Batch manager provides an iteratore like interface for tranfering data from one table into another
    using 'batches'. Batches can only be used if the data has some sort of ordering for the records
    """
    def __init__(self, spark: SparkSession, batchConfig: DateRangeBatchConfig):
        """Initializes the BatchReaderManager class. This class will create the table to store the metadata in if it
        does not already exist"""
        self.env = batchConfig.env
        self.spark = spark
        self.log_table = f"{getEnvPrefix(self.env)}raw.batch_reader_state"
        self._createLogTable()
        m = self._loadMemento(batchConfig.batch_id)
        if m is not None:
            print(
                f"BatchManager::__init__::found mememento for batch_id:{batchConfig.batch_id}"
            )
            self.state = m
        else:
            print(
                f"""BatchManager::__init__::memento not found for batch_id:{batchConfig.batch_id}.
                    creating new memento"""
            )
            self.state = batchConfig.to_memento()

    def _loadMemento(
        self,
        batch_id: str,
    ) -> BatchMemento | None:
        sql = f"select value from {self.log_table} where lower(batch_id) = '{batch_id.lower()}'"
        df = self.spark.sql(sql)
        print("BatchManager::_loadMemento::Loading batch state")
        print(f"BatchManager::_load::SQL::{sql}")

        try:
            s = str(df.collect()[0][0])
        except IndexError:
            print(f"BatchManager::_loadMemento::No memento found for {batch_id}")
            return None

        return pickle.loads(s)

    def _saveMemento(self, memento: BatchMemento) -> None:
        sql = f"""insert into {self._get_metadata_table()}
                (batch_id, value) values ('{memento.batch_id}', '{pickle.dumps(memento)}')"""
        print("BatchManager::_saveMemento::Saving batch state")
        print(f"BatchManager::_saveMemento::SQL::{sql}")
        self.spark.sql(sql).collect()

    def _createLogTable(self):
        """Creates the metadata table for the batch reader if it does not exist"""
        sql = f"""create table if not exists {self.log_table}(
                batch_id string,
                value string)"""

        print("BatchManager::_createLogTable::creating metadata table")
        print(f"BatchManager::_createLogTable::SQL::{sql}")
        self.spark.sql(sql).collect()

    def _build_source(self) -> AbstractBatchReader:
        if self.state.source_type == BatchReaderSourceType.SNOWFLAKE:
            print("BatchManager::_build_source::creating Snowflake source")
            return SnowflakeBatchReader(self.state.to_config(), self.spark)
        elif self.state.source_type == BatchReaderSourceType.NETEZZA:
            print("BatchManager::_build_source::creating Netezza source")
            return NetezzaBatchReaderLogger(self.state.to_config(), self.spark)

    def _build_target(self) -> AbstractBatchWriter:
        print("BatchManager::_build_target::creating spark delta writer")
        return SparkDeltaLakeBatchWriter(self.state.to_config(), self.spark)

    def next(self):
        print("BatchManager::process_batch::processing batch")
        source = self._build_source()
        target = self._build_target()
        print("BatchManager::process_batch::batch processed")
        # target.write(df)
        self.state.current_dt = self.state.current_dt + self.state.interval
        self._saveMemento(self.state)
