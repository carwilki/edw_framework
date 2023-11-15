import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.dbutils import DBUtils
from delta import DeltaTable
from utils.mapper import toBatchMemento, toDateRangeBatchConfig
from Datalake.utils.genericUtilities import getEnvPrefix
from Datalake.utils.sync.batch.BatchMemento import BatchMemento
from Datalake.utils.sync.batch.BatchReaderSourceType import BatchReaderSourceType
from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig
from Datalake.utils.sync.reader.AbstractBatchReader import AbstractBatchReader
from Datalake.utils.sync.reader.NetezzaJDBCBatchReader import NetezzaJDBCBatchReader
from Datalake.utils.sync.reader.SnowflakeBatchReader import SnowflakeBatchReader
from Datalake.utils.sync.writer.AbstractBatchWriter import AbstractBatchWriter
from Datalake.utils.sync.writer.SparkDeltaLakeBatchWriter import (
    SparkDeltaLakeBatchWriter,
)
from Datalake.utils.sync import dl_vars


class BatchManager(object):
    """Batch manager provides an iteratore like interface for tranfering data from one table into another
    using 'batches'. Batches can only be used if the data has some sort of ordering for the records
    """

    def __init__(self, spark: SparkSession, batchConfig: DateRangeBatchConfig):
        """Initializes the BatchReaderManager class. This class will create the table to store the metadata in if it
        does not already exist"""
        self.env = batchConfig.env
        self.spark = spark
        self.log_table = f"{getEnvPrefix(self.env)}{dl_vars.dl_metadata_table}"
        self._createLogTable()
        self._setup_job_params()
        self.log_table_schema = StructType([
            StructField("batch_id", StringType(), False),
            StructField("value", StringType(), False)
        ])
        m = self._loadMemento(batchConfig.batch_id)
        if m is not None:
            print(
                f"BatchManager::__init__::found mememento for batch_id:{batchConfig.batch_id}"
            )
            if toDateRangeBatchConfig(m) != batchConfig:
                print(
                    f"""BatchManager::__init__::memento found for batch_id:{batchConfig.batch_id}
                    but it is not the same as the one in the config file.
                    Overwriting memento"""
                )
                m = toBatchMemento(batchConfig)
                m.current_dt = batchConfig.start_dt
                self.state = m
                self._updateMemento(toBatchMemento(batchConfig))
            else:
                self.state = m
        else:
            print(
                f"""BatchManager::__init__::memento not found for batch_id:{batchConfig.batch_id}.
                    creating new memento"""
            )
            print(
                f"""BatchManager::__init__::batchConfig
                  {batchConfig}"""
            )
            batchConfig.current_dt = batchConfig.start_dt
            self.state = toBatchMemento(batchConfig)
            self._updateMemento(self.state)

            print(
                f"""BatchManager::__init__::memento created for batch_id:{batchConfig.batch_id}"""
            )
        print(self.state)

    def _setup_job_params(self):
        print("BatchManager::_setup_job_params::Setting up job params")
        self.dbutils = DBUtils(self.spark)
        context_str = (
            self.dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .toJson()
        )
        context = json.loads(context_str)
        self.task_name = context.get("tags", {}).get("taskKey", None)
        self.job_id = context.get("tags", {}).get("jobId", None)
        run_id_obj = context.get("currentRunId", {})
        self.run_id = run_id_obj.get("id", None) if run_id_obj else None
        print(f"""BatchManager::_setup_job_params::task_name:{self.task_name}""")
        print(f"""BatchManager::_setup_job_params::job_id:{self.job_id}""")
        print(f"""BatchManager::_setup_job_params::run_id:{self.run_id}""")

    def _loadMemento(
        self,
        batch_id: str,
    ) -> BatchMemento | None:
        sql = f"select value from {self.log_table} where lower(batch_id) = '{batch_id.lower()}'"
        df = self.spark.sql(sql)
        print("BatchManager::_loadMemento::Loading batch state")
        print(f"BatchManager::_loadMemento::SQL::{sql}")

        try:
            s = str(df.collect()[0][0])
        except IndexError:
            print(f"BatchManager::_loadMemento::No memento found for {batch_id}")
            return None

        return BatchMemento.parse_raw(s)

    def _updateMemento(self, memento: BatchMemento) -> None:
        print("BatchManager::_mergeMemento::Saving batch state")
        mj = memento.json()
        schema = ["batch_id", "value"]
        value = [self.state.batch_id, mj]
        t = DeltaTable.forName(self.spark, self.log_table)
        s = self.spark.createDataFrame(value, schema)
        t.merge(
            s, "batch_id = batch_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        t.optimize()
        print("BatchManager::_mergeMemento::Merge Complete")

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
            return SnowflakeBatchReader(toDateRangeBatchConfig(self.state), self.spark)
        elif self.state.source_type == BatchReaderSourceType.NETEZZA:
            print("BatchManager::_build_source::creating Netezza source")
            return NetezzaJDBCBatchReader(
                toDateRangeBatchConfig(self.state), self.spark
            )

    def _build_target(self) -> AbstractBatchWriter:
        print("BatchManager::_build_target::creating spark delta writer")
        return SparkDeltaLakeBatchWriter(toDateRangeBatchConfig(self.state), self.spark)

    # TODO: implement a gap check to make sure that we do not miss records due to intervals
    # passing them by
    def next(self):
        if self.state.current_dt <= self.state.end_dt:
            print("BatchManager::process_batch::processing batch")
            source = self._build_source()
            target = self._build_target()
            data = source.next()
            target.write(data)
            print("BatchManager::process_batch::batch processed")
            self.state.current_dt = self.state.current_dt + self.state.interval
            self._updateMemento(self.state)
        else:
            print("BatchManager::process_batch::no more batches to process")
            raise ValueError("No more batches to process. Disable Job in scheduler")
