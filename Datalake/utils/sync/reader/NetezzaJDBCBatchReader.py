from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from Datalake.utils.genericUtilities import getSFEnvSuffix
from Datalake.utils.sync.batch.BatchMemento import BatchMemento
from Datalake.utils.sync.batch.BatchReaderSourceType import BatchReaderSourceType
from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig
from Datalake.utils.sync.reader.AbstractBatchReader import AbstractBatchReader
from Datalake.utils.sync import nz_vars


class NetezzaJDBCBatchReader(AbstractBatchReader):
    """

    Args:
        AbstractBatchReader (object): The abstract ba
    """

    def __init__(self, config: BatchMemento, spark: SparkSession):
        print("NetezzaJDBCBatchReader::__init__")
        super().__init__(config)
        self._validate_sf_config(config)
        self._setup_reader(config, spark)

    def _setup_reader(self, config: DateRangeBatchConfig, spark: SparkSession):
        print("NetezzaJDBCBatchReader::_setup_reader::setting up reader")
        self.spark = spark
        self.env = config.env.strip()
        parts = config.source_table_fqn.strip().split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid source table FQN: {config.source_table_fqn}")

        self.nz_database = parts[0].strip()
        self.nz_schema = parts[1].strip()
        self.nz_table = parts[2].strip()
        self.config.source_table_fqn = (
            f"{self.nz_database}.{self.nz_schema}.{self.nz_table}"
        )
        print(f"NetezzaJDBCBatchReader::_setup_reader::nz_database: {self.nz_database}")
        print(f"NetezzaJDBCBatchReader::_setup_reader::nz_schema: {self.nz_schema}")
        print(f"NetezzaJDBCBatchReader::_setup_reader::nz_table {self.nz_table}")

        print(f"NetezzaJDBCBatchReader::_setup_reader::env: {self.env}")
        self.nzOptions: dict = {
            "driver": nz_vars.nz_jdbc_driver,
            "url": f"""{nz_vars.nz_jdbc_url}{self.nz_database};""",
            "fetchsize": nz_vars.nz_fetch_size,
            "user": nz_vars.nz_username,
            "password": nz_vars.nz_password,
            "numPartitions": nz_vars.nz_jdbc_num_part,
        }

        print(f"NetezzaJDBCBatchReader::_setup_reader::sfOptions: {self.nzOptions}")

    def _validate_sf_config(self, config: DateRangeBatchConfig):
        print("NetezzaJDBCBatchReader::_validate_sf_config::validating sf config")
        if config.source_type != BatchReaderSourceType.NETEZZA:
            raise ValueError(
                "source_type must be set to Neteeza for use with the NetezzaJDBCBatchReader"
            )
        if config.source_table_fqn is None:
            raise ValueError(
                "source_table_fqn must be set for use with the NetezzaJDBCBatchReader"
            )

    def _generate_query(self, dt: datetime) -> str:
        print("NetezzaJDBCBatchReader::_generate_query::generating query")
        query = f"""select * from {self.config.source_table_fqn}"""
        where = ""
        s_dt = dt.strftime("%Y-%m-%d 00:00:00")
        e_dt = (dt + self.config.interval).strftime("%Y-%m-%d 00:00:00")
        for col in self.config.date_columns:
            if len(where) == 0:
                where = f""" where {col} between '{s_dt}' and '{e_dt}'"""
            else:
                where = where + f""" or {col} between '{s_dt}' and '{e_dt}'"""

        query = query + where

        print(
            f"""NetezzaJDBCBatchReader::_generate_query::query generated:
                {query}"""
        )
        return query

    def next(self) -> DataFrame:
        print(
            f"""NetezzaJDBCBatchReader::next::reading batch for
            table:      {self.config.source_table_fqn}
            on range:   {self.config.current_dt} to {self.config.current_dt + self.config.interval}"""
        )
        self.nzOptions[
            "dbtable"
        ] = f"({self._generate_query(self.config.current_dt)}) as data"
        df = self.spark.read.format("jdbc").options(**self.nzOptions).load()
        df = self._convert_decimal_to_int_types(df)
        df = self._strip_colunms(df)
        return df
