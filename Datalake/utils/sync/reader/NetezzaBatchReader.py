from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession

from Datalake.utils import secrets
from Datalake.utils.genericUtilities import getSFEnvSuffix
from Datalake.utils.sync.batch.BatchReaderSourceType import BatchReaderSourceType
from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig
from Datalake.utils.sync.reader.AbstractBatchReader import AbstractBatchReader


class NetezzaJDBCBatchReader(AbstractBatchReader):
    """

    Args:
        AbstractBatchReader (object): The abstract ba
    """

    def __init__(self, config: DateRangeBatchConfig, spark: SparkSession):
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
        self.nz_database = self.nz_database + getSFEnvSuffix(self.env)
        self.nz_schema = parts[1].strip()
        self.nz_table = parts[2].strip()
        self.config.source_table_fqn = (
            f"{self.nz_database}.{self.nz_schema}.{self.nz_table}"
        )
        print(f"NetezzaJDBCBatchReader::_setup_reader::sf_database: {self.nz_database}")
        print(f"NetezzaJDBCBatchReader::_setup_reader::sf_schema: {self.nz_schema}")
        print(f"NetezzaJDBCBatchReader::_setup_reader::sf_table {self.nz_table}")

        print(f"NetezzaJDBCBatchReader::_setup_reader::env: {self.env}")
        if self.env == "prod":
            self.nzOptions = {
                "driver": config.get("netezza_jdbc_driver"),
                "url": f"""{config.get("netezza_jdbc_url")}{self.config.source_schema};""",
                "fetchsize": 100000,
                "user": config.get("netezza_jdbc_user"),
                "password": config.get("netezza_jdbc_password"),
                "numPartitions": config.get("netezza_jdbc_num_part"),
            }
        else:
            self.nzOptions = {
                "driver": config.get("netezza_jdbc_driver"),
                "url": f"""{config.get("netezza_jdbc_url")}{self.config.source_schema};""",
                "fetchsize": 100000,
                "user": config.get("netezza_jdbc_user"),
                "password": config.get("netezza_jdbc_password"),
                "numPartitions": config.get("netezza_jdbc_num_part"),
            }

        print(f"NetezzaJDBCBatchReader::_setup_reader::sfOptions: {self.sfOptions}")

    def _validate_sf_config(self, config: DateRangeBatchConfig):
        print("NetezzaJDBCBatchReader::_validate_sf_config::validating sf config")
        if config.source_type != BatchReaderSourceType.SNOWFLAKE:
            raise ValueError(
                "source_type must be set to Snowflake for use with the NetezzaJDBCBatchReader"
            )
        if config.source_table_fqn is None:
            raise ValueError(
                "source_table_fqn must be set for use with the NetezzaJDBCBatchReader"
            )

    def _generate_query(self, dt: datetime) -> str:
        print("NetezzaJDBCBatchReader::_generate_query::generating query")
        query = f"""select * from {self.nz_table}"""
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

    def _execute_query(self, query: str) -> DataFrame:
        self.spark.read()

    def _strip_colunms(self, df: DataFrame) -> DataFrame:
        print("NetezzaJDBCBatchReader::_strip_colunms::stripping excluded columns")
        df = df.drop(*self.config.excluded_columns)
        return df

    def next(self) -> DataFrame:
        print(
            f"""NetezzaJDBCBatchReader::next::reading batch for
            table:      {self.config.source_table_fqn}
            on range:   {self.config.current_dt} to {self.config.current_dt + self.config.interval}"""
        )
        
        df = self._convert_decimal_to_int_types(df)
        df = self._strip_colunms(df)

    def read(self) -> DataFrame:
        """
        Reads data from a JDBC source table and returns it as a DataFrame.

        Returns:
            DataFrame: A DataFrame containing the data from the JDBC source table.
        """
        config = HarnessJobManagerEnvironment.getConfig()
        SQL = (
            f"""Select * from {self.config.source_schema}.{self.config.source_table}"""
        )

        if self.config.source_filter is not None:
            SQL = SQL + f""" WHERE {self.config.source_filter}"""

        SQL = f"""({SQL}) as data"""

        df = self.session.read.format("jdbc").options(**reader_options).load()


        return df
