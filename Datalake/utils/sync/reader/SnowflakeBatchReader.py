from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession

from Datalake.utils import secrets
from Datalake.utils.genericUtilities import getSFEnvSuffix
from Datalake.utils.sync.batch.BatchReaderSourceType import BatchReaderSourceType
from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig
from Datalake.utils.sync.reader.AbstractBatchReader import AbstractBatchReader


class SnowflakeBatchReader(AbstractBatchReader):
    """

    Args:
        AbstractBatchReader (object): The abstract ba
    """

    def __init__(self, config: DateRangeBatchConfig, spark: SparkSession):
        print("SnowflakeBatchReader::__init__")
        super().__init__(config)
        self._validate_sf_config(config)
        self._setup_reader(config, spark)

    def _setup_reader(self, config: DateRangeBatchConfig, spark: SparkSession):
        print("SnowflakeBatchReader::_setup_reader::setting up reader")
        self.spark = spark
        self.env = config.env.strip()
        parts = config.source_table_fqn.strip().split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid source table FQN: {config.source_table_fqn}")

        self.sf_database = parts[0].strip()
        self.sf_database = self.sf_database + getSFEnvSuffix(self.env)
        self.sf_schema = parts[1].strip()
        self.sf_table = parts[2].strip()
        print(f"SnowflakeBatchReader::_setup_reader::sf_database: {self.sf_database}")
        print(f"SnowflakeBatchReader::_setup_reader::sf_schema: {self.sf_schema}")
        print(f"SnowflakeBatchReader::_setup_reader::sf_table {self.sf_table}")

        print(f"SnowflakeBatchReader::_setup_reader::env: {self.env}")
        if self.env == "prod":
            self.sfOptions = {
                "sfUrl": "petsmart.us-central1.gcp.snowflakecomputing.com",
                "sfUser": secrets.get("databricks_service_account", "username"),
                "sfPassword": secrets.get("databricks_service_account", "password"),
                "sfDatabase": self.sf_database,
                "sfSchema": self.sf_schema,
                "sfWarehouse": "IT_WH",
                "authenticator": "https://petsmart.okta.com",
                "autopushdown": "on",
                "sfRole": "ROLE_BIGDATA",
            }
        else:
            self.sfOptions = {
                "sfUrl": "petsmart.us-central1.gcp.snowflakecomputing.com",
                "sfUser": secrets.get("SVC_BD_SNOWFLAKE_NP", "username"),
                "pem_private_key": secrets.get("SVC_BD_SNOWFLAKE_NP", "pkey"),
                "sfDatabase": self.sf_database,
                "sfSchema": self.sf_schema,
                "sfWarehouse": "IT_WH",
                "autopushdown": "on",
                "sfRole": "role_databricks_nonprd",
            }

        print(f"SnowflakeBatchReader::_setup_reader::sfOptions: {self.sfOptions}")

    def _validate_sf_config(self, config: DateRangeBatchConfig):
        print("SnowflakeBatchReader::_validate_sf_config::validating sf config")
        if config.source_type != BatchReaderSourceType.SNOWFLAKE:
            raise ValueError(
                "source_type must be set to Snowflake for use with the SnowflakeBatchReader"
            )
        if config.source_table_fqn is None:
            raise ValueError(
                "source_table_fqn must be set for use with the SnowflakeBatchReader"
            )

    def _generate_query(self, dt: datetime) -> str:
        print("SnowflakeBatchReader::_generate_query::generating query")
        query = f"""select * from {self.sf_table}"""
        where = ""
        s_dt = dt.strftime("%Y-%m-%d")
        e_dt = (dt + self.config.interval).strftime("%Y-%m-%d")
        for col in self.config.date_columns:
            if len(where) == 0:
                where = f""" where {col} between '{s_dt}' and '{e_dt}'"""
            else:
                where = where + f""" and {col} between '{s_dt}' and '{e_dt}'"""
        print(
            f"""SnowflakeBatchReader::_generate_query::query generated:
                {query}"""
        )
        return query + where

    def _execute_query(self, query: str) -> DataFrame:
        self.spark.read()

    def _strip_colunms(self, df: DataFrame) -> DataFrame:
        print("SnowflakeBatchReader::_strip_colunms::stripping excluded columns")
        df = df.drop(*self.exclude_columns)
        return df

    def next(self) -> DataFrame:
        print(
            f"""SnowflakeBatchReader::next::reading batch for
            table:      {self.config.source_table_fqn}
            on range:   {self.config.current_dt} to {self.config.current_dt + self.config.interval}"""
        )
        df = (
            self.spark.read.format("net.snowflake.spark.snowflake")
            .options(**self.sfOptions)
            .option("query", self._generate_query(self.config.current_dt))
            .load()
        )
        df = self._strip_colunms(df)

        df = self._convert_decimal_to_int_types(df)

        return df
