from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession

from Datalake.utils import secrets
from Datalake.utils.genericUtilities import getSFEnvSuffix
from Datalake.utils.sync.batch.BatchReaderSourceType import BatchReaderSourceType
from Datalake.utils.sync.batch.DateRangeBatchConfig import DateRangeBatchConfig
from Datalake.utils.sync.reader.AbstractBatchReader import AbstractBatchReader


class SnowflakeBatchReader(AbstractBatchReader):
    """
    This class is used to read data from a Snowflake database and load it into a target system.
    It uses the PySpark API to read data from the Snowflake database and the PySpark API to
    write data to the target system.

    The class takes in a BatchConfig, and a SparkSession.

    The class uses the PySpark API to read data from the Snowflake database and the PySpark API
    to write data to the target system.

    The class uses the BatchReaderSourceType enum to specify the type of source system being read from.
    The class uses the BatchConfig dataclass to store the configuration information for the script.

    The class uses the BatchReaderManagerException class to handle any exceptions that may
    occur during the execution of the script.
    """

    def __init__(self, config: DateRangeBatchConfig, spark: SparkSession):
        super().__init__(config)
        self._validate_sf_config(config)
        self._setup_reader(config, spark)

    def _setup_reader(self, config: DateRangeBatchConfig, spark: SparkSession):
        self.spark = spark
        self.env = config.env.strip()
        parts = config.source_table_fqn.strip().split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid source table FQN: {config.source_table_fqn}")

        self.sf_database = parts[0].strip()
        self.sf_database = self.sf_database + getSFEnvSuffix(self.env)
        self.sf_schema = parts[1].strip()
        self.sf_table = parts[2].strip()

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

    def _validate_sf_config(self, config: DateRangeBatchConfig):
        if config.source_type != BatchReaderSourceType.SNOWFLAKE:
            raise ValueError(
                "source_type must be set to Snowflake for use with the SnowflakeBatchReader"
            )
        if config.source_table_fqn is None:
            raise ValueError(
                "source_table_fqn must be set for use with the SnowflakeBatchReader"
            )

    def _generate_query(self, dt: datetime) -> str:
        query = f"""select * from {self.sf_table}"""
        where = ""
        s_dt = dt.strftime("%Y-%m-%d")
        e_dt = (dt + timedelta(1, "day")).strftime("%Y-%m-%d")
        for col in self.date_columns:
            if len(where) == 0:
                where = f""" where {col} between '{s_dt}' and '{e_dt}'"""
            else:
                where = (
                    where
                    + f" and {col} between '{s_dt}' and '{(dt + timedelta(1,'day')).strftime('%Y-%m-%d')}'"
                )
        return query + where

    def _execute_query(self, query: str) -> DataFrame:
        self.spark.read()

    def _strip_colunms(self, df: DataFrame) -> DataFrame:
        df = df.drop(*self.exclude_columns)
        return df

    def next(self) -> DataFrame:
        df = (
            self.spark.read.format("net.snowflake.spark.snowflake")
            .options(**self.sfOptions)
            .option(
                "query",
                self._generate_query(self.config.current_dt) + self.config.interval,
            )
            .load()
        )
        df = self._strip_colunms(df)

        df = self._convert_decimal_to_int_types(df)

        return df
