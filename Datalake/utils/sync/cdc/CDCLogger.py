from Datalake.utils.sync.cdc.snowflake.SnowflakeCDCWriter import SnowflakeCDCException
from Datalake.utils.sync.cdc.snowflake.vars import (
    cdc_metadata_catalog,
    cdc_metadata_schema,
    cdc_metadata_table,
)
from Datalake.utils.genericUtilities import getEnvPrefix


from pyspark.sql import DataFrame, SparkSession


from typing import Optional


class CDCLogger:
    """Snowflake CDC Logger class. This class is used to write the metadata about successfull execution
    cdc to the snowflake database. This class will create the table to store the metadata in if it does
    not already exist.
    target_catalog, source_schema, source_table, target_catalog, target_schema, target_table are used to identity the table
    that will be logged.
    :param env: The environment variable used to identify the environment.
    :param spark: The spark session.
    :param source_schema: The schema of the datalake table.
    :param source_table: The table of the datalake table.
    :param target_catalog: The target catalog. Optional. Couold map to other terms like Database
    :param target_schema: The target schema.
    :param target_table: The target table.
    :param target_catalog: The datalake catalog. Optional.
    :param primary_keys: The primary keys of the table.
    :param update_excl_columns: Colunms that should be excluded from the update.
    """

    def __init__(
        self,
        env: str,
        spark: SparkSession,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        source_catalog: str = None,
        target_catalog: str = None,
    ):
        """Initializes the Snowflake CDC Logger class. This class will create the table to store the metadata in if it
        does not already exist"""
        self.env = env
        self.spark = spark
        self.source_schema = source_schema
        self.cdc_metadata_schema = cdc_metadata_schema
        self.cdc_metadata_catalog = cdc_metadata_catalog
        self.cdc_metadata_table = cdc_metadata_table
        self.source_table = source_table
        self.target_catalog = target_catalog
        self.target_schema = target_schema
        self.target_table = target_table
        self.target_catalog = source_catalog
        self.log_table = self._get_metadata_table()
        self.datalake_table_fqn = self._get_datalake_table()
        self.max_observed_version = self.getLastSeenVersion()
        self.max_table_history_verison = self.getMaxTableVersion()
        self._createLogTable()

    def _createLogTable(self):
        """Creates the metadata table for the snowflake cdc if it does not exist"""
        self.spark.sql(
            f"""create table if not exists {self.log_table}(
            dlSchema string,
            dlTable string,
            targetDatabase string,
            targetSchema string,
            targetTable string,
            version long,
            timestamp timestamp)"""
        ).collect()

    def _get_metadata_table(self) -> str:
        """Get the metadata table name from the env and the values set in the var.SnowflakeCDCWriter
        if var.SnowflakeCDCWriter.cdc_metadata_catalog is not None then will contain the catalog if
        given. Otherwise it will only contain the schema and the table name.
        """
        if cdc_metadata_catalog is not None:
            metadata_table = f"{self.cdc_metadata_catalog}.{self.cdc_metadata_schema}.{self.cdc_metadata_table}"
        else:
            metadata_table = f"{self.cdc_metadata_schema}.{self.cdc_metadata_table}"

        return getEnvPrefix(self.env) + metadata_table

    def _get_datalake_table(self) -> str:
        """Get the datalake table name from the provided parameters"""

        if self.target_catalog is not None:
            print("target_catalog Not None", self.target_catalog)
            dltable = f"{self.target_catalog}.{self.source_schema}.{self.source_table}"
        else:
            dltable = f"{self.source_schema}.{self.source_table}"

        return dltable

    def getLastSeenVersion(self) -> int:
        """This function gets the last version that was inserted into the cdc metadata table to
        to handle cdc
        """
        if self.target_catalog is not None:
            schema = f"{self.target_catalog}.{self.source_schema}"
        else:
            schema = self.source_schema
        query = f"""select version from {self.log_table} where
                        lower(dlSchema) = lower('{schema}')
                        and lower(dlTable) = lower('{self.source_table}')
                        and lower(targetDatabase) = lower('{self.target_catalog}')
                        and lower(targetSchema) = lower('{self.target_schema}')
                        and lower(targetTable) = lower('{self.target_table}')
                        and timestamp in (select max(timestamp) from {self.log_table}
                            where lower(dlSchema) = lower('{schema}')
                            and lower(dlTable) = lower('{self.source_table}')
                            and lower(targetDatabase) = lower('{self.target_catalog}')
                            and lower(targetSchema) = lower('{self.target_schema}')
                            and lower(targetTable) = lower('{self.target_table}'))"""

        print(
            f"""SnowflakeCDCLogger::getLastSeenVersion::Query
              {query}""",
        )
        df = self.spark.sql(query)
        if df.count() > 0:
            return df.collect()[0][0]
        else:
            return 0

    def getMaxTableVersion(self) -> int:
        """This function gets the last version that was inserted into the history metadata table to
        to handle cdc. This is used to bootstrap the cdc metadata table if there is nothing there.
        """
        ret = self.spark.sql(
            f"""select max(version) from (describe history {self.datalake_table_fqn})"""
        ).collect()[0][0]
        if ret is not None:
            return ret
        else:
            return 0

    def logLastSeenVersion(self, version: int) -> None:
        """function logs the version that is speciffied to the metadata table. this should the version
        that is most current at the time of write
        """
        if self.target_catalog is not None:
            schema = f"{self.target_catalog}.{self.source_schema}"
        else:
            schema = self.source_schema

        query = f"""insert into {self.log_table}(
                        dlSchema,dlTable,targetDatabase,targetSchema,targetTable,version,timestamp)
                    values('{schema}','{self.source_table}','{self.target_catalog}',
                    '{self.target_schema}','{self.target_table}',{version},
                    current_timestamp())"""

        print(
            f"""SnowflakeCDCLogger::logLastSeenVersion::Query
              {query}""",
        )

        self.spark.sql(query)

    def getChangesForTable(self, table_fqn: str) -> Optional[DataFrame]:
        """This function gets the changes for the table specified by the table_fqn"""

        # sanity check
        if self.max_observed_version > self.max_table_history_verison:
            raise SnowflakeCDCException(
                f"""Observed Version history:{self.max_observed_version} for table {table_fqn}
                    is greater than the max observed version {self.max_table_history_verison}.
                    This is likely due to the table being recreated.
                    update the version in the metadata table for cdf:{self.log_table}
                    and try again"""
            )

        lastSeenVersion = self.getLastSeenVersion()

        if lastSeenVersion is None:
            lastSeenVersion = 0

        print(lastSeenVersion)

        cdc_query = f"select * from table_changes('{table_fqn}',{lastSeenVersion})"

        df = self.spark.sql(cdc_query)

        count = df.count()

        if count > 0:
            return df
        else:
            return None
