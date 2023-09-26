from uuid import uuid4
from pyspark.sql import DataFrame, SparkSession
from Datalake.utils.genericUtilities import getEnvPrefix
from Datalake.utils.pk.vars import (
    pk_metadata_catalog,
    pk_metadata_schema,
    pk_metadata_table,
)


class DuplicateKeyException(Exception):
    def __init__(self, values: list, primary_keys: list[str]):
        super(DuplicateKeyException, self).__init__()
        self.args(values, primary_keys)


class DuplicateChecker(object):
    @classmethod
    def check_for_duplicate_primary_keys(
        cls,
        spark: SparkSession,
        values: DataFrame,
        primary_keys: list[str],
    ) -> None:
        keys = ",".join(primary_keys)
        temp = f"temp_dupe_check_view_{str(uuid4()).replace('-', '')}"
        values.createOrReplaceTempView(temp)
        ret = spark.sql(
            f"""select {keys} from {temp}
                  group by {keys} having count(*)>1"""
        ).collect()

        if len(ret) > 0:
            raise DuplicateKeyException(ret[:10], primary_keys)


class PrimaryKeyManager(object):
    @classmethod
    def get_pk_manager(cls, spark: SparkSession):
        return PrimaryKeyManager(spark=spark)

    def __init__(self, spark: SparkSession):
        self.pk_catalog = pk_metadata_catalog
        self.pk_schema = getEnvPrefix() + pk_metadata_schema
        self.pk_metadata_table = pk_metadata_table
        self.spark = spark
        self.table_fqn = self._get_table_pk_metadata_fqdn()
        self._bootstrap_metadata_table()

    def _get_table_pk_metadata_fqdn(self):
        if self.pk_catalog is None:
            return f"""{self.pk_schema}.{self.pk_metadata_table}"""
        else:
            return f"""{self.pk_catalog}.{self.pk_schema}.{self.pk_metadata_table}"""

    def _bootstrap_metadata_table(self):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_fqn}
            (
                catalog string,
                schema string,
                table string,
                keys string,
                created_at timestamp,
                updated_at timestamp
            )"""
        ).collect()

    def get_pk(self, schema: str, table: str, catalog: str = None) -> list[str]:
        if catalog is not None:
            filter = f"schema='{schema}' and table='{table}' and catalog='{catalog}'"
        else:
            filter = f"schema='{schema}' and table='{table}'"

        keys = self.spark.sql(
            f"""SELECT keys from {self.table_fqn} where {filter}"""
        ).collect()

        return keys.split("|")

    def upsert_pk(self, schema: str, table: str, keys: list[str], catalog: str = None):
        self.spark.sql(
            f"""
            CREATE TEMPORARY TABLE Temp_PKS
            (
                catalog string,
                schema string,
                table string,
                keys string,
            )
            AS
            SELECT '{catalog}', '{schema}', '{table}', '{keys}'
            """
        ).collect()
        keys = "|".join(keys)

        self.spark.sql(
            f"""
            MERGE INTO {self.table_fqn} USING Temp_PKS 
            ON {self.table_fqn}.catalog = '{catalog}' and {self.table_fqn}.schema = '{schema}'
                and {self.table_fqn}.table = '{table}'
            WHEN NOT MATCHED THEN
                INSERT (catalog, schema, table, keys, created_at, updated_at)
                VALUES ('{catalog}', '{schema}', '{table}', '{keys}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            WHEN MATCHED THEN
                UPDATE SET keys = '{keys}', updated_at = CURRENT_TIMESTAMP
            """
        ).collect()
