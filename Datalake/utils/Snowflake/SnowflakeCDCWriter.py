from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, row_number, when
from pyspark.sql.window import Window

from Datalake.utils import secrets
from Datalake.utils.Snowflake.CDCLogger import CDCLogger
from Datalake.utils.genericUtilities import getEnvPrefix


class SnowflakeCDCException(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class SnowflakeCDCWriter:
    """This is a self bootstrapping component that is used to pass captured changes
    to upstream tables in snowflake. This component create the table that is specfied
    by the log_table parameter. The user that runs this code must be able to creat the
    table if the table does not exist.

    The table name for the metadata is specified by the vars.SnowflakeCDCWriter
    cdc_metadata_table variable.
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

    def __init__(
        self,
        env: str,
        spark: SparkSession,
        dl_schema: str,
        dl_table: str,
        sf_database: str,
        sf_schema: str,
        sf_table: str,
        dl_catalog: str = None,
        primary_keys: str = None,
        update_excl_columns=[],
    ):
        self.cdc_logger = CDCLogger(
            env=env,
            spark=spark,
            dl_catalog=dl_catalog,
            dl_schema=getEnvPrefix(env) + dl_schema,
            dl_table=dl_table,
            sf_database=sf_database,
            sf_schema=sf_schema,
            sf_table=sf_table,
        )
        self.spark = spark
        self.env = env.strip()
        self.dl_catalog = dl_schema.strip() if dl_catalog is not None else None
        self.dl_schema = getEnvPrefix(self.env) + dl_schema.strip()
        self.dl_table = dl_table.strip()
        self.update_excl_columns = [x.lower() for x in update_excl_columns]
        self.sf_database = sf_database.strip()
        self.sf_schema = sf_schema.strip()
        self.sf_table = sf_table.strip()
        self.primary_keys = primary_keys
        self.log_table = self.cdc_logger._get_metadata_table()
        if self.env == "prod":
            self.sfOptions = {
                "sfUrl": "petsmart.us-central1.gcp.snowflakecomputing.com",
                "sfUser": secrets.get("databricks_service_account", "username"),
                "sfPassword": secrets.get("databricks_service_account", "password"),
                "sfDatabase": sf_database,
                "sfSchema": sf_schema,
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
                "sfDatabase": sf_database,
                "sfSchema": sf_schema,
                "sfWarehouse": "IT_WH",
                "autopushdown": "on",
                "sfRole": "role_databricks_nonprd",
            }

    def _run_sf_query(self, query):
        self.spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            self.sfOptions, query
        )

    def _write_df_to_sf(self, df, table=None):
        if table is None:
            table = self.sf_table
        print(f"SnowflakeCDCWriter::_write_df_to_sf::table::{table}")
        df.write.format("net.snowflake.spark.snowflake").options(
            **self.sfOptions
        ).option("dbtable", table).mode("overwrite").save()
        print("SnowflakeCDCWriter::_write_df_to_sf::Temp table write completed")

    def _get_clause(self, column_list, clause_type):
        clause_type = clause_type.lower()
        clause = ""
        for k in column_list:
            if clause == "":
                if clause_type == "merge_key" or (
                    clause_type == "update" and k not in self.update_excl_columns
                ):
                    clause = "base." + k + "=pre." + k
                elif clause_type == "insert":
                    clause = "pre." + k

            else:
                if clause_type == "merge_key":
                    clause = clause + " and " + "base." + k + " = pre." + k
                elif clause_type == "update" and k not in self.update_excl_columns:
                    clause = clause + " , " + "base." + k + " = pre." + k
                elif clause_type == "insert":
                    clause = clause + ",pre." + k

        if clause_type == "update":
            clause = clause + ", base.SNF_UPDATE_TSTMP = CURRENT_TIMESTAMP()"
        if clause_type == "insert":
            clause = clause + ", CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()"

        return clause

    def _identify_deletes(self, df):
        if self.primary_keys is None and not self.primary_keys:
            raise Exception(
                """SnowflakeCDCWriter::_identify_deletes::primary_keys cannot be null for CDC,
                create SnowflakeWriter with primary_keys"""
            )
        windowSpec = Window.partitionBy(*self.primary_keys).orderBy(
            desc("_commit_version")
        )
        return (
            df.where("_change_type != 'update_preimage'")
            .withColumn("rn", row_number().over(windowSpec))
            .withColumn(
                "hard_delete_flag",
                when(col("_change_type") == "delete", 1).otherwise(0),
            )
            .where("rn=1")
            .drop("_change_type", "_commit_version", "_commit_timestamp", "rn")
        )

    def _create_merge_query_cdc(self, cols):
        if self.primary_keys is None and not self.primary_keys:
            raise Exception(
                """SnowflakeCDCWriter::_create_merge_query_cdc::primary_keys cannot be null for write_mode = merge,
                create SnowflakeWriter with primary_keys"""
            )

        self.update_excl_columns.append("_change_type")
        self.update_excl_columns.append("_commit_version")
        self.update_excl_columns.append("_commit_timestamp")

        query = f"""merge into {self.sf_table} as base using TEMP_{self.sf_table} as pre on
                        {self._get_clause(self.primary_keys, "merge_key")}
                    when matched and hard_delete_flag = 0 then
                    update set {self._get_clause(cols, "update")}
                    when matched and hard_delete_flag = 1 then delete
                    when not matched and hard_delete_flag = 0 then
                        insert ({','.join(cols)},SNF_LOAD_TSTMP, SNF_UPDATE_TSTMP)
                        VALUES ({self._get_clause(cols, "insert")})"""

        return query

    def _push_cdc(self, df):
        if not all(
            col_name in df.columns
            for col_name in ("_change_type", "_commit_version", "_commit_timestamp")
        ):  # check if DF has control columns
            raise Exception(
                "The dataframe is missing required columns for CDC _change_type, _commit_version, _commit_timestamp"
            )
        df.persist()
        cdc_df = self._identify_deletes(
            df
        )  # logic to add hard delete flag & drop cdc control columns
        print("add hard delete flag")
        merge_query = self._create_merge_query_cdc(
            [i for i in cdc_df.columns if i not in ["hard_delete_flag"]]
        )
        self._run_sf_query(
            f"DROP TABLE IF EXISTS TEMP_{self.sf_table}"
        )  # drop temp table

        self._write_df_to_sf(
            cdc_df, f"TEMP_{self.sf_table}"
        )  # write temp table in SFLK
        print("merge :", merge_query)
        self._run_sf_query(merge_query)
        self._run_sf_query(f"DROP TABLE TEMP_{self.sf_table}")  # drop temp table
        return df.agg({"_commit_version": "max"}).collect()[0]["max(_commit_version)"]

    def push_cdc(self) -> Optional[int]:
        """Push the cdc data to the snowflake table that is configured for this writer from the
        datalake table that has been configured for this writer"""
        if self.dl_catalog is None:
            table = f"{self.dl_schema}.{self.dl_table}"
        else:
            table = f"{self.dl_catalog}.{self.dl_schema}.{self.dl_table}"

        lastSeenVersion = self.cdc_logger.getLastSeenVersion()

        if lastSeenVersion is None:
            lastSeenVersion = 0

        print(lastSeenVersion)

        cdc_query = f"select * from table_changes('{table}',{lastSeenVersion})"

        print(
            f"""SnowflakeCDCWriter::push_cdc::running cdc query:
              {cdc_query}"""
        )

        df = self.spark.sql(cdc_query)

        count = df.count()

        if count > 0:
            lastSeenVersion = self._push_cdc(df)
            self.cdc_logger.logLastSeenVersion(lastSeenVersion)
            return count
        else:
            print(
                f"""SnowflakeCDCWriter::push_cdc 
                  No changes found for {table}"""
            )
            return None
        