class SnowflakeWriter:
    def __init__(
        self, env, database, schema, table, primary_keys=None, update_excl_columns=[]
    ):
        from pyspark.sql import SparkSession
        from Datalake.utils.genericUtilities import getSFEnvSuffix

        spark: SparkSession = SparkSession.getActiveSession()
        print("initiating SF Writer class")
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        self.update_excl_columns = [x.lower() for x in update_excl_columns]
        self.table = table
        self.primary_keys = primary_keys
        self.env = env
        print(env)
        envSuffix = getSFEnvSuffix(self.env)
        self.sfOptions = {
            "sfUrl": "petsmart.us-central1.gcp.snowflakecomputing.com",
            "sfUser": dbutils.secrets.get("databricks_service_account", "username"),
            "sfPassword": dbutils.secrets.get("databricks_service_account", "password"),
            "sfDatabase": database,
            "sfSchema": schema,
            "sfWarehouse": "IT_WH",
            "authenticator": "https://petsmart.okta.com",
            "autopushdown": "on",
            "sfRole": f"edw{envSuffix}_owner",
        }

    def run_sf_query(self, query):
        from pyspark.sql import SparkSession

        spark: SparkSession = SparkSession.getActiveSession()

        spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
            self.sfOptions, query
        )

    def write_df_to_sf(self, df, table=None):
        if table is None:
            table = self.table
        df.write.format("net.snowflake.spark.snowflake").options(
            **self.sfOptions
        ).option("dbtable", table).mode("overwrite").save()

    def get_clause(self, column_list, clause_type):
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

        return clause

    def create_upsert_query(self, cols):
        if self.primary_keys is None and not self.primary_keys:
            raise Exception(
                f"primary_keys cannot be null for write_mode = merge, create SnowflakeWriter with primary_keys"
            )
        return f"""merge into {self.table} as base using TEMP_{self.table} as pre on 
      {self.get_clause(self.primary_keys, "merge_key")}
      when matched then update set
      {self.get_clause(cols, "update")}
      when not matched then insert ({','.join(cols)}) VALUES ({self.get_clause(cols, "insert")})"""

    def push_data(self, df, write_mode="merge"):
        if write_mode.lower() == "merge":
            upsert_query = self.create_upsert_query(df.columns)
            self.write_df_to_sf(df, f"TEMP_{self.table}")
            print("running upsert ", upsert_query)
            self.run_sf_query(upsert_query)
            # self.run_sf_query(f"TRUNCATE TABLE TEMP_{self.table}")
        elif write_mode.lower() == "full":
            self.run_sf_query(f"TRUNCATE TABLE {self.table}")
            self.write_df_to_sf(df)
        elif write_mode.lower() == "append":
            self.write_df_to_sf(df)
        else:
            raise Exception(f"{write_mode} not supported. Try : merge, full or append")


def getAppendQuery(env, deltaTable, conditionCols):
    print("get Append query")
    from pyspark.sql import SparkSession
    from Datalake.utils.genericUtilities import getEnvPrefix

    spark: SparkSession = SparkSession.getActiveSession()
    import json
    from datetime import datetime, timedelta

    raw = getEnvPrefix(env) + "raw"

    prev_run_dt = spark.sql(
        f"""select max(prev_run_date)  from {raw}.log_run_details where table_name='{deltaTable}' and lower(status)= 'completed'"""
    ).collect()[0][0]
    prev_run_dt = datetime.strptime(str(prev_run_dt), "%Y-%m-%d %H:%M:%S")
    prev_run_dt = prev_run_dt - timedelta(days=7)
    prev_run_dt = prev_run_dt.strftime("%Y-%m-%d")
    append_query = ""
    for i in json.loads(conditionCols):
        if json.loads(conditionCols).index(i) == 0:
            append_query = append_query + f"""{i} >= '{prev_run_dt}'"""
        else:
            append_query = append_query + f""" or {i} >= '{prev_run_dt}'"""

    return append_query
