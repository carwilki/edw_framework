class DeltaLakeWriter:
    def __init__(self, env, sfOptions, table, primary_keys=None):
        from pyspark.sql import SparkSession
        from Datalake.utils.genericUtilities import getSFEnvSuffix

        spark = SparkSession.getActiveSession()
        spark: SparkSession = SparkSession.getActiveSession()
        print("initiating DeltaLake Writer class")
        from pyspark.dbutils import DBUtils

        self.dbutils = DBUtils(spark)
        self.table = table
        self.primary_keys = primary_keys
        self.env = env
        self.sfOptions = sfOptions

    def logRun(
        self, process, table, sf_row_count, delta_row_count, status, error, logTableName
    ):
        from logging import getLogger, INFO
        import json

        logger = getLogger()
        from datetime import datetime as dt

        # Getting current date and time
        now = dt.now()

        s = now.strftime("%Y-%m-%d %H:%M:%S")
        s = str(s)

        context_str = (
            self.dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .toJson()
        )
        context = json.loads(context_str)
        task_name = context.get("tags", {}).get("taskKey", None)
        job_id = context.get("tags", {}).get("jobId", None)
        run_id_obj = context.get("currentRunId", {})
        run_id = run_id_obj.get("id", None) if run_id_obj else None

        if task_name or job_id or run_id is None:
            task_name = "null"
            job_id = run_id = 1

        if status.lower() == "failed":
            logger.info(
                "Inserting failed run details into historical_run_details_from_sf table"
            )
            sql_query = f"""
            INSERT INTO {logTableName}
            (job_id, run_id, task_name,  process, table_name,sf_rowCount, delta_rowCount,  status, error, run_date) VALUES
            ('{job_id}', '{run_id}', '{task_name}', '{process}', '{table}', '{sf_row_count}', '{delta_row_count}', '{status}', '{error}', '{s}')
         """
        else:
            logger.info(
                "Inserting success run details into historical_run_details_from_sf table"
            )
            sql_query = f"""
            INSERT INTO {logTableName}
            (job_id, run_id, task_name,  process, table_name,sf_rowCount, delta_rowCount, status, error, run_date) VALUES
            ('{job_id}', '{run_id}', '{task_name}', '{process}', '{table}', '{sf_row_count}', '{delta_row_count}', '{status}', '{error}', '{s}')
            """

        logger.info("Logging the status")
        logger.info(sql_query)
        spark.sql(sql_query)
        logger.info("Logging Completed")

    def ingestFromSF(self):
        from logging import getLogger, INFO
        from Datalake.utils.genericUtilities import getEnvPrefix, sfReader

        logger = getLogger()

        schemaForDeltaTable = getEnvPrefix(self.env) + "refine"

        try:
            logger.info(f"Getting data for table {0}".format(self.table))
            df = sfReader(self.sfOptions, self.table)
            df.write.format("delta").saveAsTable(
                f"{0}.{1}".format(schemaForDeltaTable, self.table, "overwrite")
            )
            sf_row_count = df.count()
            delta_row_count = spark.sql(
                f"select count(*) from {0}.{1}".format(schemaForDeltaTable, self.table)
            )
            if sf_row_count == delta_row_count:
                logger.info("All records have been ingested to delta lake")
            else:
                logger.info("Records mismatch")
            self.logRun(
                process="Delta Writer -" + self.table,
                table=self.table,
                sf_row_count=sf_row_count,
                delta_row_count=delta_row_count,
                status="Succeeded",
                error=None,
                logTableName=f"{schemaForDeltaTable}.historical_run_details_from_sf",
            )
        except Exception as e:
            self.logRun(
                process="Delta Writer -" + self.table,
                table=self.table,
                sf_row_count=None,
                delta_row_count=None,
                status="Failed",
                error=str(e),
                logTableName=f"{schemaForDeltaTable}.historical_run_details_from_sf",
            )
            raise e
