import argparse
from Datalake.utils.genericUtilities import getSfCredentials
from logging import getLogger
import json
from Datalake.utils.genericUtilities import getEnvPrefix, sfReader
from Datalake.utils.logger import logPrevRunDt


parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()

parser.add_argument("env", type=str, help="Env Variable")
parser.add_argument("table_list", type=str, help="list of tables")
args = parser.parse_args()
env = args.env
table_list = args.table_list
table_list = [table for table in args.table_list.split(",")]
table_list = json.dumps(table_list)


def logHistoricalRun(table_name, status, error, sf_rowCount, delta_rowCount):
    from logging import getLogger, INFO

    logger = getLogger()
    from datetime import datetime as dt

    # Getting current date and time
    now = dt.now()

    s = now.strftime("%Y-%m-%d %H:%M:%S")
    s = str(s)

    context_str = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
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
        logger.info("Inserting failed run details into log_run_details table")
        sql_query = f"""
        INSERT INTO {logTableName}
        (job_id, run_id, task_name,  process, table_name, status, error, prev_run_date) VALUES
        ('{job_id}', '{run_id}', '{task_name}', '{process}', '{table_name}', '{status}', '{error}', null)
        """
    else:
        logger.info("Inserting success run details into log_run_details table")
        sql_query = f"""
        INSERT INTO {logTableName}
        (job_id, run_id, task_name,  process, table_name, status, error, prev_run_date) VALUES
        ('{job_id}', '{run_id}', '{task_name}', '{process}', '{table_name}', '{status}', '{error}', '{s}')
        """

    logger.info("Logging the status")
    logger.info(sql_query)
    spark.sql(sql_query)
    logger.info("Logging Completed")


def IngestFromSFHistoricalData(env, table_list):
    print("Ingest_historical_data_from_Snowflake_to_deltalake for table")

    logger = getLogger()
    sfOptions = getSfCredentials(env)

    schemaForDeltaTable = getEnvPrefix(env) + "refine"

    for table in table_list:
        try:
            logger.info(f"Getting data for table {0}".format(table))
            df = sfReader(sfOptions, table)
            df.write.format("delta").saveAsTable(
                f"{0}.{1}".format(schemaForDeltaTable, table)
            )
            logPrevRunDt(
                "Delta Writer -" + table,
                table,
                "Succeeded",
                NULL,
                f"{schemaForDeltaTable}.historical_run_details_from_sf",
            )
        except Exception as e:
            logPrevRunDt(
                "Delta Writer -" + table,
                table,
                "Failed",
                str(e),
                f"{schemaForDeltaTable}.historical_run_details_from_sf",
            )
            raise e

        sf_row_count = df.count()
        delta_row_count = spark.sql(
            f"select count(*) from {0}.{1}".format(schemaForDeltaTable, table)
        )

    if sf_row_count == delta_row_count:
        logger.info("All records have been ingested to delta lake")
    else:
        logger.info("Records mismatch")


IngestFromSFHistoricalData(env, table_list)
