import json
from logging import INFO
import logging
import sys
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.getActiveSession()
dbutils = DBUtils(spark)


def getLogger() -> logging.Logger:
    FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO, stream=sys.stdout)
    return logging.getLogger()


# Function to Log the Success/Failure to log_run_details table
# #Usage   - logPrevRunDt('test','test','Completed','N/A',"devrefine.log_run_details")
def logPrevRunDt(process, table_name, status, error, logTableName):
    logger = getLogger()
    from datetime import datetime as dt

    escape_charlist = str.maketrans(
        {
            "-": r"\-",
            "]": r"\]",
            "\\": r"\\",
            "^": r"\^",
            "$": r"\$",
            "*": r"\*",
            ".": r"\.",
            "'": r"\'",
        }
    )
    error = error.translate(escape_charlist)

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
