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
        except Exception as e:
            logPrevRunDt(
                "Delta Writer -" + table,
                table,
                "Failed",
                str(e),
                f"{schemaForDeltaTable}.log_run_details",
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
