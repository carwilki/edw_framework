import argparse
from logging import INFO, getLogger

from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession

from Datalake.utils.genericUtilities import *
from Datalake.utils.mergeUtils import mergeToSFv2

parser = argparse.ArgumentParser()
parser.add_argument("env", type=str, help="Environment value")
parser.add_argument("deltaTable", type=str, help="Delta Table")
parser.add_argument("mode", type=str, help="Writing mode append/overwrite")


args = parser.parse_args()
env = args.env
deltaTable = args.deltaTable
writeMode = str(args.mode).lower()


sfOptions = getSfCredentials(env)
schemaForDeltaTable = getEnvPrefix(env) + "legacy"
SFTable = f"{deltaTable}_lgcy"
df_table = spark.table(f"{schemaForDeltaTable}.{deltaTable}")
df_table = df_table.withColumn("SNF_LOAD_TSTMP", current_timestamp()).withColumn(
    "SNF_UPDATE_TSTMP", current_timestamp()
)

try:
    from Datalake.utils.SF_Merge_Utils_v2 import SnowflakeWriter

    logger.info(
        "Appending/Overwriting data to Snowflake tables for table - ", deltaTable
    )
    if writeMode == "overwrite":
        SnowflakeWriter(sfOptions, SFTable).push_data(df_table, write_mode="full")
    elif writeMode == "append":
        SnowflakeWriter(sfOptions, SFTable).push_data(df_table, write_mode="append")

    logger.info("Data write to SF completed for table - ", deltaTable)
except Exception as e:
    raise e
