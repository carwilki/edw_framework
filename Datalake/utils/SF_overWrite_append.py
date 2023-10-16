import argparse
from logging import INFO, getLogger

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
mode = args.mode


sfOptions = getSfCredentials(env)
schemaForDeltaTable = getEnvPrefix(env) + "refine"
SFTable = f"{deltaTable}"
df = spark.table(f"{schemaForDeltaTable}.{deltaTable}")

try:
    from Datalake.utils.SF_Merge_Utils_v2 import SnowflakeWriter
    logger.info("Ingesting data to Snowflake tables for table - ", deltaTable)
    if mode == "overwrite":
        SnowflakeWriter(sfOptions, SFTable).push_data(df, write_mode="full")
    elif mode == "append":
        SnowflakeWriter(sfOptions, SFTable).push_data(df, write_mode="append")
    logger.info("Data write to SF completed for table - ", deltaTable)
except Exception as e:
    raise e
