import argparse
import json
from logging import INFO, getLogger

from pyspark.sql.session import SparkSession

from Datalake.utils.mergeUtils import *

parser = argparse.ArgumentParser()
parser.add_argument("env", type=str, help="Environment value")
parser.add_argument("deltaTablePIISchema", type=str, help="Delta Table PII Schema")
parser.add_argument("deltaTable", type=str, help="Delta Table")
parser.add_argument("primaryKeys", type=str, help="Primary Keys to the delta table")
parser.add_argument("conditionCols", type=str, help="condition cols to merge on")
parser.add_argument("sfSchema", type=str, help="snowflake table schema")
parser.add_argument("refineOrLegacy", type=str, help="delta table schema")
parser.add_argument("mode", type=str, help="overwrite/append/merge")


args = parser.parse_args()
env = args.env
deltaTable = args.deltaTable
primaryKeys = [pKey for pKey in args.primaryKeys.split(",")]
conditionCols = [conditionCol for conditionCol in args.conditionCols.split(",")]
primaryKeys_list = json.dumps(primaryKeys)
conditionCols_list = json.dumps(conditionCols)
deltaTableSchema = args.deltaTablePIISchema
SFSchema = args.sfSchema
mode = args.mode
refineOrLegacy = args.refineOrLegacy


spark: SparkSession = SparkSession.getActiveSession()
logger = getLogger()
logger.setLevel(INFO)


try:
    logger.info("Ingesting data to Snowflake tables for table - ", deltaTable)
    mergeToSFPII(
        env,
        deltaTableSchema,
        deltaTable,
        primaryKeys_list,
        conditionCols_list,
        refineOrLegacy,
        mode,
        SFSchema,
    )
    logger.info("Data write to SF completed for table - ", deltaTable)

except Exception as e:
    raise e
