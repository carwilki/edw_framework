from pyspark.sql.session import SparkSession
from logging import getLogger, INFO
from Datalake.utils.mergeUtils import mergeToSFLegacy
import argparse
import json


parser = argparse.ArgumentParser()
parser.add_argument("env", type=str, help="Environment value")
parser.add_argument("deltaTable", type=str, help="Delta Table")
parser.add_argument("primaryKeys", type=str, help="Primary Keys to the delta table")
parser.add_argument("conditionCols", type=str, help="condition cols to merge on")


args = parser.parse_args()
env = args.env
deltaTable = args.deltaTable
primaryKeys = [pKey for pKey in args.primaryKeys.split(",")]
conditionCols = [conditionCol for conditionCol in args.conditionCols.split(",")]
primaryKeys_list = json.dumps(primaryKeys)
conditionCols_list = json.dumps(conditionCols)

spark: SparkSession = SparkSession.getActiveSession()
logger = getLogger()
logger.setLevel(INFO)


try:
    logger.info("Ingesting data to Snowflake tables for table - ", deltaTable)
    mergeToSFLegacy(env, deltaTable, primaryKeys_list, conditionCols_list)
    logger.info("Data write to SF completed for table - ", deltaTable)

except Exception as e:
    raise e
