from pyspark.dbutils import DBUtils
from pyspark.sql.session import SparkSession
from logging import getLogger, INFO
from Datalake.utils.genericUtilities import getEnvPrefix, ingestToSF
from Datalake.utils.logger import logPrevRunDt
from Datalake.utils.mergeUtils import executeMerge
import argparse


parser = argparse.ArgumentParser()

spark: SparkSession = SparkSession.getActiveSession()

parser.add_argument("env", type=str, help="Env Variable")
args = parser.parse_args()
env = args.env

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"
logger = getLogger()
logger.setLevel(INFO)
deltaTable = refine + ".WM_E_CONSOL_PERF_SMRY"
SFTable = "WM_E_CONSOL_PERF_SMRY_LGCY"

try:
    logger.info("Ingesting data to Snowflake tables")
    ingestToSF(raw, deltaTable, SFTable, env)
    logger.info("Data write to SF completed")
except Exception as e:
    raise e
