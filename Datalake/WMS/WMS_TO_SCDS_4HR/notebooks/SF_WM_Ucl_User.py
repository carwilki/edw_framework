from pyspark.sql import SparkSession
from logging import getLogger
from Datalake.utils.genericUtilities import getEnvPrefix, ingestToSF
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

deltaTable = refine + ".WM_E_CONSOL_PERF_SMRY"
SFTable = "WM_E_CONSOL_PERF_SMRY_LGCY"

deltaTable = f"{refine}.WM_UCL_USER"
SFTable = "WM_UCL_USER_LGCY"

try:
    ingestToSF(raw, deltaTable, SFTable, env)
    logger.info("Data write to SF completed")
except Exception as e:
    raise e
