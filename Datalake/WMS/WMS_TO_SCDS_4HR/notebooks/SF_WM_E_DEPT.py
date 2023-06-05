from argparse import ArgumentParser
from pyspark.sql import SparkSession
from logging import getLogger
from Datalake.utils.genericUtilities import getEnvPrefix, ingestToSF

parser = ArgumentParser()

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

deltaTable = f"{refine}.WM_E_DEPT"
SFTable = "WM_E_DEPT_LGCY"

try:
    ingestToSF(raw, deltaTable, SFTable, env)
    logger.info("Data write to SF completed succesfully")
except Exception as e:
    raise e
