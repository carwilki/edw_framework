# Databricks notebook source
from pyspark.dbutils import DBUtils
from pyspark.sql.session import SparkSession
from logging import getLogger
from Datalake.WMS.notebooks.utils.genericUtilities import getEnvPrefix,ingestToSF


spark: SparkSession = SparkSession.getActiveSession()
dbutils: DBUtils = DBUtils(spark)
env = dbutils.jobs.taskValues.get(key='env', default='')

if env is None or env == "":
    raise ValueError("env is not set")

logger = getLogger()
refine = getEnvPrefix(env)+'refine'
raw = getEnvPrefix(env)+'raw'
legacy = getEnvPrefix(env)+'legacy'

deltaTable=refine+'.WM_E_CONSOL_PERF_SMRY'
SFTable='WM_E_CONSOL_PERF_SMRY_LGCY'

try:
    ingestToSF(raw,deltaTable,SFTable)
    logger.info('Data write to SF completed')
except Exception as e:
    raise e
