# Databricks notebook source
import logging

logger=logging.getLogger()
logger.setLevel(logging.INFO)

# COMMAND ----------

# MAGIC %run ./logger

# COMMAND ----------

username = dbutils.secrets.get("databricks_service_account", "username")
password = dbutils.secrets.get("databricks_service_account", "password")

# COMMAND ----------

def getSfCredentials(env,username,password):

    if env.lower()=='dev':
        url="petsmart.us-central1.gcp.snowflakecomputing.com"
        db="edw_"+env
        schema="public"
        warehouse="IT_WH"
    if env.lower()=='qa':
        url="petsmart.us-central1.gcp.snowflakecomputing.com"
        db="edw_"+env
        schema="public"
        warehouse="IT_WH"
    if env.lower()=='prod':
        url="petsmart.us-central1.gcp.snowflakecomputing.com"
        db="edw_"+env
        schema="public"
        warehouse="IT_WH"  

    sfOptions = {"sfUrl": url,"sfUser": username,"sfPassword": password,"sfDatabase": db,"sfSchema": schema,"sfWarehouse": warehouse,"authenticator" : "https://petsmart.okta.com"}
    
    return sfOptions


# COMMAND ----------

def sfWriter(df,options,tblName,mode):
    df.write.format("snowflake") \
    .options(**options) \
    .option("dbtable", tblName) \
    .mode(mode) \
    .save()

# COMMAND ----------

def deltaReader(tblReference,isPath):
    if isPath:
        df=spark.read.format('delta').load(tblReference)
    else :
        df=spark.table(tblReference)    
    return df


# COMMAND ----------

def ingestToSF(env,deltaTable,SFTable):
    try:
        options=getSfCredentials(env,username,password)
        df = deltaReader(deltaTable,False)
        sfWriter(df,options,SFTable,"overwrite")
        
        logPrevRunDt("SF Writer -" + SFTable,SFTable,'Completed','N/A',f"{env}_raw.log_run_details")
    except Exception as e:
        logPrevRunDt("SF Writer -" + SFTable,SFTable,'Failed',str(e),f"{env}_raw.log_run_details")
        raise e

# COMMAND ----------

# try:
#     options=getSfCredentials(env,username,password)
#     tblReference='dev_refine.WM_E_CONSOL_PERF_SMRY'
#     tblName='WM_E_CONSOL_PERF_SMRY_LGCY'

#     df = deltaReader(tblReference,False)
#     sfWriter(df,options,tblName,"overwrite")
#     logger.info('Data write to SF completed')
#     logPrevRunDt("SF Writer -" + tblName,tblName,'Completed','N/A',f"{env}_raw.log_run_details")
# except Exception as e:
#     logPrevRunDt("SF Writer -" + tblName,tblName,'Failed',str(e),f"{env}_raw.log_run_details")
#     raise e
