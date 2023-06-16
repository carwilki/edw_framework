
from logging import getLogger,INFO
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from Datalake.utils.logger import logPrevRunDt
# from Datalake.utils.mergeUtils import executeMerge

logger=getLogger()
logger.setLevel(INFO)

spark:SparkSession=SparkSession.getActiveSession()
dbutils:DBUtils=DBUtils(spark)




def getSfCredentials():
  username = dbutils.secrets.get("databricks_service_account", "username")
  password = dbutils.secrets.get("databricks_service_account", "password")
  if "dev" in spark.conf.get("spark.databricks.clusterUsageTags.gcpProjectId"):
    env = "DEV"
  elif "qa" in spark.conf.get("spark.databricks.clusterUsageTags.gcpProjectId"):
    env = "QA"
  else:
    env = "PRD"

  if env.lower()=='dev':
      url="petsmart.us-central1.gcp.snowflakecomputing.com"
      db="edw_"+env
      schema = "PUBLIC"
      warehouse = "IT_WH"
  if env.lower()=='qa':
      url="petsmart.us-central1.gcp.snowflakecomputing.com"
      db="edw_"+env
      schema = "PUBLIC"
      warehouse="IT_WH"
  if env.lower()=="prod":
      url="petsmart.us-central1.gcp.snowflakecomputing.com"
      db="edw_prd"
      schema = "PUBLIC"
      warehouse="IT_WH"  

  sfOptions = {"sfUrl": url,"sfUser": username,"sfPassword": password,"sfDatabase": db,"sfSchema": schema,"sfWarehouse": warehouse,"authenticator" : "https://petsmart.okta.com"}
  
  return sfOptions




def sfWriter(df,options,tblName,mode):
    df.write.format("snowflake") \
    .options(**options) \
    .option("dbtable", tblName) \
    .mode(mode) \
    .save()



def deltaReader(tblReference,isPath):
    if isPath:
        df=spark.read.format('delta').load(tblReference)
    else :
        df=spark.table(tblReference)    
    return df




# def ingestToSF(schema,deltaTable,SFTable,env):
#     from logging import getLogger, INFO
#     logger = getLogger()
    
#     try:
#         from pyspark.dbutils import DBUtils
#         from pyspark.sql import SparkSession
#         from Datalake.utils.logger import logPrevRunDt
#         from Datalake.utils.genericUtilities import sfWriter,getSfCredentials,deltaReader
#         from Datalake.utils.mergeUtils import executeMerge
        
#         spark:SparkSession=SparkSession.getActiveSession()
#         dbutils:DBUtils=DBUtils(spark)

#         username = dbutils.secrets.get("databricks_service_account", "username")
#         password = dbutils.secrets.get("databricks_service_account", "password")
#         logger.info("username and password obtained from secrets")

#         options=getSfCredentials(env,username,password)
#         logger.info("env, username and password obtained successfully")
#         df = deltaReader(deltaTable,False)
#         logger.info("sfWriter function is called")
#         sfWriter(df,options,SFTable,"overwrite")
#         logger.info("dataframe written to snowflake successfully")
        
#         logPrevRunDt("SF Writer -" + SFTable,SFTable,'Completed','N/A',f"{schema}.log_run_details")
#     except Exception as e:
#         logPrevRunDt("SF Writer -" + SFTable,SFTable,'Failed',str(e),f"{schema}.log_run_details")
#         raise e



#for the env we need to get the env prefix
#if the env is != 'prod' then we need to add the env prefix to the table name
def getEnvPrefix(env:str):
    if env.lower()=='dev':
        envPrefix='dev_'
    elif env.lower()=='qa':
        envPrefix='qa_'
    elif env.lower()=='prod':
        envPrefix=''
    else:
        raise Exception("Invalid environment")
    return envPrefix
  

def importUtilities():
  import argparse
  from datetime import datetime
  from pyspark.sql.session import SparkSession
  from pyspark.sql.types import DecimalType, StringType, TimestampType

  from Datalake.utils.configs import getConfig, getMaxDate
  from Datalake.utils.genericUtilities import getEnvPrefix
  from logging import getLogger, INFO
  from pyspark.dbutils import DBUtils
  from pyspark.sql.functions import (col,
      lit,
      when,
      current_timestamp,
      monotonically_increasing_id,
    )
  from Datalake.utils.logger import logPrevRunDt
  from Datalake.utils.mergeUtils import executeMerge

  spark: SparkSession = SparkSession.getActiveSession()

  logger = getLogger()
  return logger,spark
