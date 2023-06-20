
from logging import getLogger,INFO
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from Datalake.utils.logger import logPrevRunDt


logger=getLogger()
logger.setLevel(INFO)

spark:SparkSession=SparkSession.getActiveSession()
dbutils:DBUtils=DBUtils(spark)




def getSfCredentials(env):
  print("getting SF credentials")
  username = dbutils.secrets.get("databricks_service_account", "username")
  password = dbutils.secrets.get("databricks_service_account", "password")

  envSuffix = getSFEnvSuffix(env)
  url="petsmart.us-central1.gcp.snowflakecomputing.com"
  db= f"edw{envSuffix}"
  schema = "PUBLIC"
  warehouse = "IT_WH"
 

  sfOptions = {"env": env, "sfUrl": url,"sfUser": username,"sfPassword": password,"sfDatabase": db,"sfSchema": schema,"sfWarehouse": warehouse,"authenticator" : "https://petsmart.okta.com"}
  
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

def getSFEnvSuffix(env:str):
    print(env)
    if env.lower()=='dev':
        envSuffix='_dev'
    elif env.lower()=='qa':
        envSuffix='_qa'
    elif env.lower()=='prod':
        envSuffix='_prd'
    else:
        raise Exception("Invalid environment")
    return envSuffix

def genPrevRunDt(refine_table_name,refine,raw):
  print("get Prev_run date")
  from Datalake.utils.configs import getMaxDate
  from datetime import datetime

  prev_run_dt = spark.sql(f"""select max(prev_run_date)
        from {raw}.log_run_details
        where table_name='{refine_table_name}' and lower(status)= 'completed'"""
    ).collect()[0][0]
  logger.info("Extracted prev_run_dt from log_run_details table")


  if prev_run_dt is None:
    logger.info("Prev_run_dt is none so getting prev_run_dt from getMaxDate function")
    prev_run_dt = getMaxDate(refine_table_name, refine)

  else:
    prev_run_dt = datetime.strptime(str(prev_run_dt), "%Y-%m-%d %H:%M:%S")
    prev_run_dt = prev_run_dt.strftime("%Y-%m-%d")
  
  return prev_run_dt



def jdbcOracleConnection(query,username,password,connection_string):

  df = (
        spark.read.format("jdbc")
        .option("url", connection_string)
        .option("query", query)
        .option("user", username)
        .option("password", password)
        .option("numPartitions", 3)
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("fetchsize", 10000)
        .option("oracle.jdbc.timezoneAsRegion", "false")
        .option(
            "sessionInitStatement",
            """begin 
            execute immediate 'alter session set time_zone=''-07:00''';
            end;
        """,
        )
        .load()
    )
  return df



def overwriteDeltaPartition(df,partition,partitionvalue,target_table_name):
  df.write.partitionBy(partition).mode("overwrite").option(
        "replaceWhere", f"{partition}={partitionvalue}"
    ).saveAsTable(target_table_name)



def parseArgEnv(env):
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument(env, type=str, help=f"{env} Variable")
  args = parser.parse_args()
  #env = args.env
  return args




