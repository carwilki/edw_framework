
from logging import getLogger,INFO
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from Datalake.utils.logger import logPrevRunDt
from Datalake.utils.mergeUtils import executeMerge

logger=getLogger()
logger.setLevel(INFO)

spark:SparkSession=SparkSession.getActiveSession()
dbutils:DBUtils=DBUtils(spark)
username = dbutils.secrets.get("databricks_service_account", "username")
password = dbutils.secrets.get("databricks_service_account", "password")



def getSfCredentials(env,username,password):

    if env.lower()=='dev_':
        url="petsmart.us-central1.gcp.snowflakecomputing.com"
        db="edw_"+env
        schema="public"
        warehouse="IT_WH"
    if env.lower()=='qa_':
        url="petsmart.us-central1.gcp.snowflakecomputing.com"
        db="edw_"+env
        schema="public"
        warehouse="IT_WH"
    if env.lower()=="prod":
        url="petsmart.us-central1.gcp.snowflakecomputing.com"
        db="edw_prd"
        schema="public"
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




def ingestToSF(schema,deltaTable,SFTable,env):
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        spark:SparkSession=SparkSession.getActiveSession()
        dbutils:DBUtils=DBUtils(spark)
        username = dbutils.secrets.get("databricks_service_account", "username")
        password = dbutils.secrets.get("databricks_service_account", "password")
        options=getSfCredentials(env,username,password)
        df = deltaReader(deltaTable,False)
        sfWriter(df,options,SFTable,"overwrite")
        
        logPrevRunDt("SF Writer -" + SFTable,SFTable,'Completed','N/A',f"{schema}.log_run_details")
    except Exception as e:
        logPrevRunDt("SF Writer -" + SFTable,SFTable,'Failed',str(e),f"{schema}.log_run_details")
        raise e



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
