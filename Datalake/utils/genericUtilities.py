import Datalake.utils.secrets as secrets
from logging import INFO, getLogger

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from Datalake.utils.logger import logPrevRunDt

logger = getLogger()
logger.setLevel(INFO)

spark: SparkSession = SparkSession.getActiveSession()
dbutils: DBUtils = DBUtils(spark)


def getSfCredentials(env):
    print("getting SF credentials")

    envSuffix = getSFEnvSuffix(env)
    if env == "dev" or env == "qa":
        sfRole = "role_databricks_nonprd"
    elif env == "prod":
        sfRole = "role_databricks_prd"

    if env == "dev" or env == "qa":
        username = dbutils.secrets.get("SVC_BD_SNOWFLAKE_NP", "username")
    elif env == "prod":
        username = dbutils.secrets.get("SVC_BD_SNOWFLAKE_P", "username")

    if env == "dev" or env == "qa":
        private_key = dbutils.secrets.get("SVC_BD_SNOWFLAKE_NP", "pkey")
    elif env == "prod":
        private_key = dbutils.secrets.get("SVC_BD_SNOWFLAKE_P", "pkey")

    url = "petsmart.us-central1.gcp.snowflakecomputing.com"
    db = f"edw{envSuffix}"
    schema = "PUBLIC"
    warehouse = "IT_WH"

    sfOptions = {
        "env": env,
        "sfUrl": url,
        "sfUser": username,
        "pem_private_key": private_key,
        "sfDatabase": db,
        "sfSchema": schema,
        "sfWarehouse": warehouse,
        "autopushdown": "on",
        "sfRole": sfRole,
    }

    return sfOptions


def sfWriter(df, options, tblName, mode):
    df.write.format("snowflake").options(**options).option("dbtable", tblName).mode(
        mode
    ).save()


def sfReader(options, query):
    df = spark.read.format("snowflake").options(**options).option("query", query).load()
    return df


def deltaReader(tblReference, isPath):
    if isPath:
        df = spark.read.format("delta").load(tblReference)
    else:
        df = spark.table(tblReference)
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

#         username = secrets.get("databricks_service_account", "username")
#         password = secrets.get("databricks_service_account", "password")
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


# for the env we need to get the env prefix
# if the env is != 'prod' then we need to add the env prefix to the table name
def getEnvPrefix(env: str):
    if env.lower() == "dev":
        envPrefix = "dev_"
    elif env.lower() == "qa":
        envPrefix = "qa_"
    elif env.lower() == "prod":
        envPrefix = ""
    else:
        raise Exception("Invalid environment")
    return envPrefix


def getSFEnvSuffix(env: str):
    print(env)
    if env.lower() == "dev":
        envSuffix = "_dev"
    elif env.lower() == "qa":
        envSuffix = "_qa"
    elif env.lower() == "prod":
        envSuffix = "_prd"
    else:
        raise Exception("Invalid environment")
    return envSuffix


# Schema for custsensitive prefix. In non prod schema is prefixed with 'data_harness_'
def getCustSensitivePrefix(env: str):
    print(env)
    if env.lower() == "dev":
        custSensitivePrefix = "data_harness_"
    elif env.lower() == "qa":
        custSensitivePrefix = "data_harness_"
    elif env.lower() == "prod":
        custSensitivePrefix = ""
    else:
        raise Exception("Invalid environment")
    return custSensitivePrefix


def genPrevRunDt(refine_table_name, refine, raw):
    print("get Prev_run date")
    from datetime import datetime

    from Datalake.utils.configs import getMaxDate

    prev_run_dt = spark.sql(
        f"""select max(prev_run_date)
        from {raw}.log_run_details
        where table_name='{refine_table_name}' and lower(status)= 'completed'"""
    ).collect()[0][0]
    logger.info("Extracted prev_run_dt from log_run_details table")

    if prev_run_dt is None:
        logger.info(
            "Prev_run_dt is none so getting prev_run_dt from getMaxDate function"
        )
        prev_run_dt = getMaxDate(refine_table_name, refine)

    else:
        prev_run_dt = datetime.strptime(str(prev_run_dt), "%Y-%m-%d %H:%M:%S")
        prev_run_dt = prev_run_dt.strftime("%Y-%m-%d")

    return prev_run_dt


def genPrevRunDtFlatFile(refine_table_name, raw):
    print("get Prev_run date")
    from datetime import datetime , date , timedelta

    refine_table_name=refine_table_name.lower()
    prev_run_dt = spark.sql(
        f"""select max(prev_run_date)
        from {raw}.log_run_details
        where lower(table_name)='{refine_table_name}' and lower(status)= 'completed'"""
    ).collect()[0][0]
    print("Extracted prev_run_dt from log_run_details table")

    if prev_run_dt is None:
        print(
            "Prev_run_dt is none so getting prev_run_dt from current date-1 "
        )
        prev_run_dt = str(date.today() - timedelta(days=1))

    else:
        prev_run_dt = datetime.strptime(str(prev_run_dt), "%Y-%m-%d %H:%M:%S")
        prev_run_dt = prev_run_dt.strftime("%Y-%m-%d")
    print(prev_run_dt)
    return prev_run_dt


def jdbcOracleConnection(query, username, password, connection_string):
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


def jdbcSqlServerConnection(query, username, password, connection_string):
    df = spark.read.jdbc(
        url=connection_string,
        table=query,
        properties={
            "Driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user": username,
            "password": password,
        },
    )
    return df


def overwriteDeltaPartition(df, partition, partitionvalue, target_table_name):
    df.write.partitionBy(partition).mode("overwrite").option(
        "replaceWhere", f"{partition}={partitionvalue}"
    ).saveAsTable(target_table_name)


def parseArgEnv(env):
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(env, type=str, help=f"{env} Variable")
    args = parser.parse_args()
    # env = args.env
    return args


def getParameterValue(raw, param_file_name, param_section, param_key):
    from datetime import datetime
    from Datalake.utils.configs import getMaxDate

    param_value = spark.sql(
        f"""select parameter_value
        from {raw}.parameter_config
        where parameter_file_name='{param_file_name}' and parameter_section='{param_section}' and parameter_key='{param_key}'"""
    ).collect()[0][0]
    logger.info("Extracted param_value from parameter_config table")

    return param_value


def resetPrevRunDt(input_csv, reset_date, logTableName):
    import pandas
    from datetime import datetime
    from pyspark.sql.functions import col

    df = pandas.read_csv(input_csv, header=0)
    tblList = df.tables.values.tolist()

    s = datetime.strptime(reset_date, "%Y-%m-%d %H:%M:%S")
    s = str(s)

    spark.table(logTableName).filter(
        col("table_name").isin(tblList)
    ).createOrReplaceTempView("resetTable")
    spark.sql(
        f"""delete from {logTableName} where task_name in (select task_name from resetTable)"""
    )

    print("The list of tables deleted are " + str(tblList))

    ins_sql_query = f"""
          INSERT INTO {logTableName}
          (job_id, run_id, task_name,  process, table_name, status, error, prev_run_date) VALUES
          """

    tblList = [x.upper().strip() for x in tblList]

    for tbl in tblList:
        ins_sql_query = (
            ins_sql_query
            + f"""('1', '1', 'null', 'Reset Utility', '{tbl}', 'Completed', 'NA', '{s}'),"""
        )

    index = len(ins_sql_query)
    ins_sql_query = ins_sql_query[: index - 1]

    print("Final Insert Query is " + ins_sql_query)

    spark.sql(ins_sql_query)

    print("Reset is completed for the tables in the query " + ins_sql_query)
    
def removeTransactionFiles(filePath):
    fileList = dbutils.fs.ls(filePath)

    for file in fileList:
        if (
            file.name.startswith("_SUCCESS")
            or file.name.startswith("_started")
            or file.name.startswith("_committed")
        ):
            print(file.name)
            dbutils.fs.rm(filePath.strip("/") + "/" + file.name)


def renamePartFileName(filePath, newFilename):
    fileList = dbutils.fs.ls(filePath)

    for file in fileList:
        if file.name.startswith("part-0000"):
            print(file.name)
            partFileName = filePath.strip("/") + "/" + file.name
            print('part file name:',partFileName)
            dbutils.fs.mv(partFileName, newFilename)


def writeToFlatFile(df, filePath, fileName, mode):
    print(filePath)
    if mode == 'overwrite':
        dbutils.fs.rm(filePath.strip("/") + "/",True)
        
    df.repartition(1).write.mode(mode).option("header", "True").option(
        "inferSchema", "true"
    ).option("delimiter", "|").option("ignoreTrailingWhiteSpace","False").csv(filePath)
    print('File added to GCS Path')
    removeTransactionFiles(filePath)
    newFilePath=filePath.strip("/") + "/" + fileName

    renamePartFileName(filePath, newFilePath)

import os,paramiko
def execute_cmd_on_edge_node (cmd_parameter,mykey): 

    try:
      from StringIO import StringIO
    except ImportError:
      from io import StringIO
    
    from datetime import datetime
    
    p = paramiko.SSHClient()
    p.load_system_host_keys()
    p.set_missing_host_key_policy(paramiko.AutoAddPolicy()) 
    p.connect("10.120.0.80", port=22, username="gcpdatajobs-shared_petsmart_com", pkey=mykey)

    tr = p.get_transport()
    p.default_max_packet_size = 300000000
    p.default_window_size = 100000000
    getcmd=cmd_parameter

    print("[Info] "+datetime.now().astimezone().strftime("%d-%b-%Y %I:%M:%S %p %Z")+" --> Command Execution Starts \n" +getcmd)

    stdin, stdout, stderr = p.exec_command(getcmd)
    error_message = stderr.readlines()
    stdout_message = stdout.readlines()

    if stdout.channel.recv_exit_status() > 0:
        err_out = "".join(map(str, error_message))
        print("\n[Error] "+datetime.now().astimezone().strftime("%d-%b-%Y %I:%M:%S %p %Z")+" -->  Command Execution Failed\n")
        print("[Error] "+datetime.now().astimezone().strftime("%d-%b-%Y %I:%M:%S %p %Z")+" -->  Error Message : \n"+err_out)
        p.close()
        raise Exception("Command Execution Failed!")
    else:
        std_out = "".join(map(str, stdout_message))
        print("\n[Info] "+datetime.now().astimezone().strftime("%d-%b-%Y %I:%M:%S %p %Z")+" -->  Command Execution Successfull\n")
        print("[Info] "+datetime.now().astimezone().strftime("%d-%b-%Y %I:%M:%S %p %Z")+" -->  Standard Output Message : \n"+std_out)
        p.close()
        return std_out
spark.udf.register("execute_cmd_on_edge_node", execute_cmd_on_edge_node)

def copy_file_to_nas(gs_source_path,nas_target_path):
      try:
        from StringIO import StringIO
      except ImportError:
        from io import StringIO

      key_string=dbutils.secrets.get(scope = "dataprocedgenode-creds",key = "pkey")
      keyfile = StringIO(key_string)
      mykey = paramiko.RSAKey.from_private_key(keyfile)
      execute_cmd_on_edge_node("gsutil cp "+gs_source_path+ " "+nas_target_path, mykey)
