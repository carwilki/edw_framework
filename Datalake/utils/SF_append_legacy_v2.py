from pyspark.sql.session import SparkSession
from Datalake.utils.mergeUtils import mergeToSFv2
from Datalake.utils.genericUtilities import *
from logging import getLogger, INFO
import argparse
from pyspark.sql.functions import *
from Datalake.utils.SF_Merge_Utils_v2 import SnowflakeWriter, getAppendQuery,getAppendQuery_tstm
import json


parser = argparse.ArgumentParser()
parser.add_argument("env", type=str, help="Environment value")
parser.add_argument("deltaTable", type=str, help="Delta Table")
parser.add_argument("conditionCols", type=str, help="condition cols to merge on")
parser.add_argument("parallelism", type=str, help="D - Default, L - 30, XL- 60, XXL - 120, XXXL- 400")


args = parser.parse_args()
env = args.env
deltaTable = args.deltaTable
conditionCols = args.conditionCols
conditionCols = [conditionCol for conditionCol in args.conditionCols.split(",")]
conditionCols = json.dumps(conditionCols)
parallelism=args.parallelism

sfOptions = getSfCredentials(env)
schemaForDeltaTable = getEnvPrefix(env) + "legacy"
SFTable = f"{deltaTable}_lgcy"
df_table = spark.table(f"{schemaForDeltaTable}.{deltaTable}")

append_query = getAppendQuery_tstm(env, deltaTable, conditionCols)

mergeDatasetSql = (
    f"""select * from `{schemaForDeltaTable}`.`{deltaTable}` where {append_query}"""
)
print(mergeDatasetSql)
df_table = spark.sql(mergeDatasetSql)

df_table = df_table.withColumn("SNF_LOAD_TSTMP", current_timestamp()).withColumn(
    "SNF_UPDATE_TSTMP", current_timestamp()
)
#df_table.display(20)
if  (parallelism == 'L'):
    df_table = df_table.repartition(32)    #4 cores * 8 nodes 
elif (parallelism == 'XL'):
    df_table = df_table.repartition(64)     #8 cores * 8 nodes 
elif (parallelism == 'XXL'):
    df_table = df_table.repartition(128)     #16 cores * 8 nodes     
elif (parallelism == 'XXXL'):
    df_table = df_table.repartition(256)     #32 cores * 8 nodes     
elif (parallelism == 'XXXXL'):
    df_table = df_table.repartition(384)     #48 cores * 8 nodes    
elif (parallelism == 'XXXXXL'):
    df_table = df_table.repartition(512)     #64 cores * 8 nodes        
else:
    df_table = df_table    

row_count = df_table.count()
print('row count ' + str(row_count))

if row_count == 0:
    print("No record to log")
    logger.info("No new records to insert or update into Snowflake")
else:
    try:
        logger.info("Appending data to Snowflake tables for table - ", deltaTable)
        SnowflakeWriter(sfOptions, SFTable).push_data(df_table, write_mode="append")

        logger.info("Data appending to SF completed for table - ", deltaTable)
    except Exception as e:
        raise e
