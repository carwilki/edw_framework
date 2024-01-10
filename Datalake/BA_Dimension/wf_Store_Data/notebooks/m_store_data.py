# Databricks notebook source
#Code converted on 2023-10-25 16:52:07
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

#  THIS FUNCTION SHOULD BE IN merge utils not pushed to main yet
# def executeMergeNoAction(sourceDataFrame, targetTable, primaryKeyString):
#     from logging import getLogger, INFO
#     logger = getLogger()    

#     try:
#         logger.info("executing executeMergeNoAction Function")

#         sourceDataFrame.createOrReplaceTempView("source")
#         query=f"""merge into {targetTable} target
#                 using  source on {primaryKeyString}
#                 when matched then update set *
#                 when not matched then insert *"""

#         print(query)        
#         spark.sql(query)
#         logger.info(f"Merge with {targetTable} completed]")

#     except Exception as e:
#         raise e

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script

source_bucket=getParameterValue(raw,'wf_Store_Data','m_store_data','source_bucket')
key=getParameterValue(raw,'wf_Store_Data','m_store_data','key')

#source_file=get_src_file(key,source_bucket)
source_file = get_file_for_processing(key,source_bucket)

# COMMAND ----------

# print(source_file)

# COMMAND ----------

# Processing node SQ_Shortcut_to_STORE_DATA_FLAT1, type SOURCE 
# COLUMN COUNT: 20

SQ_Shortcut_to_STORE_DATA_FLAT1 = spark.read.csv(source_file, sep=';', header='false',inferSchema=True).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_STORE_DATA_FLAT1 = SQ_Shortcut_to_STORE_DATA_FLAT1 \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[0],'SITE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[1],'COMPANY_CD') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[2],'CURRENCY_CD') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[3],'MERCH_ID') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[4],'TAX_JURISDICTION_CD') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[5],'EARLIEST_VALUE_POST_DT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[6],'TOT_VALUE_WARNING_LMT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[7],'TOT_VALUE_ERROR_LMT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[8],'TRANS_CNT_LOWER_LMT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[9],'TRANS_CNT_WARNING_LMT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[10],'TRANS_CNT_ERROR_LMT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[11],'TRANS_VALUE_ERROR_LMT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[12],'TRANS_SEQ_GAP_CNT_ERROR_LMT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[13],'OUT_OF_BALANCE_CNT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[14],'OUT_OF_BALANCE_VALUE') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[15],'BANK_DEPOSIT_VAR_PCT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[16],'CREATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[17],'CREATE_USER') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[18],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_STORE_DATA_FLAT1.columns[19],'UPDATE_USER')

# COMMAND ----------

# SQ_Shortcut_to_STORE_DATA_FLAT1.show()

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_STORE_DATA_FLAT1_temp = SQ_Shortcut_to_STORE_DATA_FLAT1.toDF(*["SQ_Shortcut_to_STORE_DATA_FLAT1___" + col for col in SQ_Shortcut_to_STORE_DATA_FLAT1.columns])

EXPTRANS = SQ_Shortcut_to_STORE_DATA_FLAT1_temp.selectExpr(
	"SQ_Shortcut_to_STORE_DATA_FLAT1___SITE_NBR as SITE_NBR",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___COMPANY_CD as COMPANY_CD",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___CURRENCY_CD as CURRENCY_CD",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___MERCH_ID as MERCH_ID",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___TAX_JURISDICTION_CD as TAX_JURISDICTION_CD",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___EARLIEST_VALUE_POST_DT as i_EARLIEST_VALUE_POST_DT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___TOT_VALUE_WARNING_LMT as i_TOT_VALUE_WARNING_LMT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___TOT_VALUE_ERROR_LMT as i_TOT_VALUE_ERROR_LMT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___TRANS_CNT_LOWER_LMT as TRANS_CNT_LOWER_LMT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___TRANS_CNT_WARNING_LMT as TRANS_CNT_WARNING_LMT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___TRANS_CNT_ERROR_LMT as TRANS_CNT_ERROR_LMT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___TRANS_VALUE_ERROR_LMT as TRANS_VALUE_ERROR_LMT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___TRANS_SEQ_GAP_CNT_ERROR_LMT as TRANS_SEQ_GAP_CNT_ERR_LMT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___OUT_OF_BALANCE_CNT as i_OUT_OF_BALANCE_CNT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___OUT_OF_BALANCE_VALUE as i_OUT_OF_BALANCE_VALUE",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___BANK_DEPOSIT_VAR_PCT as i_BANK_DEPOSIT_VAR_PCT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___CREATE_DT as i_CREATE_DT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___CREATE_USER as CREATE_USER",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___UPDATE_DT as i_UPDATE_DT",
	"SQ_Shortcut_to_STORE_DATA_FLAT1___UPDATE_USER as UPDATE_USER").selectExpr(
	# "SQ_Shortcut_to_STORE_DATA_FLAT1___sys_row_id as sys_row_id",
	"SITE_NBR as SITE_NBR",
	"COMPANY_CD as COMPANY_CD",
	"CURRENCY_CD as CURRENCY_CD",
	"MERCH_ID as MERCH_ID",
	"TAX_JURISDICTION_CD as TAX_JURISDICTION_CD",
	"TO_DATE ( i_EARLIEST_VALUE_POST_DT , 'yyyyMMdd' ) as o_EARLIEST_VALUE_POST_DT",
	"DECIMAL(i_TOT_VALUE_WARNING_LMT) as o_TOT_VALUE_WARNING_LMT",
	"DECIMAL(i_TOT_VALUE_ERROR_LMT) as o_TOT_VALUE_ERROR_LMT",
	"TRANS_CNT_LOWER_LMT as TRANS_CNT_LOWER_LMT",
	"TRANS_CNT_WARNING_LMT as TRANS_CNT_WARNING_LMT",
	"TRANS_CNT_ERROR_LMT as TRANS_CNT_ERROR_LMT",
	"TRANS_VALUE_ERROR_LMT as TRANS_VALUE_ERROR_LMT",
	"TRANS_SEQ_GAP_CNT_ERR_LMT as TRANS_SEQ_GAP_CNT_ERR_LMT",
	"CAST(i_OUT_OF_BALANCE_CNT  AS DECIMAL(10, 2)) as o_OUT_OF_BALANCE_CNT",  #used cast function instead of Decimal
	"CAST(i_OUT_OF_BALANCE_VALUE  AS DECIMAL(10, 2)) as o_OUT_OF_BALANCE_VALUE", #used cast function instead of Decimal
	"CAST(i_BANK_DEPOSIT_VAR_PCT  AS DECIMAL(10, 2)) as o_BANK_DEPOSIT_VAR_PCT", #used cast function instead of Decimal
	"TO_DATE ( i_CREATE_DT , 'yyyyMMdd' ) as o_CREATE_DT",
	"CREATE_USER as CREATE_USER",
	"TO_DATE ( i_UPDATE_DT , 'yyyyMMdd' ) as o_UPDATE_DT",
	"UPDATE_USER as UPDATE_USER"
)

# COMMAND ----------

# Processing node Shortcut_to_STORE_DATA, type TARGET 
# COLUMN COUNT: 20


Shortcut_to_STORE_DATA = EXPTRANS.selectExpr(
	"CAST(SITE_NBR AS smallint) as SITE_NBR",
	"CAST(COMPANY_CD AS smallint) as COMPANY_CD",
	"CAST(CURRENCY_CD AS STRING) as CURRENCY_CD",
	"CAST(MERCH_ID AS STRING) as MERCH_ID",
	"CAST(TAX_JURISDICTION_CD AS STRING) as TAX_JURISDICTION_CD",
	"CAST(o_EARLIEST_VALUE_POST_DT AS TIMESTAMP) as EARLIEST_VALUE_POST_DT",
	"CAST(o_TOT_VALUE_WARNING_LMT AS DECIMAL(10, 2)) as TOT_VALUE_WARNING_LMT",
	"CAST(o_TOT_VALUE_ERROR_LMT AS DECIMAL(10, 2)) as TOT_VALUE_ERROR_LMT",
	"CAST(TRANS_CNT_LOWER_LMT AS DECIMAL(8, 0)) as TRANS_CNT_LOWER_LMT",
	"CAST(TRANS_CNT_WARNING_LMT AS DECIMAL(8, 0)) as TRANS_CNT_WARNING_LMT",
	"CAST(TRANS_CNT_ERROR_LMT AS DECIMAL(8, 0)) as TRANS_CNT_ERR_LMT",
	"CAST(TRANS_VALUE_ERROR_LMT AS DECIMAL(8, 0)) as TRANS_VALUE_ERR_LMT",
	"CAST(TRANS_SEQ_GAP_CNT_ERR_LMT AS DECIMAL(8, 0)) as TRANS_SEQ_GAP_CNT_ERR_LMT",
	"CAST(o_OUT_OF_BALANCE_CNT AS DECIMAL(10, 2)) as OUT_OF_BAL_CNT",
	"CAST(o_OUT_OF_BALANCE_VALUE AS DECIMAL(10, 2)) as OUT_OF_BAL_VALUE",
	"CAST(o_BANK_DEPOSIT_VAR_PCT AS DECIMAL(10, 2)) as BANK_DEPOSIT_VAR_PCT",
	"CAST(o_CREATE_DT AS TIMESTAMP) as CREATE_DT",
	"CAST(CREATE_USER AS STRING) as CREATE_USER",
	"CAST(o_UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(UPDATE_USER AS STRING) as UPDATE_USER"
)
# try:
#     chk=DuplicateChecker()
#     chk.check_for_duplicate_primary_keys(Shortcut_to_STORE_DATA,['SITE_NBR'])
#     Shortcut_to_STORE_DATA.write.saveAsTable(f'{legacy}.STORE_DATA', mode = 'append')
# except Exception as e:
#     raise e



# try:
# 	primary_key = """source.SITE_NBR = target.SITE_NBR """
# 	refined_perf_table = f"{legacy}.STORE_DATA"
# 	chk=DuplicateChecker()
# 	chk.check_for_duplicate_primary_keys(Shortcut_to_STORE_DATA,['SITE_NBR'])
# 	executeMergeNoAction(Shortcut_to_STORE_DATA, refined_perf_table, primary_key)
# 	logger.info(f"Merge with {refined_perf_table} completed]")
# 	logPrevRunDt("STORE_DATA", "STORE_DATA", "Completed", "N/A", f"{raw}.log_run_details")
# except Exception as e:
# 	logPrevRunDt("STORE_DATA", "STORE_DATA","Failed",str(e), f"{raw}.log_run_details", )
# 	raise e    

# COMMAND ----------


Shortcut_to_STORE_DATA.createOrReplaceTempView("temp_table")


# COMMAND ----------



# COMMAND ----------

#facing parsing eror with executemergenoaction code, replaced with Merge code
try:
    primary_key = """source.SITE_NBR = target.SITE_NBR """
    refined_perf_table = f"{legacy}.STORE_DATA"
    chk = DuplicateChecker()
    chk.check_for_duplicate_primary_keys(Shortcut_to_STORE_DATA, ['SITE_NBR'])
    
    merge_sql = f"""
        MERGE INTO {refined_perf_table} AS target
        USING temp_table AS source
        ON target.SITE_NBR = source.SITE_NBR
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    
    logger.info(f"Merge with {refined_perf_table} completed")
    logPrevRunDt("STORE_DATA", "STORE_DATA", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
    logPrevRunDt("STORE_DATA", "STORE_DATA", "Failed", str(e), f"{raw}.log_run_details")
    raise e


# COMMAND ----------


