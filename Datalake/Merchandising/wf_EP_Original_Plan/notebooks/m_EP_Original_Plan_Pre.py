# Databricks notebook source
#Code converted on 2023-10-24 13:16:13
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
(username, password, connection_string) = or_enp_read(env)


# COMMAND ----------

# Processing node SQ_Shortcut_to_PF_FT_PL_OP1_000, type SOURCE 
# COLUMN COUNT: 49

SQ_Shortcut_to_PF_FT_PL_OP1_000 = jdbcOracleConnection(f"""SELECT
PRODUCT_KEY,
ORGANIZATION_KEY,
CALENDAR_KEY,
ADJ_UNT_OP,
ADJ_VAL_OP,
BFRX_VAL_OP,
BOP_UNT_OP,
BOP_VAL_OP,
CLRFUND_VAL_OP,
COMBSLS_RTL_OP,
COMPSLS_RTL_OP,
COMPSLS_UNT_OP,
DCCOSTS_VAL_OP,
DEFECT_VAL_OP,
CD_VAL_OP,
EOPNORM_UNT_OP,
EOPNORM_VAL_OP,
EOP_UNT_OP,
EOP_VAL_OP,
MDCLR_VAL_OP,
MDPRO_VAL_OP,
NCOMSLS_RTL_OP,
OBSO_UNT_OP,
OBSO_VAL_OP,
OTHADJ_VAL_OP,
OUTFRT_VAL_OP,
PROFUND_VAL_OP,
REC_UNT_OP,
REC_VAL_OP,
REMOVAL_UNT_OP,
REMOVAL_VAL_OP,
ROYS_VAL_OP,
RSLSCLR_VAL_OP,
RSLSPRO_VAL_OP,
SHRINK_UNT_OP,
SHRINK_VAL_OP,
TRTLSLS_VAL_OP,
TTLSLS_UNT_OP,
TTLSLS_VAL_OP,
UPDATE_DATE,
DAMAGES_UNT_OP,
DAMAGES_VAL_OP,
SERVADJ_VAL_OP,
SERVEXP_VAL_OP,
COUNTBPRD_OP,
DUMMYSTR_OP,
COMPGM_VAL_OP,
PF_DM_PROD_CLASS.MEMB_NAME,
PF_DM_TIME_WEEK.MEMB_NAME as MEMB_NAME1 
FROM EKBF.PF_DM_TIME_WEEK, EKBF.PF_FT_PL_OP1_000, EKBF.PF_DM_PROD_CLASS
WHERE PF_FT_PL_OP1_000.PRODUCT_KEY=PF_DM_PROD_CLASS.MEMB_KEY

AND

PF_FT_PL_OP1_000.CALENDAR_KEY=PF_DM_TIME_WEEK.MEMB_KEY""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_PF_FT_PL_OP1_000 = SQ_Shortcut_to_PF_FT_PL_OP1_000 \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[0],'PRODUCT_KEY') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[1],'ORGANIZATION_KEY') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[2],'CALENDAR_KEY') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[3],'ADJ_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[4],'ADJ_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[5],'BFRX_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[6],'BOP_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[7],'BOP_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[8],'CLRFUND_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[9],'COMBSLS_RTL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[10],'COMPSLS_RTL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[11],'COMPSLS_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[12],'DCCOSTS_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[13],'DEFECT_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[14],'CD_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[15],'EOPNORM_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[16],'EOPNORM_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[17],'EOP_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[18],'EOP_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[19],'MDCLR_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[20],'MDPRO_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[21],'NCOMSLS_RTL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[22],'OBSO_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[23],'OBSO_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[24],'OTHADJ_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[25],'OUTFRT_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[26],'PROFUND_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[27],'REC_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[28],'REC_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[29],'REMOVAL_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[30],'REMOVAL_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[31],'ROYS_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[32],'RSLSCLR_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[33],'RSLSPRO_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[34],'SHRINK_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[35],'SHRINK_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[36],'TRTLSLS_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[37],'TTLSLS_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[38],'TTLSLS_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[39],'UPDATE_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[40],'DAMAGES_UNT_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[41],'DAMAGES_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[42],'SERVADJ_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[43],'SERVEXP_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[44],'COUNTBPRD_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[45],'DUMMYSTR_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[46],'COMPGM_VAL_OP') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[47],'MEMB_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_PF_FT_PL_OP1_000.columns[48],'MEMB_NAME1')

# COMMAND ----------

# SQ_Shortcut_to_PF_FT_PL_OP1_000.show()

# COMMAND ----------

# Processing node EXP_LOAD_TSTMP, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 50

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_PF_FT_PL_OP1_000_temp = SQ_Shortcut_to_PF_FT_PL_OP1_000.toDF(*["SQ_Shortcut_to_PF_FT_PL_OP1_000___" + col for col in SQ_Shortcut_to_PF_FT_PL_OP1_000.columns])

EXP_LOAD_TSTMP = SQ_Shortcut_to_PF_FT_PL_OP1_000_temp.selectExpr(
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___PRODUCT_KEY as PRODUCT_KEY",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___ORGANIZATION_KEY as ORGANIZATION_KEY",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___CALENDAR_KEY as CALENDAR_KEY",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___ADJ_UNT_OP as ADJ_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___ADJ_VAL_OP as ADJ_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___BFRX_VAL_OP as BFRX_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___BOP_UNT_OP as BOP_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___BOP_VAL_OP as BOP_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___CLRFUND_VAL_OP as CLRFUND_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___COMBSLS_RTL_OP as COMBSLS_RTL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___COMPSLS_RTL_OP as COMPSLS_RTL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___COMPSLS_UNT_OP as COMPSLS_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___DCCOSTS_VAL_OP as DCCOSTS_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___DEFECT_VAL_OP as DEFECT_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___CD_VAL_OP as CD_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___EOPNORM_UNT_OP as EOPNORM_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___EOPNORM_VAL_OP as EOPNORM_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___EOP_UNT_OP as EOP_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___EOP_VAL_OP as EOP_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___MDCLR_VAL_OP as MDCLR_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___MDPRO_VAL_OP as MDPRO_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___NCOMSLS_RTL_OP as NCOMSLS_RTL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___OBSO_UNT_OP as OBSO_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___OBSO_VAL_OP as OBSO_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___OTHADJ_VAL_OP as OTHADJ_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___OUTFRT_VAL_OP as OUTFRT_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___PROFUND_VAL_OP as PROFUND_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___REC_UNT_OP as REC_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___REC_VAL_OP as REC_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___REMOVAL_UNT_OP as REMOVAL_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___REMOVAL_VAL_OP as REMOVAL_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___ROYS_VAL_OP as ROYS_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___RSLSCLR_VAL_OP as RSLSCLR_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___RSLSPRO_VAL_OP as RSLSPRO_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___SHRINK_UNT_OP as SHRINK_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___SHRINK_VAL_OP as SHRINK_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___TRTLSLS_VAL_OP as TRTLSLS_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___TTLSLS_UNT_OP as TTLSLS_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___TTLSLS_VAL_OP as TTLSLS_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___UPDATE_DATE as UPDATE_DATE",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___DAMAGES_UNT_OP as DAMAGES_UNT_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___DAMAGES_VAL_OP as DAMAGES_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___SERVADJ_VAL_OP as SERVADJ_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___SERVEXP_VAL_OP as SERVEXP_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___COUNTBPRD_OP as COUNTBPRD_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___DUMMYSTR_OP as DUMMYSTR_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___COMPGM_VAL_OP as COMPGM_VAL_OP",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___MEMB_NAME as CLS_MEMB_NAME",
	"SQ_Shortcut_to_PF_FT_PL_OP1_000___MEMB_NAME1 as WK_MEMB_NAME").selectExpr(
	# "SQ_Shortcut_to_PF_FT_PL_OP1_000___sys_row_id as sys_row_id",
	"PRODUCT_KEY as PRODUCT_KEY",
	"ORGANIZATION_KEY as ORGANIZATION_KEY",
	"CALENDAR_KEY as CALENDAR_KEY",
	"ADJ_UNT_OP as ADJ_UNT_OP",
	"ADJ_VAL_OP as ADJ_VAL_OP",
	"BFRX_VAL_OP as BFRX_VAL_OP",
	"BOP_UNT_OP as BOP_UNT_OP",
	"BOP_VAL_OP as BOP_VAL_OP",
	"CLRFUND_VAL_OP as CLRFUND_VAL_OP",
	"COMBSLS_RTL_OP as COMBSLS_RTL_OP",
	"COMPSLS_RTL_OP as COMPSLS_RTL_OP",
	"COMPSLS_UNT_OP as COMPSLS_UNT_OP",
	"DCCOSTS_VAL_OP as DCCOSTS_VAL_OP",
	"DEFECT_VAL_OP as DEFECT_VAL_OP",
	"CD_VAL_OP as CD_VAL_OP",
	"EOPNORM_UNT_OP as EOPNORM_UNT_OP",
	"EOPNORM_VAL_OP as EOPNORM_VAL_OP",
	"EOP_UNT_OP as EOP_UNT_OP",
	"EOP_VAL_OP as EOP_VAL_OP",
	"MDCLR_VAL_OP as MDCLR_VAL_OP",
	"MDPRO_VAL_OP as MDPRO_VAL_OP",
	"NCOMSLS_RTL_OP as NCOMSLS_RTL_OP",
	"OBSO_UNT_OP as OBSO_UNT_OP",
	"OBSO_VAL_OP as OBSO_VAL_OP",
	"OTHADJ_VAL_OP as OTHADJ_VAL_OP",
	"OUTFRT_VAL_OP as OUTFRT_VAL_OP",
	"PROFUND_VAL_OP as PROFUND_VAL_OP",
	"REC_UNT_OP as REC_UNT_OP",
	"REC_VAL_OP as REC_VAL_OP",
	"REMOVAL_UNT_OP as REMOVAL_UNT_OP",
	"REMOVAL_VAL_OP as REMOVAL_VAL_OP",
	"ROYS_VAL_OP as ROYS_VAL_OP",
	"RSLSCLR_VAL_OP as RSLSCLR_VAL_OP",
	"RSLSPRO_VAL_OP as RSLSPRO_VAL_OP",
	"SHRINK_UNT_OP as SHRINK_UNT_OP",
	"SHRINK_VAL_OP as SHRINK_VAL_OP",
	"TRTLSLS_VAL_OP as TRTLSLS_VAL_OP",
	"TTLSLS_UNT_OP as TTLSLS_UNT_OP",
	"TTLSLS_VAL_OP as TTLSLS_VAL_OP",
	"UPDATE_DATE as UPDATE_DATE",
	"DAMAGES_UNT_OP as DAMAGES_UNT_OP",
	"DAMAGES_VAL_OP as DAMAGES_VAL_OP",
	"SERVADJ_VAL_OP as SERVADJ_VAL_OP",
	"SERVEXP_VAL_OP as SERVEXP_VAL_OP",
	"COUNTBPRD_OP as COUNTBPRD_OP",
	"DUMMYSTR_OP as DUMMYSTR_OP",
	"COMPGM_VAL_OP as COMPGM_VAL_OP",
	"CLS_MEMB_NAME as CLS_MEMB_NAME",
	"WK_MEMB_NAME as WK_MEMB_NAME",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_EP_PF_FT_PL_OP1_000_PRE, type TARGET 
# COLUMN COUNT: 50


Shortcut_to_EP_PF_FT_PL_OP1_000_PRE = EXP_LOAD_TSTMP.selectExpr(
	"CAST(PRODUCT_KEY AS INT) as PRODUCT_KEY",
	"CAST(ORGANIZATION_KEY AS INT) as ORGANIZATION_KEY",
	"CAST(CALENDAR_KEY AS INT) as CALENDAR_KEY",
	"CAST(CLS_MEMB_NAME AS STRING) as PRODUCT_NAME",
	"CAST(WK_MEMB_NAME AS STRING) as CALENDAR_NAME",
	"CAST(ADJ_UNT_OP AS DECIMAL(24,12)) as ADJ_UNT_OP",
	"CAST(ADJ_VAL_OP AS DECIMAL(24,12)) as ADJ_VAL_OP",
	"CAST(BFRX_VAL_OP AS DECIMAL(24,12)) as BFRX_VAL_OP",
	"CAST(BOP_UNT_OP AS DECIMAL(24,12)) as BOP_UNT_OP",
	"CAST(BOP_VAL_OP AS DECIMAL(24,12)) as BOP_VAL_OP",
	"CAST(CLRFUND_VAL_OP AS DECIMAL(24,12)) as CLRFUND_VAL_OP",
	"CAST(COMBSLS_RTL_OP AS DECIMAL(24,12)) as COMBSLS_RTL_OP",
	"CAST(COMPSLS_RTL_OP AS DECIMAL(24,12)) as COMPSLS_RTL_OP",
	"CAST(COMPSLS_UNT_OP AS DECIMAL(24,12)) as COMPSLS_UNT_OP",
	"CAST(DCCOSTS_VAL_OP AS DECIMAL(24,12)) as DCCOSTS_VAL_OP",
	"CAST(DEFECT_VAL_OP AS DECIMAL(24,12)) as DEFECT_VAL_OP",
	"CAST(CD_VAL_OP AS DECIMAL(24,12)) as CD_VAL_OP",
	"CAST(EOPNORM_UNT_OP AS DECIMAL(24,12)) as EOPNORM_UNT_OP",
	"CAST(EOPNORM_VAL_OP AS DECIMAL(24,12)) as EOPNORM_VAL_OP",
	"CAST(EOP_UNT_OP AS DECIMAL(24,12)) as EOP_UNT_OP",
	"CAST(EOP_VAL_OP AS DECIMAL(24,12)) as EOP_VAL_OP",
	"CAST(MDCLR_VAL_OP AS DECIMAL(24,12)) as MDCLR_VAL_OP",
	"CAST(MDPRO_VAL_OP AS DECIMAL(24,12)) as MDPRO_VAL_OP",
	"CAST(NCOMSLS_RTL_OP AS DECIMAL(24,12)) as NCOMSLS_RTL_OP",
	"CAST(OBSO_UNT_OP AS DECIMAL(24,12)) as OBSO_UNT_OP",
	"CAST(OBSO_VAL_OP AS DECIMAL(24,12)) as OBSO_VAL_OP",
	"CAST(OTHADJ_VAL_OP AS DECIMAL(24,12)) as OTHADJ_VAL_OP",
	"CAST(OUTFRT_VAL_OP AS DECIMAL(24,12)) as OUTFRT_VAL_OP",
	"CAST(PROFUND_VAL_OP AS DECIMAL(24,12)) as PROFUND_VAL_OP",
	"CAST(REC_UNT_OP AS DECIMAL(24,12)) as REC_UNT_OP",
	"CAST(REC_VAL_OP AS DECIMAL(24,12)) as REC_VAL_OP",
	"CAST(REMOVAL_UNT_OP AS DECIMAL(24,12)) as REMOVAL_UNT_OP",
	"CAST(REMOVAL_VAL_OP AS DECIMAL(24,12)) as REMOVAL_VAL_OP",
	"CAST(ROYS_VAL_OP AS DECIMAL(24,12)) as ROYS_VAL_OP",
	"CAST(RSLSCLR_VAL_OP AS DECIMAL(24,12)) as RSLSCLR_VAL_OP",
	"CAST(RSLSPRO_VAL_OP AS DECIMAL(24,12)) as RSLSPRO_VAL_OP",
	"CAST(SHRINK_UNT_OP AS DECIMAL(24,12)) as SHRINK_UNT_OP",
	"CAST(SHRINK_VAL_OP AS DECIMAL(24,12)) as SHRINK_VAL_OP",
	"CAST(TRTLSLS_VAL_OP AS DECIMAL(24,12)) as TRTLSLS_VAL_OP",
	"CAST(TTLSLS_UNT_OP AS DECIMAL(24,12)) as TTLSLS_UNT_OP",
	"CAST(TTLSLS_VAL_OP AS DECIMAL(24,12)) as TTLSLS_VAL_OP",
	"CAST(DAMAGES_UNT_OP AS DECIMAL(24,12)) as DAMAGES_UNT_OP",
	"CAST(DAMAGES_VAL_OP AS DECIMAL(24,12)) as DAMAGES_VAL_OP",
	"CAST(SERVADJ_VAL_OP AS DECIMAL(24,12)) as SERVADJ_VAL_OP",
	"CAST(SERVEXP_VAL_OP AS DECIMAL(24,12)) as SERVEXP_VAL_OP",
	"CAST(COUNTBPRD_OP AS DECIMAL(24,12)) as COUNTBPRD_OP",
	"CAST(DUMMYSTR_OP AS DECIMAL(24,12)) as DUMMYSTR_OP",
	"CAST(COMPGM_VAL_OP AS DECIMAL(24,12)) as COMPGM_VAL_OP",
	"CAST(UPDATE_DATE AS TIMESTAMP) as UPDATE_DATE",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
try:
    chk=DuplicateChecker()
    chk.check_for_duplicate_primary_keys(Shortcut_to_EP_PF_FT_PL_OP1_000_PRE,["PRODUCT_KEY","ORGANIZATION_KEY","CALENDAR_KEY"])
    Shortcut_to_EP_PF_FT_PL_OP1_000_PRE.write.saveAsTable(f'{raw}.EP_PF_FT_PL_OP1_000_PRE', mode = 'overwrite')
except Exception as e:
    raise e

# COMMAND ----------

# def or_enp_read(env):
#     if env.lower() == "dev" or env.lower() == "qa":
#         username = "SVC_BD_ORA_NP_READ"
#         hostname = "172.17.89.238"
#         portnumber = "1800"
#         db = "enpp1"
#         password = secrets.get(scope="SVC_BD_ORA_NP_READ", key=f"temp_enpp1_password")
#         connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

#         return (username, password, connection_string)


# COMMAND ----------


