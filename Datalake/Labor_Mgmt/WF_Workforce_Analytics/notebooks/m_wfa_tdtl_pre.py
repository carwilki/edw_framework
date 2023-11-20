#Code converted on 2023-08-08 15:42:12
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')

args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
empl_protected = getEnvPrefix(env) + 'empl_protected'

(username,password,connection_string) = or_kro_read_krap1(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_FCT_TDTL, type SOURCE 
# COLUMN COUNT: 49

SQ_Shortcut_to_FCT_TDTL = jdbcOracleConnection(f"""SELECT f.tdtl_skey, 
       f.tdtl_id, 
       f.tsitem_skey, 
       f.recorded_dat, 
       f.recorded_for_dat, 
       f.adj_swt, 
       f.edt_swt, 
       f.emp_skey, 
       f.strt_dtm, 
       f.end_dtm, 
       f.unschd_strt_dtm, 
       f.unschd_end_dtm, 
       f.strt_tzone_skey, 
       f.lbracct_skey, 
       f.hm_lbracct_swt, 
       f.hm_lbracct_skey, 
       f.from_lbracct_skey, 
       f.paycd_skey, 
       f.core_hrs_swt, 
       f.from_paycd_skey, 
       f.supv_skey, 
       f.pri_job_skey, 
       f.job_skey, 
       f.pri_org_skey, 
       f.org_skey, 
       f.payper_skey, 
       f.hday_skey, 
       f.strt_pnchevnt_skey, 
       f.end_pnchevnt_skey, 
       f.strt_dsrc_skey, 
       f.end_dsrc_skey, 
       f.dsrc_skey, 
       f.empstat_skey, 
       f.age_nbr, 
       f.tenure_mo_nbr, 
       f.dflt_pay_rule_skey, 
       f.dflt_wrk_rule_swt, 
       f.dflt_wrk_rule_skey, 
       f.wrk_rule_skey, 
       f.money_amt, 
       f.drtn_amt, 
       f.core_amt, 
       f.non_core_amt, 
       f.drtn_diff_amt, 
       f.drtn_hrs, 
       f.core_hrs, 
       f.non_core_hrs, 
       f.grp_schd_skey, 
       f.updt_dtm 
FROM   ia.fct_tdtl f 
WHERE  f.recorded_dat > CURRENT_DATE - 36""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_to_FCT_TDTL = SQ_Shortcut_to_FCT_TDTL \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[0],'TDTL_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[1],'TDTL_ID') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[2],'TSITEM_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[3],'RECORDED_DAT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[4],'RECORDED_FOR_DAT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[5],'ADJ_SWT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[6],'EDT_SWT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[7],'EMP_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[8],'STRT_DTM') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[9],'END_DTM') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[10],'UNSCHD_STRT_DTM') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[11],'UNSCHD_END_DTM') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[12],'STRT_TZONE_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[13],'LBRACCT_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[14],'HM_LBRACCT_SWT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[15],'HM_LBRACCT_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[16],'FROM_LBRACCT_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[17],'PAYCD_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[18],'CORE_HRS_SWT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[19],'FROM_PAYCD_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[20],'SUPV_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[21],'PRI_JOB_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[22],'JOB_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[23],'PRI_ORG_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[24],'ORG_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[25],'PAYPER_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[26],'HDAY_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[27],'STRT_PNCHEVNT_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[28],'END_PNCHEVNT_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[29],'STRT_DSRC_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[30],'END_DSRC_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[31],'DSRC_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[32],'EMPSTAT_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[33],'AGE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[34],'TENURE_MO_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[35],'DFLT_PAY_RULE_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[36],'DFLT_WRK_RULE_SWT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[37],'DFLT_WRK_RULE_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[38],'WRK_RULE_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[39],'MONEY_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[40],'DRTN_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[41],'CORE_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[42],'NON_CORE_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[43],'DRTN_DIFF_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[44],'DRTN_HRS') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[45],'CORE_HRS') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[46],'NON_CORE_HRS') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[47],'GRP_SCHD_SKEY') \
	.withColumnRenamed(SQ_Shortcut_to_FCT_TDTL.columns[48],'UPDT_DTM')

# COMMAND ----------
# Processing node Shortcut_to_WFA_TDTL_PRE, type TARGET 
# COLUMN COUNT: 50


Shortcut_to_WFA_TDTL_PRE = SQ_Shortcut_to_FCT_TDTL.selectExpr(
	"CAST(TDTL_SKEY AS DECIMAL(20,0)) as TDTL_SKEY",
	"CAST(TDTL_ID AS BIGINT) as TDTL_ID",
	"CAST(TSITEM_SKEY AS DECIMAL(20,0)) as TSITEM_SKEY",
	"CAST(RECORDED_DAT AS TIMESTAMP) as RECORDED_DAT",
	"CAST(RECORDED_FOR_DAT AS TIMESTAMP) as RECORDED_FOR_DAT",
	"CAST(ADJ_SWT AS INT) as ADJ_SWT",
	"CAST(EDT_SWT AS INT) as EDT_SWT",
	"CAST(EMP_SKEY AS BIGINT)as EMP_SKEY",
	"CAST(STRT_DTM AS TIMESTAMP) as STRT_DTM",
	"CAST(END_DTM AS TIMESTAMP) as END_DTM",
	"CAST(UNSCHD_STRT_DTM AS TIMESTAMP) as UNSCHD_STRT_DTM",
	"CAST(UNSCHD_END_DTM AS TIMESTAMP) as UNSCHD_END_DTM",
	"CAST(STRT_TZONE_SKEY AS BIGINT) as STRT_TZONE_SKEY",
	"CAST(LBRACCT_SKEY AS BIGINT) as LBRACCT_SKEY",
	"CAST(HM_LBRACCT_SWT AS INT) as HM_LBRACCT_SWT",
	"CAST(HM_LBRACCT_SKEY AS BIGINT) as HM_LBRACCT_SKEY",
	"CAST(FROM_LBRACCT_SKEY AS BIGINT) as FROM_LBRACCT_SKEY",
	"CAST(PAYCD_SKEY AS BIGINT) as PAYCD_SKEY",
	"CAST(CORE_HRS_SWT AS INT) as CORE_HRS_SWT",
	"CAST(FROM_PAYCD_SKEY AS BIGINT) as FROM_PAYCD_SKEY",
	"CAST(SUPV_SKEY AS BIGINT) as SUPV_SKEY",
	"CAST(PRI_JOB_SKEY AS BIGINT) as PRI_JOB_SKEY",
	"CAST(JOB_SKEY AS BIGINT) as JOB_SKEY",
	"CAST(PRI_ORG_SKEY AS BIGINT) as PRI_ORG_SKEY",
	"CAST(ORG_SKEY AS BIGINT) as ORG_SKEY",
	"CAST(PAYPER_SKEY AS BIGINT) as PAYPER_SKEY",
	"CAST(HDAY_SKEY AS BIGINT) as HDAY_SKEY",
	"CAST(STRT_PNCHEVNT_SKEY AS BIGINT) as STRT_PNCHEVNT_SKEY",
	"CAST(END_PNCHEVNT_SKEY AS BIGINT) as END_PNCHEVNT_SKEY",
	"CAST(STRT_DSRC_SKEY AS BIGINT)  as STRT_DSRC_SKEY",
	"CAST(END_DSRC_SKEY AS BIGINT) as END_DSRC_SKEY",
	"CAST(DSRC_SKEY AS BIGINT) as DSRC_SKEY",
	"CAST(EMPSTAT_SKEY AS BIGINT)  as EMPSTAT_SKEY",
	"CAST(AGE_NBR AS BIGINT) as AGE_NBR",
	"CAST(TENURE_MO_NBR AS BIGINT) as TENURE_MO_NBR",
	"CAST(DFLT_PAY_RULE_SKEY AS BIGINT) as DFLT_PAY_RULE_SKEY",
	"CAST(DFLT_WRK_RULE_SWT AS INT) as DFLT_WRK_RULE_SWT",
	"CAST(DFLT_WRK_RULE_SKEY AS BIGINT) as DFLT_WRK_RULE_SKEY",
	"CAST(WRK_RULE_SKEY AS BIGINT) as WRK_RULE_SKEY",
	"CAST(MONEY_AMT AS DECIMAL(16,6)) as MONEY_AMT",
	"CAST(DRTN_AMT AS DECIMAL(16,6)) as DRTN_AMT",
	"CAST(CORE_AMT AS DECIMAL(16,6)) as CORE_AMT",
	"CAST(NON_CORE_AMT AS DECIMAL(16,6)) as NON_CORE_AMT",
	"CAST(DRTN_DIFF_AMT AS DECIMAL(16,6)) as DRTN_DIFF_AMT",
	"CAST(DRTN_HRS AS DECIMAL(16,6)) as DRTN_HRS",
	"CAST(CORE_HRS AS DECIMAL(16,6)) as CORE_HRS",
	"CAST(NON_CORE_HRS AS DECIMAL(16,6)) as NON_CORE_HRS",
	"CAST(null AS INT) as LOCKED_SWT",
	"CAST(GRP_SCHD_SKEY AS BIGINT) as GRP_SCHD_SKEY",
	"CAST(UPDT_DTM AS TIMESTAMP) as UPDT_DTM"
)
# overwriteDeltaPartition(Shortcut_to_WFA_TDTL_PRE,'DC_NBR',dcnbr,f'{raw}.WFA_TDTL_PRE')
Shortcut_to_WFA_TDTL_PRE.write.mode("overwrite").saveAsTable(f'{empl_protected}.raw_WFA_TDTL_PRE')