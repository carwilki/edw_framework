#Code converted on 2023-08-08 15:42:05
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


# DC_NBR = 'dc38' #args.DC_NBR
# (username,password,connection_string) = getConfig(DC_NBR, env)
(username,password,connection_string) = or_kro_read_krap1(env)

# COMMAND ----------
# Processing node SQ_Shortcut_to_DIM_EMP, type SOURCE 
# COLUMN COUNT: 20

SQ_Shortcut_to_DIM_EMP = jdbcOracleConnection(f"""SELECT
DIM_EMP.EMP_ID,
DIM_EMP.EMP_SKEY,
DIM_EMP.CO_HIRE_DAT,
DIM_EMP.MGR_SIGNOFF_DAT,
DIM_EMP.PRSN_NBR_TXT,
DIM_EMP.SR_RANK_DAT,
DIM_EMP.DFLT_TZONE_SKEY,
DIM_EMP.DFLT_TZONE_DES,
DIM_EMP.DVC_GRP_NAM,
DIM_EMP.HM_LBRACCT_SKEY,
DIM_EMP.EMP_HM_LBRACCT_EFF_DAT,
DIM_EMP.EMP_HM_LBRACCT_EXP_DAT,
DIM_EMP.EMPSTAT_SKEY,
DIM_EMP.EMP_EMPSTAT_EFF_DAT,
DIM_EMP.EMP_EMPSTAT_EXP_DAT,
DIM_EMP.BADGE_COD,
DIM_EMP.EMP_BADGE_EFF_DAT,
DIM_EMP.EMP_BADGE_EXP_DAT,
DIM_EMP.EM_MNR_SWT,
DIM_EMP.UPDT_DTM
FROM ia.DIM_EMP
WHERE DIM_EMP.EMP_ID > 0""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node FIL_IS_NUMBER, type FILTER 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_DIM_EMP_temp = SQ_Shortcut_to_DIM_EMP.toDF(*["SQ_Shortcut_to_DIM_EMP___" + col for col in SQ_Shortcut_to_DIM_EMP.columns])

FIL_IS_NUMBER = SQ_Shortcut_to_DIM_EMP_temp.selectExpr(
	"SQ_Shortcut_to_DIM_EMP___EMP_ID as EMP_ID",
	"SQ_Shortcut_to_DIM_EMP___EMP_SKEY as EMP_SKEY",
	"SQ_Shortcut_to_DIM_EMP___CO_HIRE_DAT as CO_HIRE_DAT",
	"SQ_Shortcut_to_DIM_EMP___MGR_SIGNOFF_DAT as MGR_SIGNOFF_DAT",
	"SQ_Shortcut_to_DIM_EMP___PRSN_NBR_TXT as PRSN_NBR_TXT",
	"SQ_Shortcut_to_DIM_EMP___SR_RANK_DAT as SR_RANK_DAT",
	"SQ_Shortcut_to_DIM_EMP___DFLT_TZONE_SKEY as DFLT_TZONE_SKEY",
	"SQ_Shortcut_to_DIM_EMP___DFLT_TZONE_DES as DFLT_TZONE_DES",
	"SQ_Shortcut_to_DIM_EMP___DVC_GRP_NAM as DVC_GRP_NAM",
	"SQ_Shortcut_to_DIM_EMP___HM_LBRACCT_SKEY as HM_LBRACCT_SKEY",
	"SQ_Shortcut_to_DIM_EMP___EMP_HM_LBRACCT_EFF_DAT as EMP_HM_LBRACCT_EFF_DAT",
	"SQ_Shortcut_to_DIM_EMP___EMP_HM_LBRACCT_EXP_DAT as EMP_HM_LBRACCT_EXP_DAT",
	"SQ_Shortcut_to_DIM_EMP___EMPSTAT_SKEY as EMPSTAT_SKEY",
	"SQ_Shortcut_to_DIM_EMP___EMP_EMPSTAT_EFF_DAT as EMP_EMPSTAT_EFF_DAT",
	"SQ_Shortcut_to_DIM_EMP___EMP_EMPSTAT_EXP_DAT as EMP_EMPSTAT_EXP_DAT",
	"SQ_Shortcut_to_DIM_EMP___BADGE_COD as BADGE_COD",
	"SQ_Shortcut_to_DIM_EMP___EMP_BADGE_EFF_DAT as EMP_BADGE_EFF_DAT",
	"SQ_Shortcut_to_DIM_EMP___EMP_BADGE_EXP_DAT as EMP_BADGE_EXP_DAT",
	"SQ_Shortcut_to_DIM_EMP___EM_MNR_SWT as EM_MNR_SWT",
	"SQ_Shortcut_to_DIM_EMP___UPDT_DTM as UPDT_DTM").filter("length(translate(prsn_nbr_txt,'0123456789', '' )) = 0").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_WFA_EMP_PRE, type TARGET 
# COLUMN COUNT: 20


Shortcut_to_WFA_EMP_PRE = FIL_IS_NUMBER.selectExpr(
	"CAST(EMP_ID AS BIGINT) as EMP_ID",
	"CAST(EMP_SKEY AS BIGINT) as EMP_SKEY",
	"CAST(CO_HIRE_DAT AS TIMESTAMP) as CO_HIRE_DAT",
	"CAST(MGR_SIGNOFF_DAT AS TIMESTAMP) as MGR_SIGNOFF_DAT",
	"PRSN_NBR_TXT as PRSN_NBR_TXT",
	"CAST(SR_RANK_DAT AS TIMESTAMP) as SR_RANK_DAT",
	"CAST(DFLT_TZONE_SKEY AS BIGINT) as DFLT_TZONE_SKEY",
	"DFLT_TZONE_DES as DFLT_TZONE_DES",
	"DVC_GRP_NAM as DVC_GRP_NAM",
	"CAST(HM_LBRACCT_SKEY AS BIGINT) as HM_LBRACCT_SKEY",
	"CAST(EMP_HM_LBRACCT_EFF_DAT AS TIMESTAMP) as EMP_HM_LBRACCT_EFF_DAT",
	"CAST(EMP_HM_LBRACCT_EXP_DAT AS TIMESTAMP) as EMP_HM_LBRACCT_EXP_DAT",
	"CAST(EMPSTAT_SKEY AS BIGINT) as EMPSTAT_SKEY",
	"CAST(EMP_EMPSTAT_EFF_DAT AS TIMESTAMP) as EMP_EMPSTAT_EFF_DAT",
	"CAST(EMP_EMPSTAT_EXP_DAT AS TIMESTAMP) as EMP_EMPSTAT_EXP_DAT",
	"BADGE_COD as BADGE_COD",
	"CAST(EMP_BADGE_EFF_DAT AS TIMESTAMP) as EMP_BADGE_EFF_DAT",
	"CAST(EMP_BADGE_EXP_DAT AS TIMESTAMP) as EMP_BADGE_EXP_DAT",
	"CAST(EM_MNR_SWT AS INT) as EM_MNR_SWT",
	"CAST(UPDT_DTM AS TIMESTAMP) as UPDT_DTM"
)
# overwriteDeltaPartition(Shortcut_to_WFA_EMP_PRE,'DC_NBR',dcnbr,f'{raw}.WFA_EMP_PRE')
Shortcut_to_WFA_EMP_PRE.write.mode("overwrite").saveAsTable(f'{raw}.WFA_EMP_PRE')