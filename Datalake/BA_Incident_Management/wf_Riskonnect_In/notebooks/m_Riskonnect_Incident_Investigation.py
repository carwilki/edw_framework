# Databricks notebook source
#Code converted on 2023-08-24 12:12:51
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

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE, type SOURCE 
# COLUMN COUNT: 69

SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE = spark.sql(f"""SELECT 
CLAIM_NUMBER,
ACTION_PLAN_DEADLINE_1,
ACTION_PLAN_DEADLINE_2,
ACTION_PLAN_DEADLINE_3,
ACTION_PLAN_OWNER_1,
ACTION_PLAN_OWNER_2,
ACTION_PLAN_OWNER_3,
ACTION_PLAN_OWNER_JOB_TITLE_1,
ACTION_PLAN_OWNER_JOB_TITLE_2,
ACTION_PLAN_OWNER_JOB_TITLE_3,
ACTION_PLAN_OWNER_PHONE_1,
ACTION_PLAN_OWNER_PHONE_2,
ACTION_PLAN_OWNER_PHONE_3,
ACTION_PLAN_STATUS_1,
ACTION_PLAN_STATUS_2,
ACTION_PLAN_STATUS_3,
ADDITIONAL_CONTRIBUTING_CONDITIONS,
ASSOCIATE_S_SHIFT_END,
DATE_OF_ASSOCIATE_RE_TRAINING,
DATE_OF_COMPLETION_1,
DATE_OF_COMPLETION_2,
DATE_OF_COMPLETION_3,
DATE_OF_TEAM_RE_TRAINING,
DESCRIBE_UNSAFE_CONDITION,
DESCRIPTION_DEVIATION,
DESCRIPTION_EQUIPMENT,
DESCRIPTION_IF_FUNCTIONING_IMPROPERLY,
DESCRIPTION_MACHIN_TOOL_SUBSTANCE_OBJECT,
DESCRIPTION_OF_CORRECTIVE_ACTION_1,
DESCRIPTION_OF_CORRECTIVE_ACTION_2,
DESCRIPTION_OF_CORRECTIVE_ACTION_3,
DESCRIPTION_OF_INCIDENT,
DESCRIPTION_UNSAFE_ACT,
DEVIATION_FROM_CURRENT_POLICY_PROCEDURE,
DID_ASSOCIATE_GO_TO_HOSPITAL,
EMPLOYEE_JOB_TITLE,
EQUIP_FUNCTIONING_PROPERLY,
HOP_COMPLETED_BY,
HOW_INCIDENT_COULD_HAVE_BEEN_PREVENTED,
HOW_WILL_STORE_LEADERSHIP_SUPPORT,
LEADER_COMPLETING,
LEADER_JOB_TITLE,
MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED,
PERSONAL_PROTECTIVE_EQUIP_REQUIRED,
PET_INCIDENT_CASE_NUMBER,
PRIOR_INCIDENTS,
RESPONSIBLE_ASSOCIATE_ID,
SERVICES_LEADER,
SERVICES_LEADER_PRESENT,
SERVICES_LEADER_SIGN_OFF,
SIGN_OFF_DATE_SERVICES_LEADER,
SIGN_OFF_DATE_STORE_LEADER,
SIMILAR_INCIDENTS_LAST_12_MONTHS,
STORE_LEADER_SIGN_OFF,
TENURE_IN_CURRENT_ROLE,
UNSAFE_ACT_CONTRIBUTED,
UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT,
UNSAFE_CONDITION_CONTRIBUTED,
WAS_PERSONAL_PROTECTIVE_EQUIP_WORN,
WITNESS_1_FIRST_NAME,
WITNESS_1_JOB_TITLE,
WITNESS_1_LAST_NAME,
WITNESS_2_FIRST_NAME,
WITNESS_2_JOB_TITLE,
WITNESS_2_LAST_NAME,
WITNESS_3_FIRST_NAME,
WITNESS_3_JOB_TITLE,
WITNESS_3_LAST_NAME,
LOAD_TSTMP
FROM
(SELECT
*,
row_number() over (partition by claim_number order by ACTION_PLAN_DEADLINE_1 DESC, ACTION_PLAN_DEADLINE_2 DESC, ACTION_PLAN_DEADLINE_3 DESC) as rn
FROM {raw}.INCIDENT_INVESTIGATIONS_PRE
) a
WHERE rn=1""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_INCIDENT_INVESTIGATIONS, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_INCIDENT_INVESTIGATIONS = spark.sql(f"""SELECT
SRC_CD,
CLAIM_NBR,
LOAD_TSTMP
FROM {legacy}.INCIDENT_INVESTIGATIONS
WHERE SRC_CD='RSKC'""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Jnr_inc_inv, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 72

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE_temp = SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE.toDF(*["SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___" + col for col in SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE.columns])
SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_temp = SQ_Shortcut_to_INCIDENT_INVESTIGATIONS.toDF(*["SQ_Shortcut_to_INCIDENT_INVESTIGATIONS___" + col for col in SQ_Shortcut_to_INCIDENT_INVESTIGATIONS.columns])

Jnr_inc_inv = SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE_temp.join(SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_temp,[SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE_temp.SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___CLAIM_NUMBER == SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_temp.SQ_Shortcut_to_INCIDENT_INVESTIGATIONS___CLAIM_NBR],'left_outer').selectExpr(
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___CLAIM_NUMBER as CLAIM_NUMBER",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_DEADLINE_1 as ACTION_PLAN_DEADLINE_1",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_DEADLINE_2 as ACTION_PLAN_DEADLINE_2",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_DEADLINE_3 as ACTION_PLAN_DEADLINE_3",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_OWNER_1 as ACTION_PLAN_OWNER_1",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_OWNER_2 as ACTION_PLAN_OWNER_2",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_OWNER_3 as ACTION_PLAN_OWNER_3",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_OWNER_JOB_TITLE_1 as ACTION_PLAN_OWNER_JOB_TITLE_1",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_OWNER_JOB_TITLE_2 as ACTION_PLAN_OWNER_JOB_TITLE_2",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_OWNER_JOB_TITLE_3 as ACTION_PLAN_OWNER_JOB_TITLE_3",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_OWNER_PHONE_1 as ACTION_PLAN_OWNER_PHONE_1",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_OWNER_PHONE_2 as ACTION_PLAN_OWNER_PHONE_2",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_OWNER_PHONE_3 as ACTION_PLAN_OWNER_PHONE_3",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_STATUS_1 as ACTION_PLAN_STATUS_1",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_STATUS_2 as ACTION_PLAN_STATUS_2",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ACTION_PLAN_STATUS_3 as ACTION_PLAN_STATUS_3",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ADDITIONAL_CONTRIBUTING_CONDITIONS as ADDITIONAL_CONTRIBUTING_CONDITIONS",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___ASSOCIATE_S_SHIFT_END as ASSOCIATE_S_SHIFT_END",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DATE_OF_ASSOCIATE_RE_TRAINING as DATE_OF_ASSOCIATE_RE_TRAINING",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DATE_OF_COMPLETION_1 as DATE_OF_COMPLETION_1",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DATE_OF_COMPLETION_2 as DATE_OF_COMPLETION_2",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DATE_OF_COMPLETION_3 as DATE_OF_COMPLETION_3",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DATE_OF_TEAM_RE_TRAINING as DATE_OF_TEAM_RE_TRAINING",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIBE_UNSAFE_CONDITION as DESCRIBE_UNSAFE_CONDITION",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIPTION_DEVIATION as DESCRIPTION_DEVIATION",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIPTION_EQUIPMENT as DESCRIPTION_EQUIPMENT",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIPTION_IF_FUNCTIONING_IMPROPERLY as DESCRIPTION_IF_FUNCTIONING_IMPROPERLY",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIPTION_MACHIN_TOOL_SUBSTANCE_OBJECT as DESCRIPTION_MACHIN_TOOL_SUBSTANCE_OBJECT",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIPTION_OF_CORRECTIVE_ACTION_1 as DESCRIPTION_OF_CORRECTIVE_ACTION_1",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIPTION_OF_CORRECTIVE_ACTION_2 as DESCRIPTION_OF_CORRECTIVE_ACTION_2",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIPTION_OF_CORRECTIVE_ACTION_3 as DESCRIPTION_OF_CORRECTIVE_ACTION_3",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIPTION_OF_INCIDENT as DESCRIPTION_OF_INCIDENT",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DESCRIPTION_UNSAFE_ACT as DESCRIPTION_UNSAFE_ACT",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DEVIATION_FROM_CURRENT_POLICY_PROCEDURE as DEVIATION_FROM_CURRENT_POLICY_PROCEDURE",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___DID_ASSOCIATE_GO_TO_HOSPITAL as DID_ASSOCIATE_GO_TO_HOSPITAL",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___EMPLOYEE_JOB_TITLE as EMPLOYEE_JOB_TITLE",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___EQUIP_FUNCTIONING_PROPERLY as EQUIP_FUNCTIONING_PROPERLY",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___HOP_COMPLETED_BY as HOP_COMPLETED_BY",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___HOW_INCIDENT_COULD_HAVE_BEEN_PREVENTED as HOW_INCIDENT_COULD_HAVE_BEEN_PREVENTED",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___HOW_WILL_STORE_LEADERSHIP_SUPPORT as HOW_WILL_STORE_LEADERSHIP_SUPPORT",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___LEADER_COMPLETING as LEADER_COMPLETING",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___LEADER_JOB_TITLE as LEADER_JOB_TITLE",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED as MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___PERSONAL_PROTECTIVE_EQUIP_REQUIRED as PERSONAL_PROTECTIVE_EQUIP_REQUIRED",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___PET_INCIDENT_CASE_NUMBER as PET_INCIDENT_CASE_NUMBER",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___PRIOR_INCIDENTS as PRIOR_INCIDENTS",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___RESPONSIBLE_ASSOCIATE_ID as RESPONSIBLE_ASSOCIATE_ID",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___SERVICES_LEADER as SERVICES_LEADER",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___SERVICES_LEADER_PRESENT as SERVICES_LEADER_PRESENT",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___SERVICES_LEADER_SIGN_OFF as SERVICES_LEADER_SIGN_OFF",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___SIGN_OFF_DATE_SERVICES_LEADER as SIGN_OFF_DATE_SERVICES_LEADER",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___SIGN_OFF_DATE_STORE_LEADER as SIGN_OFF_DATE_STORE_LEADER",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___SIMILAR_INCIDENTS_LAST_12_MONTHS as SIMILAR_INCIDENTS_LAST_12_MONTHS",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___STORE_LEADER_SIGN_OFF as STORE_LEADER_SIGN_OFF",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___TENURE_IN_CURRENT_ROLE as TENURE_IN_CURRENT_ROLE",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___UNSAFE_ACT_CONTRIBUTED as UNSAFE_ACT_CONTRIBUTED",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT as UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___UNSAFE_CONDITION_CONTRIBUTED as UNSAFE_CONDITION_CONTRIBUTED",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WAS_PERSONAL_PROTECTIVE_EQUIP_WORN as WAS_PERSONAL_PROTECTIVE_EQUIP_WORN",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WITNESS_1_FIRST_NAME as WITNESS_1_FIRST_NAME",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WITNESS_1_JOB_TITLE as WITNESS_1_JOB_TITLE",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WITNESS_1_LAST_NAME as WITNESS_1_LAST_NAME",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WITNESS_2_FIRST_NAME as WITNESS_2_FIRST_NAME",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WITNESS_2_JOB_TITLE as WITNESS_2_JOB_TITLE",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WITNESS_2_LAST_NAME as WITNESS_2_LAST_NAME",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WITNESS_3_FIRST_NAME as WITNESS_3_FIRST_NAME",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WITNESS_3_JOB_TITLE as WITNESS_3_JOB_TITLE",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___WITNESS_3_LAST_NAME as WITNESS_3_LAST_NAME",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___LOAD_TSTMP as LOAD_TSTMP",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS___SRC_CD as SRC_CD",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS___CLAIM_NBR as CLAIM_NBR",
	"SQ_Shortcut_to_INCIDENT_INVESTIGATIONS___LOAD_TSTMP as LOAD_TSTMP1",
  "SQ_Shortcut_to_INCIDENT_INVESTIGATIONS_PRE___sys_row_id AS sys_row_id")

# COMMAND ----------

# Processing node exp_upd_inc_investigation, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 73

# for each involved DataFrame, append the dataframe name to each column
Jnr_inc_inv_temp = Jnr_inc_inv.toDF(*["Jnr_inc_inv___" + col for col in Jnr_inc_inv.columns])

exp_upd_inc_investigation = Jnr_inc_inv_temp.selectExpr(
	"Jnr_inc_inv___sys_row_id as sys_row_id",
	"IF (Jnr_inc_inv___SRC_CD IS NULL, 'RSKC', Jnr_inc_inv___SRC_CD) as SRC_CD",
	"Jnr_inc_inv___CLAIM_NBR as CLAIM_NBR",
	"Jnr_inc_inv___CLAIM_NUMBER as CLAIM_NUMBER",
	"to_date(Jnr_inc_inv___ACTION_PLAN_DEADLINE_1, 'MMddyyyy') as ACTION_PLAN_1_DEADLINE_DT",
	"to_date(Jnr_inc_inv___ACTION_PLAN_DEADLINE_2, 'MMddyyyy') as ACTION_PLAN_2_DEADLINE_DT",
	"to_date(Jnr_inc_inv___ACTION_PLAN_DEADLINE_3, 'MMddyyyy') as ACTION_PLAN_3_DEADLINE_DT",
	"Jnr_inc_inv___ACTION_PLAN_OWNER_1 as ACTION_PLAN_OWNER_1",
	"Jnr_inc_inv___ACTION_PLAN_OWNER_2 as ACTION_PLAN_OWNER_2",
	"Jnr_inc_inv___ACTION_PLAN_OWNER_3 as ACTION_PLAN_OWNER_3",
	"Jnr_inc_inv___ACTION_PLAN_OWNER_JOB_TITLE_1 as ACTION_PLAN_OWNER_JOB_TITLE_1",
	"Jnr_inc_inv___ACTION_PLAN_OWNER_JOB_TITLE_2 as ACTION_PLAN_OWNER_JOB_TITLE_2",
	"Jnr_inc_inv___ACTION_PLAN_OWNER_JOB_TITLE_3 as ACTION_PLAN_OWNER_JOB_TITLE_3",
	"Jnr_inc_inv___ACTION_PLAN_OWNER_PHONE_1 as ACTION_PLAN_OWNER_PHONE_1",
	"Jnr_inc_inv___ACTION_PLAN_OWNER_PHONE_2 as ACTION_PLAN_OWNER_PHONE_2",
	"Jnr_inc_inv___ACTION_PLAN_OWNER_PHONE_3 as ACTION_PLAN_OWNER_PHONE_3",
	"Jnr_inc_inv___ACTION_PLAN_STATUS_1 as ACTION_PLAN_STATUS_1",
	"Jnr_inc_inv___ACTION_PLAN_STATUS_2 as ACTION_PLAN_STATUS_2",
	"Jnr_inc_inv___ACTION_PLAN_STATUS_3 as ACTION_PLAN_STATUS_3",
	"Jnr_inc_inv___ADDITIONAL_CONTRIBUTING_CONDITIONS as ADDITIONAL_CONTRIBUTING_CONDITIONS",
	"to_date(Jnr_inc_inv___ASSOCIATE_S_SHIFT_END, 'MMddyyyy') as EMPLOYEE_SHIFT_END_TSTMP",
	"to_date(Jnr_inc_inv___DATE_OF_ASSOCIATE_RE_TRAINING, 'MMddyyyy') as EMPLOYEE_RETRAINING_DT",
	"to_date(Jnr_inc_inv___DATE_OF_COMPLETION_1, 'MMddyyyy') as ACTION_PLAN_1_COMPLETION_DT",
	"to_date(Jnr_inc_inv___DATE_OF_COMPLETION_2, 'MMddyyyy') as ACTION_PLAN_2_COMPLETION_DT",
	"to_date(Jnr_inc_inv___DATE_OF_COMPLETION_3, 'MMddyyyy') as ACTION_PLAN_3_COMPLETION_DT",
	"to_date(Jnr_inc_inv___DATE_OF_TEAM_RE_TRAINING, 'MMddyyyy') as TEAM_RETRAINING_DT",
	"Jnr_inc_inv___DESCRIBE_UNSAFE_CONDITION as DESCRIBE_UNSAFE_CONDITION",
	"Jnr_inc_inv___DESCRIPTION_DEVIATION as DESCRIPTION_DEVIATION",
	"Jnr_inc_inv___DESCRIPTION_EQUIPMENT as DESCRIPTION_EQUIPMENT",
	"Jnr_inc_inv___DESCRIPTION_IF_FUNCTIONING_IMPROPERLY as DESCRIPTION_IF_FUNCTIONING_IMPROPERLY",
	"Jnr_inc_inv___DESCRIPTION_MACHIN_TOOL_SUBSTANCE_OBJECT as DESCRIPTION_MACHIN_TOOL_SUBSTANCE_OBJECT",
	"Jnr_inc_inv___DESCRIPTION_OF_CORRECTIVE_ACTION_1 as DESCRIPTION_OF_CORRECTIVE_ACTION_1",
	"Jnr_inc_inv___DESCRIPTION_OF_CORRECTIVE_ACTION_2 as DESCRIPTION_OF_CORRECTIVE_ACTION_2",
	"Jnr_inc_inv___DESCRIPTION_OF_CORRECTIVE_ACTION_3 as DESCRIPTION_OF_CORRECTIVE_ACTION_3",
	"Jnr_inc_inv___DESCRIPTION_OF_INCIDENT as DESCRIPTION_OF_INCIDENT",
	"Jnr_inc_inv___DESCRIPTION_UNSAFE_ACT as DESCRIPTION_UNSAFE_ACT",
	"CASE WHEN Jnr_inc_inv___DEVIATION_FROM_CURRENT_POLICY_PROCEDURE = 'Yes' THEN '1' WHEN Jnr_inc_inv___DEVIATION_FROM_CURRENT_POLICY_PROCEDURE = 'No' THEN '0' ELSE NULL END as DEVIATION_FROM_CURRENT_POLICY_PROCEDURE_FLAG",
	"CASE WHEN Jnr_inc_inv___DID_ASSOCIATE_GO_TO_HOSPITAL = 'Yes' THEN '0' WHEN Jnr_inc_inv___DID_ASSOCIATE_GO_TO_HOSPITAL = 'No' THEN '0' ELSE NULL END as DID_ASSOCIATE_GO_TO_HOSPITAL",
	"Jnr_inc_inv___EMPLOYEE_JOB_TITLE as EMPLOYEE_JOB_TITLE",
	"CASE WHEN Jnr_inc_inv___EQUIP_FUNCTIONING_PROPERLY  = 'Yes' THEN '0' WHEN Jnr_inc_inv___EQUIP_FUNCTIONING_PROPERLY = 'No' THEN '0' ELSE NULL END as EQUIP_FUNCTIONING_PROPERLY",
	"Jnr_inc_inv___HOP_COMPLETED_BY as HOP_COMPLETED_BY",
	"Jnr_inc_inv___HOW_INCIDENT_COULD_HAVE_BEEN_PREVENTED as HOW_INCIDENT_COULD_HAVE_BEEN_PREVENTED",
	"Jnr_inc_inv___HOW_WILL_STORE_LEADERSHIP_SUPPORT as HOW_WILL_STORE_LEADERSHIP_SUPPORT",
	"Jnr_inc_inv___LEADER_COMPLETING as LEADER_COMPLETING",
	"Jnr_inc_inv___LEADER_JOB_TITLE as LEADER_JOB_TITLE",
	"CASE WHEN Jnr_inc_inv___MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED = 'Yes' THEN '0' WHEN Jnr_inc_inv___MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED = 'No' THEN '0' ELSE NULL END as MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED",
	"CASE WHEN Jnr_inc_inv___PERSONAL_PROTECTIVE_EQUIP_REQUIRED = 'Yes' THEN '0' WHEN Jnr_inc_inv___PERSONAL_PROTECTIVE_EQUIP_REQUIRED = 'No' THEN '0' ELSE NULL END as PERSONAL_PROTECTIVE_EQUIP_REQUIRED",
	"Jnr_inc_inv___PET_INCIDENT_CASE_NUMBER as PET_INCIDENT_CASE_NUMBER",
	"Jnr_inc_inv___PRIOR_INCIDENTS as PRIOR_INCIDENTS",
	"Jnr_inc_inv___RESPONSIBLE_ASSOCIATE_ID as RESPONSIBLE_ASSOCIATE_ID",
	"Jnr_inc_inv___SERVICES_LEADER as SERVICES_LEADER",
	"CASE WHEN Jnr_inc_inv___SERVICES_LEADER_PRESENT = 'Yes' THEN '1' WHEN Jnr_inc_inv___SERVICES_LEADER_PRESENT = 'No' THEN '0' ELSE NULL END as SERVICES_LEADER_PRESENT_FLAG",
	"CASE WHEN Jnr_inc_inv___SERVICES_LEADER_SIGN_OFF = 'Yes' THEN '1' WHEN Jnr_inc_inv___SERVICES_LEADER_SIGN_OFF = 'No' THEN '0' ELSE NULL END as SERVICES_LEADER_SIGN_OFF_FLAG",
	"to_date(Jnr_inc_inv___SIGN_OFF_DATE_SERVICES_LEADER, 'MMddyyyy') as SERVICES_LEADER_SIGN_OFF_DT",
	"to_date(Jnr_inc_inv___SIGN_OFF_DATE_STORE_LEADER, 'MMddyyyy') as STORE_LEADER_SIGN_OFF_DT",
	"CASE WHEN Jnr_inc_inv___SIMILAR_INCIDENTS_LAST_12_MONTHS = 'Yes' THEN '1' WHEN Jnr_inc_inv___SIMILAR_INCIDENTS_LAST_12_MONTHS = 'No' THEN '0' ELSE NULL END as SIMILAR_INCIDENTS_LAST_12_MO_FLAG",
	"CASE WHEN Jnr_inc_inv___STORE_LEADER_SIGN_OFF = 'Yes' THEN '1' WHEN Jnr_inc_inv___STORE_LEADER_SIGN_OFF = 'No' THEN '0' ELSE NULL END as STORE_LEADER_SIGN_OFF_FLAG",
	"CASE WHEN Jnr_inc_inv___TENURE_IN_CURRENT_ROLE IS NOT NULL THEN cast(Jnr_inc_inv___TENURE_IN_CURRENT_ROLE as int) ELSE NULL END as EMPLOYEE_TENURE_IN_CURRENT_ROLE",
	"CASE WHEN Jnr_inc_inv___UNSAFE_ACT_CONTRIBUTED = 'Yes' THEN '1' WHEN Jnr_inc_inv___UNSAFE_ACT_CONTRIBUTED = 'No' THEN '0' ELSE NULL END as UNSAFE_ACT_CONTRIBUTED_FLAG",
	"CASE WHEN Jnr_inc_inv___UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT = 'Yes' THEN '1' WHEN Jnr_inc_inv___UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT = 'No' THEN '0' ELSE NULL END as UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT_FLAG",
	"CASE WHEN Jnr_inc_inv___UNSAFE_CONDITION_CONTRIBUTED = 'Yes' THEN '1' WHEN Jnr_inc_inv___UNSAFE_CONDITION_CONTRIBUTED = 'No' THEN '0' ELSE NULL END as UNSAFE_CONDITION_CONTRIBUTED_FLAG",
	"CASE WHEN Jnr_inc_inv___WAS_PERSONAL_PROTECTIVE_EQUIP_WORN = 'Yes' THEN '1' WHEN Jnr_inc_inv___WAS_PERSONAL_PROTECTIVE_EQUIP_WORN = 'No' THEN '0' ELSE NULL END as PERSONAL_PROTECTIVE_EQUIP_WORN_FLAG",
	"Jnr_inc_inv___WITNESS_1_FIRST_NAME as WITNESS_1_FIRST_NAME",
	"Jnr_inc_inv___WITNESS_1_JOB_TITLE as WITNESS_1_JOB_TITLE",
	"Jnr_inc_inv___WITNESS_1_LAST_NAME as WITNESS_1_LAST_NAME",
	"Jnr_inc_inv___WITNESS_2_FIRST_NAME as WITNESS_2_FIRST_NAME",
	"Jnr_inc_inv___WITNESS_2_JOB_TITLE as WITNESS_2_JOB_TITLE",
	"Jnr_inc_inv___WITNESS_2_LAST_NAME as WITNESS_2_LAST_NAME",
	"Jnr_inc_inv___WITNESS_3_FIRST_NAME as WITNESS_3_FIRST_NAME",
	"Jnr_inc_inv___WITNESS_3_JOB_TITLE as WITNESS_3_JOB_TITLE",
	"Jnr_inc_inv___WITNESS_3_LAST_NAME as WITNESS_3_LAST_NAME",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"Jnr_inc_inv___LOAD_TSTMP1 as LOAD_TSTMP11",
	"IF (Jnr_inc_inv___CLAIM_NBR IS NULL, CURRENT_TIMESTAMP, Jnr_inc_inv___LOAD_TSTMP1) as LOAD_TSTMP1"
)

# COMMAND ----------

# Processing node upd_inc_investigation, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 72

# for each involved DataFrame, append the dataframe name to each column
exp_upd_inc_investigation_temp = exp_upd_inc_investigation.toDF(*["exp_upd_inc_investigation___" + col for col in exp_upd_inc_investigation.columns])

upd_inc_investigation = exp_upd_inc_investigation_temp.selectExpr(
	"exp_upd_inc_investigation___SRC_CD as SRC_CD",
	"exp_upd_inc_investigation___CLAIM_NBR as CLAIM_NBR",
	"exp_upd_inc_investigation___CLAIM_NUMBER as CLAIM_NUMBER",
	"exp_upd_inc_investigation___ACTION_PLAN_1_DEADLINE_DT as ACTION_PLAN_1_DEADLINE_DT",
	"exp_upd_inc_investigation___ACTION_PLAN_2_DEADLINE_DT as ACTION_PLAN_2_DEADLINE_DT",
	"exp_upd_inc_investigation___ACTION_PLAN_3_DEADLINE_DT as ACTION_PLAN_3_DEADLINE_DT",
	"exp_upd_inc_investigation___ACTION_PLAN_OWNER_1 as ACTION_PLAN_OWNER_1",
	"exp_upd_inc_investigation___ACTION_PLAN_OWNER_2 as ACTION_PLAN_OWNER_2",
	"exp_upd_inc_investigation___ACTION_PLAN_OWNER_3 as ACTION_PLAN_OWNER_3",
	"exp_upd_inc_investigation___ACTION_PLAN_OWNER_JOB_TITLE_1 as ACTION_PLAN_OWNER_JOB_TITLE_1",
	"exp_upd_inc_investigation___ACTION_PLAN_OWNER_JOB_TITLE_2 as ACTION_PLAN_OWNER_JOB_TITLE_2",
	"exp_upd_inc_investigation___ACTION_PLAN_OWNER_JOB_TITLE_3 as ACTION_PLAN_OWNER_JOB_TITLE_3",
	"exp_upd_inc_investigation___ACTION_PLAN_OWNER_PHONE_1 as ACTION_PLAN_OWNER_PHONE_1",
	"exp_upd_inc_investigation___ACTION_PLAN_OWNER_PHONE_2 as ACTION_PLAN_OWNER_PHONE_2",
	"exp_upd_inc_investigation___ACTION_PLAN_OWNER_PHONE_3 as ACTION_PLAN_OWNER_PHONE_3",
	"exp_upd_inc_investigation___ACTION_PLAN_STATUS_1 as ACTION_PLAN_STATUS_1",
	"exp_upd_inc_investigation___ACTION_PLAN_STATUS_2 as ACTION_PLAN_STATUS_2",
	"exp_upd_inc_investigation___ACTION_PLAN_STATUS_3 as ACTION_PLAN_STATUS_3",
	"exp_upd_inc_investigation___ADDITIONAL_CONTRIBUTING_CONDITIONS as ADDITIONAL_CONTRIBUTING_CONDITIONS",
	"exp_upd_inc_investigation___EMPLOYEE_SHIFT_END_TSTMP as EMPLOYEE_SHIFT_END_TSTMP",
	"exp_upd_inc_investigation___EMPLOYEE_RETRAINING_DT as EMPLOYEE_RETRAINING_DT",
	"exp_upd_inc_investigation___ACTION_PLAN_1_COMPLETION_DT as ACTION_PLAN_1_COMPLETION_DT",
	"exp_upd_inc_investigation___ACTION_PLAN_2_COMPLETION_DT as ACTION_PLAN_2_COMPLETION_DT",
	"exp_upd_inc_investigation___ACTION_PLAN_3_COMPLETION_DT as ACTION_PLAN_3_COMPLETION_DT",
	"exp_upd_inc_investigation___TEAM_RETRAINING_DT as TEAM_RETRAINING_DT",
	"exp_upd_inc_investigation___DESCRIBE_UNSAFE_CONDITION as DESCRIBE_UNSAFE_CONDITION",
	"exp_upd_inc_investigation___DESCRIPTION_DEVIATION as DESCRIPTION_DEVIATION",
	"exp_upd_inc_investigation___DESCRIPTION_EQUIPMENT as DESCRIPTION_EQUIPMENT",
	"exp_upd_inc_investigation___DESCRIPTION_IF_FUNCTIONING_IMPROPERLY as DESCRIPTION_IF_FUNCTIONING_IMPROPERLY",
	"exp_upd_inc_investigation___DESCRIPTION_MACHIN_TOOL_SUBSTANCE_OBJECT as DESCRIPTION_MACHIN_TOOL_SUBSTANCE_OBJECT",
	"exp_upd_inc_investigation___DESCRIPTION_OF_CORRECTIVE_ACTION_1 as DESCRIPTION_OF_CORRECTIVE_ACTION_1",
	"exp_upd_inc_investigation___DESCRIPTION_OF_CORRECTIVE_ACTION_2 as DESCRIPTION_OF_CORRECTIVE_ACTION_2",
	"exp_upd_inc_investigation___DESCRIPTION_OF_CORRECTIVE_ACTION_3 as DESCRIPTION_OF_CORRECTIVE_ACTION_3",
	"exp_upd_inc_investigation___DESCRIPTION_OF_INCIDENT as DESCRIPTION_OF_INCIDENT",
	"exp_upd_inc_investigation___DESCRIPTION_UNSAFE_ACT as DESCRIPTION_UNSAFE_ACT",
	"exp_upd_inc_investigation___DEVIATION_FROM_CURRENT_POLICY_PROCEDURE_FLAG as DEVIATION_FROM_CURRENT_POLICY_PROCEDURE_FLAG",
	"exp_upd_inc_investigation___DID_ASSOCIATE_GO_TO_HOSPITAL as DID_ASSOCIATE_GO_TO_HOSPITAL",
	"exp_upd_inc_investigation___EMPLOYEE_JOB_TITLE as EMPLOYEE_JOB_TITLE",
	"exp_upd_inc_investigation___EQUIP_FUNCTIONING_PROPERLY as EQUIP_FUNCTIONING_PROPERLY",
	"exp_upd_inc_investigation___HOP_COMPLETED_BY as HOP_COMPLETED_BY",
	"exp_upd_inc_investigation___HOW_INCIDENT_COULD_HAVE_BEEN_PREVENTED as HOW_INCIDENT_COULD_HAVE_BEEN_PREVENTED",
	"exp_upd_inc_investigation___HOW_WILL_STORE_LEADERSHIP_SUPPORT as HOW_WILL_STORE_LEADERSHIP_SUPPORT",
	"exp_upd_inc_investigation___LEADER_COMPLETING as LEADER_COMPLETING",
	"exp_upd_inc_investigation___LEADER_JOB_TITLE as LEADER_JOB_TITLE",
	"exp_upd_inc_investigation___MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED as MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED",
	"exp_upd_inc_investigation___PERSONAL_PROTECTIVE_EQUIP_REQUIRED as PERSONAL_PROTECTIVE_EQUIP_REQUIRED",
	"exp_upd_inc_investigation___PET_INCIDENT_CASE_NUMBER as PET_INCIDENT_CASE_NUMBER",
	"exp_upd_inc_investigation___PRIOR_INCIDENTS as PRIOR_INCIDENTS",
	"exp_upd_inc_investigation___RESPONSIBLE_ASSOCIATE_ID as RESPONSIBLE_ASSOCIATE_ID",
	"exp_upd_inc_investigation___SERVICES_LEADER as SERVICES_LEADER",
	"exp_upd_inc_investigation___SERVICES_LEADER_PRESENT_FLAG as SERVICES_LEADER_PRESENT_FLAG",
	"exp_upd_inc_investigation___SERVICES_LEADER_SIGN_OFF_FLAG as SERVICES_LEADER_SIGN_OFF_FLAG",
	"exp_upd_inc_investigation___SERVICES_LEADER_SIGN_OFF_DT as SERVICES_LEADER_SIGN_OFF_DT",
	"exp_upd_inc_investigation___STORE_LEADER_SIGN_OFF_DT as STORE_LEADER_SIGN_OFF_DT",
	"exp_upd_inc_investigation___SIMILAR_INCIDENTS_LAST_12_MO_FLAG as SIMILAR_INCIDENTS_LAST_12_MO_FLAG",
	"exp_upd_inc_investigation___STORE_LEADER_SIGN_OFF_FLAG as STORE_LEADER_SIGN_OFF_FLAG",
	"exp_upd_inc_investigation___EMPLOYEE_TENURE_IN_CURRENT_ROLE as EMPLOYEE_TENURE_IN_CURRENT_ROLE",
	"exp_upd_inc_investigation___UNSAFE_ACT_CONTRIBUTED_FLAG as UNSAFE_ACT_CONTRIBUTED_FLAG",
	"exp_upd_inc_investigation___UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT_FLAG as UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT_FLAG",
	"exp_upd_inc_investigation___UNSAFE_CONDITION_CONTRIBUTED_FLAG as UNSAFE_CONDITION_CONTRIBUTED_FLAG",
	"exp_upd_inc_investigation___PERSONAL_PROTECTIVE_EQUIP_WORN_FLAG as PERSONAL_PROTECTIVE_EQUIP_WORN_FLAG",
	"exp_upd_inc_investigation___WITNESS_1_FIRST_NAME as WITNESS_1_FIRST_NAME",
	"exp_upd_inc_investigation___WITNESS_1_JOB_TITLE as WITNESS_1_JOB_TITLE",
	"exp_upd_inc_investigation___WITNESS_1_LAST_NAME as WITNESS_1_LAST_NAME",
	"exp_upd_inc_investigation___WITNESS_2_FIRST_NAME as WITNESS_2_FIRST_NAME",
	"exp_upd_inc_investigation___WITNESS_2_JOB_TITLE as WITNESS_2_JOB_TITLE",
	"exp_upd_inc_investigation___WITNESS_2_LAST_NAME as WITNESS_2_LAST_NAME",
	"exp_upd_inc_investigation___WITNESS_3_FIRST_NAME as WITNESS_3_FIRST_NAME",
	"exp_upd_inc_investigation___WITNESS_3_JOB_TITLE as WITNESS_3_JOB_TITLE",
	"exp_upd_inc_investigation___WITNESS_3_LAST_NAME as WITNESS_3_LAST_NAME",
	"exp_upd_inc_investigation___LOAD_TSTMP1 as LOAD_TSTMP",
	"exp_upd_inc_investigation___UPDATE_TSTMP as UPDATE_TSTMP") \
	.withColumn('pyspark_data_action', when(col("CLAIM_NBR").isNull(),lit(0)).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_INCIDENT_INVESTIGATIONS, type TARGET 
# COLUMN COUNT: 71


Shortcut_to_INCIDENT_INVESTIGATIONS = upd_inc_investigation.selectExpr(
	"CAST(SRC_CD AS STRING) as SRC_CD",
	"CAST(CLAIM_NUMBER AS STRING) as CLAIM_NBR",
	"CAST(PET_INCIDENT_CASE_NUMBER AS STRING) as PET_INCIDENT_CASE_NBR",
	"CAST(DESCRIPTION_OF_INCIDENT AS STRING) as INVESTIGATION_INCIDENT_DESC",
	"CAST(EMPLOYEE_SHIFT_END_TSTMP AS TIMESTAMP) as EMPLOYEE_SHIFT_END_TSTMP",
	"CAST(RESPONSIBLE_ASSOCIATE_ID AS STRING) as RESPONSIBLE_EMPLOYEE_ID",
	"CAST(EMPLOYEE_JOB_TITLE AS STRING) as EMPLOYEE_JOB_TITLE",
	"CAST(EMPLOYEE_TENURE_IN_CURRENT_ROLE AS STRING) as EMPLOYEE_TENURE_IN_CURRENT_ROLE",
	"CAST(LEADER_COMPLETING AS STRING) as LEADER_COMPLETING_INVESTIGATION",
	"CAST(LEADER_JOB_TITLE AS STRING) as LEADER_JOB_TITLE",
	"CAST(SERVICES_LEADER_PRESENT_FLAG AS TINYINT) as SERVICES_LEADER_PRESENT_FLAG",
	"CAST(SERVICES_LEADER AS STRING) as SERVICES_LEADER_NAME",
	"CAST(SERVICES_LEADER_SIGN_OFF_FLAG AS TINYINT) as SERVICES_LEADER_SIGN_OFF_FLAG",
	"CAST(SERVICES_LEADER_SIGN_OFF_DT AS DATE) as SERVICES_LEADER_SIGN_OFF_DT",
	"CAST(STORE_LEADER_SIGN_OFF_FLAG AS TINYINT) as STORE_LEADER_SIGN_OFF_FLAG",
	"CAST(STORE_LEADER_SIGN_OFF_DT AS DATE) as STORE_LEADER_SIGN_OFF_DT",
	"CAST(SIMILAR_INCIDENTS_LAST_12_MO_FLAG AS TINYINT) as SIMILAR_INCIDENTS_LAST_12_MO_FLAG",
	"CAST(PRIOR_INCIDENTS AS TINYINT) as PRIOR_PET_INCIDENTS_FLAG",
	"CAST(DID_ASSOCIATE_GO_TO_HOSPITAL AS TINYINT) as EMPLOYEE_GO_TO_HOSPITAL_FLAG",
	"CAST(HOP_COMPLETED_BY AS STRING) as HOP_COMPLETED_BY",
	"CAST(EMPLOYEE_RETRAINING_DT AS DATE) as EMPLOYEE_RETRAINING_DT",
	"CAST(TEAM_RETRAINING_DT AS DATE) as TEAM_RETRAINING_DT",
	"CAST(UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT_FLAG AS TINYINT) as UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT_FLAG",
	"CAST(UNSAFE_CONDITION_CONTRIBUTED_FLAG AS TINYINT) as UNSAFE_CONDITION_CONTRIBUTED_FLAG",
	"CAST(DESCRIBE_UNSAFE_CONDITION AS STRING) as UNSAFE_CONDITION_DESC",
	"CAST(UNSAFE_ACT_CONTRIBUTED_FLAG AS TINYINT) as UNSAFE_ACT_CONTRIBUTED_FLAG",
	"CAST(DESCRIPTION_UNSAFE_ACT AS STRING) as UNSAFE_ACT_DESC",
	"CAST(DEVIATION_FROM_CURRENT_POLICY_PROCEDURE_FLAG AS TINYINT) as DEVIATION_FROM_CURRENT_POLICY_PROCEDURE_FLAG",
	"CAST(DESCRIPTION_DEVIATION AS STRING) as DEVIATION_DESC",
	"CAST(PERSONAL_PROTECTIVE_EQUIP_REQUIRED AS TINYINT) as PERSONAL_PROTECTIVE_EQUIP_REQUIRED_FLAG",
	"CAST(PERSONAL_PROTECTIVE_EQUIP_WORN_FLAG AS TINYINT) as PERSONAL_PROTECTIVE_EQUIP_WORN_FLAG",
	"CAST(DESCRIPTION_EQUIPMENT AS STRING) as EQUIPMENT_DESC",
	"CAST(EQUIP_FUNCTIONING_PROPERLY AS TINYINT) as EQUIPMENT_FUNCTIONING_PROPERLY_FLAG",
	"CAST(DESCRIPTION_IF_FUNCTIONING_IMPROPERLY AS STRING) as EQUIPMENT_FUNCTIONING_IMPROPERLY_DESC",
	"CAST(MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED AS TINYINT) as MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED_FLAG",
	"CAST(DESCRIPTION_MACHIN_TOOL_SUBSTANCE_OBJECT AS STRING) as MACHINE_TOOL_SUBSTANCE_OBJECT_DESC",
	"CAST(ADDITIONAL_CONTRIBUTING_CONDITIONS AS STRING) as ADDITIONAL_CONTRIBUTING_CONDITIONS",
	"CAST(HOW_INCIDENT_COULD_HAVE_BEEN_PREVENTED AS STRING) as PREVENTION_STRATEGY",
	"CAST(HOW_WILL_STORE_LEADERSHIP_SUPPORT AS STRING) as LEADERSHIP_SUPPORT_STRATEGY",
	"CAST(WITNESS_1_FIRST_NAME AS STRING) as WITNESS_1_FIRST_NAME",
	"CAST(WITNESS_1_JOB_TITLE AS STRING) as WITNESS_1_JOB_TITLE",
	"CAST(WITNESS_1_LAST_NAME AS STRING) as WITNESS_1_LAST_NAME",
	"CAST(WITNESS_2_FIRST_NAME AS STRING) as WITNESS_2_FIRST_NAME",
	"CAST(WITNESS_2_JOB_TITLE AS STRING) as WITNESS_2_JOB_TITLE",
	"CAST(WITNESS_2_LAST_NAME AS STRING) as WITNESS_2_LAST_NAME",
	"CAST(WITNESS_3_FIRST_NAME AS STRING) as WITNESS_3_FIRST_NAME",
	"CAST(WITNESS_3_JOB_TITLE AS STRING) as WITNESS_3_JOB_TITLE",
	"CAST(WITNESS_3_LAST_NAME AS STRING) as WITNESS_3_LAST_NAME",
	"CAST(DESCRIPTION_OF_CORRECTIVE_ACTION_1 AS STRING) as ACTION_PLAN_1_DESC",
	"CAST(ACTION_PLAN_OWNER_1 AS STRING) as ACTION_PLAN_1_OWNER",
	"CAST(ACTION_PLAN_OWNER_JOB_TITLE_1 AS STRING) as ACTION_PLAN_1_OWNER_JOB_TITLE",
	"CAST(ACTION_PLAN_OWNER_PHONE_1 AS STRING) as ACTION_PLAN_1_OWNER_PHONE_NBR",
	"CAST(ACTION_PLAN_1_DEADLINE_DT AS DATE) as ACTION_PLAN_1_DEADLINE_DT",
	"CAST(ACTION_PLAN_1_COMPLETION_DT AS DATE) as ACTION_PLAN_1_COMPLETION_DT",
	"CAST(ACTION_PLAN_STATUS_1 AS STRING) as ACTION_PLAN_1_STATUS",
	"CAST(DESCRIPTION_OF_CORRECTIVE_ACTION_2 AS STRING) as ACTION_PLAN_2_DESC",
	"CAST(ACTION_PLAN_OWNER_2 AS STRING) as ACTION_PLAN_2_OWNER",
	"CAST(ACTION_PLAN_OWNER_JOB_TITLE_2 AS STRING) as ACTION_PLAN_2_OWNER_JOB_TITLE",
	"CAST(ACTION_PLAN_OWNER_PHONE_2 AS STRING) as ACTION_PLAN_2_OWNER_PHONE_NBR",
	"CAST(ACTION_PLAN_2_DEADLINE_DT AS DATE) as ACTION_PLAN_2_DEADLINE_DT",
	"CAST(ACTION_PLAN_2_COMPLETION_DT AS DATE) as ACTION_PLAN_2_COMPLETION_DT",
	"CAST(ACTION_PLAN_STATUS_2 AS STRING) as ACTION_PLAN_2_STATUS",
	"CAST(DESCRIPTION_OF_CORRECTIVE_ACTION_3 AS STRING) as ACTION_PLAN_3_DESC",
	"CAST(ACTION_PLAN_OWNER_3 AS STRING) as ACTION_PLAN_3_OWNER",
	"CAST(ACTION_PLAN_OWNER_JOB_TITLE_3 AS STRING) as ACTION_PLAN_3_OWNER_JOB_TITLE",
	"CAST(ACTION_PLAN_OWNER_PHONE_3 AS STRING) as ACTION_PLAN_3_OWNER_PHONE_NBR",
	"CAST(ACTION_PLAN_3_DEADLINE_DT AS DATE) as ACTION_PLAN_3_DEADLINE_DT",
	"CAST(ACTION_PLAN_3_COMPLETION_DT AS DATE) as ACTION_PLAN_3_COMPLETION_DT",
	"CAST(ACTION_PLAN_STATUS_3 AS STRING) as ACTION_PLAN_3_STATUS",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.SRC_CD = target.SRC_CD AND source.CLAIM_NBR = target.CLAIM_NBR"""
	refined_perf_table = f"{legacy}.INCIDENT_INVESTIGATIONS"
	executeMerge(Shortcut_to_INCIDENT_INVESTIGATIONS, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("INCIDENT_INVESTIGATIONS", "INCIDENT_INVESTIGATIONS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("INCIDENT_INVESTIGATIONS", "INCIDENT_INVESTIGATIONS","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		
