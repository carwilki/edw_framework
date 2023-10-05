# Databricks notebook source
# Code converted on 2023-08-24 12:12:55
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

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"


# COMMAND ----------

# Processing node SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT, type SOURCE
# COLUMN COUNT: 68

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-qa-raw-p1-gcs-gbl/sap/riskonnect_in/')
# source_bucket = dbutils.widgets.get('source_bucket')

_bucket = getParameterValue(
    raw,
    "BA_Incident_Management_Parameter.prm",
    "BA_Incident_Management.WF:wf_Riskonnect_In",
    "source_bucket",
)
source_bucket = _bucket + "riskonnect_in/"


def get_source_file(key, _bucket):
    import builtins

    lst = dbutils.fs.ls(_bucket)
    fldr = builtins.max(lst, key=lambda x: x.name).name
    _path = os.path.join(_bucket, fldr)
    lst = dbutils.fs.ls(_path)
    files = [x.path for x in lst if x.name.startswith(key)]
    return files[0] if files else None


source_file = get_source_file("Riskonnect_Incident_Investigation", source_bucket)

SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT = spark.read.csv(
    source_file, sep="|", header=True, quote='"', multiLine=True
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_CLAIM_NBR, type FILTER
# COLUMN COUNT: 68

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT_temp = (
    SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT.toDF(
        *[
            "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___" + col
            for col in SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT.columns
        ]
    )
)

FIL_CLAIM_NBR = (
    SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT_temp.selectExpr(
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Claim_Number as Claim_Number",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Deadline_1 as Action_Plan_Deadline_1",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Deadline_2 as Action_Plan_Deadline_2",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Deadline_3 as Action_Plan_Deadline_3",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Owner_1 as Action_Plan_Owner_1",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Owner_2 as Action_Plan_Owner_2",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Owner_3 as Action_Plan_Owner_3",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Owner_Job_Title_1 as Action_Plan_Owner_Job_Title_1",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Owner_Job_Title_2 as Action_Plan_Owner_Job_Title_2",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Owner_Job_Title_3 as Action_Plan_Owner_Job_Title_3",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Owner_Phone_1 as Action_Plan_Owner_Phone_1",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Owner_Phone_2 as Action_Plan_Owner_Phone_2",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Owner_Phone_3 as Action_Plan_Owner_Phone_3",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Status_1 as Action_Plan_Status_1",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Status_2 as Action_Plan_Status_2",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Action_Plan_Status_3 as Action_Plan_Status_3",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Additional_Contributing_Conditions as Additional_Contributing_Conditions",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Associate_s_Shift_End as Associate_s_Shift_End",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Date_of_Associate_re_training as Date_of_Associate_re_training",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Date_of_Completion_1 as Date_of_Completion_1",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Date_of_Completion_2 as Date_of_Completion_2",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Date_of_Completion_3 as Date_of_Completion_3",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Date_of_Team_Re_Training as Date_of_Team_Re_Training",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Describe_Unsafe_Condition as Describe_Unsafe_Condition",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Description_Deviation as Description_Deviation",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Description_Equipment as Description_Equipment",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Description_If_Functioning_Improperly as Description_If_Functioning_Improperly",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Description_Machin_Tool_Substance_Object as Description_Machin_Tool_Substance_Object",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Description_of_Corrective_Action_1 as Description_of_Corrective_Action_1",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Description_of_Corrective_Action_2 as Description_of_Corrective_Action_2",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Description_of_Corrective_Action_3 as Description_of_Corrective_Action_3",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Description_of_Incident as Description_of_Incident",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Description_Unsafe_Act as Description_Unsafe_Act",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Deviation_from_Current_Policy_Procedure as Deviation_from_Current_Policy_Procedure",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Did_Associate_Go_to_Hospital as Did_Associate_Go_to_Hospital",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Employee_Job_Title as Employee_Job_Title",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Equip_Functioning_Properly as Equip_Functioning_Properly",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___HOP_Completed_By as HOP_Completed_By",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___How_Incident_Could_Have_Been_Prevented as How_Incident_Could_Have_Been_Prevented",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___How_Will_Store_Leadership_Support as How_Will_Store_Leadership_Support",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Leader_Completing as Leader_Completing",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Leader_Job_Title as Leader_Job_Title",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Machine_Tool_Substance_Object_Involved as Machine_Tool_Substance_Object_Involved",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Personal_Protective_Equip_Required as Personal_Protective_Equip_Required",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Pet_Incident_Case_Number as Pet_Incident_Case_Number",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Prior_Incidents as Prior_Incidents",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Responsible_Associate_ID as Responsible_Associate_ID",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Services_Leader as Services_Leader",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Services_Leader_Present as Services_Leader_Present",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Services_Leader_Sign_Off as Services_Leader_Sign_Off",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Sign_Off_Date_Services_Leader as Sign_Off_Date_Services_Leader",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Sign_Off_Date_Store_Leader as Sign_Off_Date_Store_Leader",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Similar_Incidents_Last_12_Months as Similar_Incidents_Last_12_Months",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Store_Leader_Sign_Off as Store_Leader_Sign_Off",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Tenure_in_Current_Role as Tenure_in_Current_Role",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Unsafe_Act_Contributed as Unsafe_Act_Contributed",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Unsafe_Condition_Act_Prior_to_Incident as Unsafe_Condition_Act_Prior_to_Incident",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Unsafe_Condition_Contributed as Unsafe_Condition_Contributed",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Was_Personal_Protective_Equip_Worn as Was_Personal_Protective_Equip_Worn",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Witness_1_First_Name as Witness_1_First_Name",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Witness_1_Job_Title as Witness_1_Job_Title",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Witness_1_Last_Name as Witness_1_Last_Name",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Witness_2_First_Name as Witness_2_First_Name",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Witness_2_Job_Title as Witness_2_Job_Title",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Witness_2_Last_Name as Witness_2_Last_Name",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Witness_3_First_Name as Witness_3_First_Name",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Witness_3_Job_Title as Witness_3_Job_Title",
        "SQ_Shortcut_to_RISKONNECT_INCIDENT_INVESTIGATION_FLAT___Witness_3_Last_Name as Witness_3_Last_Name",
    )
    .filter("Claim_Number IS NOT NULL")
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node EXP_LOAD_TSTMP, type EXPRESSION
# COLUMN COUNT: 69

# for each involved DataFrame, append the dataframe name to each column
FIL_CLAIM_NBR_temp = FIL_CLAIM_NBR.toDF(
    *["FIL_CLAIM_NBR___" + col for col in FIL_CLAIM_NBR.columns]
)

EXP_LOAD_TSTMP = FIL_CLAIM_NBR_temp.selectExpr(
    "FIL_CLAIM_NBR___sys_row_id as sys_row_id",
    "FIL_CLAIM_NBR___Claim_Number as Claim_Number",
    "FIL_CLAIM_NBR___Action_Plan_Deadline_1 as Action_Plan_Deadline_1",
    "FIL_CLAIM_NBR___Action_Plan_Deadline_2 as Action_Plan_Deadline_2",
    "FIL_CLAIM_NBR___Action_Plan_Deadline_3 as Action_Plan_Deadline_3",
    "FIL_CLAIM_NBR___Action_Plan_Owner_1 as Action_Plan_Owner_1",
    "FIL_CLAIM_NBR___Action_Plan_Owner_2 as Action_Plan_Owner_2",
    "FIL_CLAIM_NBR___Action_Plan_Owner_3 as Action_Plan_Owner_3",
    "FIL_CLAIM_NBR___Action_Plan_Owner_Job_Title_1 as Action_Plan_Owner_Job_Title_1",
    "FIL_CLAIM_NBR___Action_Plan_Owner_Job_Title_2 as Action_Plan_Owner_Job_Title_2",
    "FIL_CLAIM_NBR___Action_Plan_Owner_Job_Title_3 as Action_Plan_Owner_Job_Title_3",
    "FIL_CLAIM_NBR___Action_Plan_Owner_Phone_1 as Action_Plan_Owner_Phone_1",
    "FIL_CLAIM_NBR___Action_Plan_Owner_Phone_2 as Action_Plan_Owner_Phone_2",
    "FIL_CLAIM_NBR___Action_Plan_Owner_Phone_3 as Action_Plan_Owner_Phone_3",
    "FIL_CLAIM_NBR___Action_Plan_Status_1 as Action_Plan_Status_1",
    "FIL_CLAIM_NBR___Action_Plan_Status_2 as Action_Plan_Status_2",
    "FIL_CLAIM_NBR___Action_Plan_Status_3 as Action_Plan_Status_3",
    "FIL_CLAIM_NBR___Additional_Contributing_Conditions as Additional_Contributing_Conditions",
    "FIL_CLAIM_NBR___Associate_s_Shift_End as Associate_s_Shift_End",
    "FIL_CLAIM_NBR___Date_of_Associate_re_training as Date_of_Associate_re_training",
    "FIL_CLAIM_NBR___Date_of_Completion_1 as Date_of_Completion_1",
    "FIL_CLAIM_NBR___Date_of_Completion_2 as Date_of_Completion_2",
    "FIL_CLAIM_NBR___Date_of_Completion_3 as Date_of_Completion_3",
    "FIL_CLAIM_NBR___Date_of_Team_Re_Training as Date_of_Team_Re_Training",
    "FIL_CLAIM_NBR___Describe_Unsafe_Condition as Describe_Unsafe_Condition",
    "FIL_CLAIM_NBR___Description_Deviation as Description_Deviation",
    "FIL_CLAIM_NBR___Description_Equipment as Description_Equipment",
    "FIL_CLAIM_NBR___Description_If_Functioning_Improperly as Description_If_Functioning_Improperly",
    "FIL_CLAIM_NBR___Description_Machin_Tool_Substance_Object as Description_Machin_Tool_Substance_Object",
    "FIL_CLAIM_NBR___Description_of_Corrective_Action_1 as Description_of_Corrective_Action_1",
    "FIL_CLAIM_NBR___Description_of_Corrective_Action_2 as Description_of_Corrective_Action_2",
    "FIL_CLAIM_NBR___Description_of_Corrective_Action_3 as Description_of_Corrective_Action_3",
    "FIL_CLAIM_NBR___Description_of_Incident as Description_of_Incident",
    "FIL_CLAIM_NBR___Description_Unsafe_Act as Description_Unsafe_Act",
    "FIL_CLAIM_NBR___Deviation_from_Current_Policy_Procedure as Deviation_from_Current_Policy_Procedure",
    "FIL_CLAIM_NBR___Did_Associate_Go_to_Hospital as Did_Associate_Go_to_Hospital",
    "FIL_CLAIM_NBR___Employee_Job_Title as Employee_Job_Title",
    "FIL_CLAIM_NBR___Equip_Functioning_Properly as Equip_Functioning_Properly",
    "FIL_CLAIM_NBR___HOP_Completed_By as HOP_Completed_By",
    "FIL_CLAIM_NBR___How_Incident_Could_Have_Been_Prevented as How_Incident_Could_Have_Been_Prevented",
    "FIL_CLAIM_NBR___How_Will_Store_Leadership_Support as How_Will_Store_Leadership_Support",
    "FIL_CLAIM_NBR___Leader_Completing as Leader_Completing",
    "FIL_CLAIM_NBR___Leader_Job_Title as Leader_Job_Title",
    "FIL_CLAIM_NBR___Machine_Tool_Substance_Object_Involved as Machine_Tool_Substance_Object_Involved",
    "FIL_CLAIM_NBR___Personal_Protective_Equip_Required as Personal_Protective_Equip_Required",
    "FIL_CLAIM_NBR___Pet_Incident_Case_Number as Pet_Incident_Case_Number",
    "FIL_CLAIM_NBR___Prior_Incidents as Prior_Incidents",
    "FIL_CLAIM_NBR___Responsible_Associate_ID as Responsible_Associate_ID",
    "FIL_CLAIM_NBR___Services_Leader as Services_Leader",
    "FIL_CLAIM_NBR___Services_Leader_Present as Services_Leader_Present",
    "FIL_CLAIM_NBR___Services_Leader_Sign_Off as Services_Leader_Sign_Off",
    "FIL_CLAIM_NBR___Sign_Off_Date_Services_Leader as Sign_Off_Date_Services_Leader",
    "FIL_CLAIM_NBR___Sign_Off_Date_Store_Leader as Sign_Off_Date_Store_Leader",
    "FIL_CLAIM_NBR___Similar_Incidents_Last_12_Months as Similar_Incidents_Last_12_Months",
    "FIL_CLAIM_NBR___Store_Leader_Sign_Off as Store_Leader_Sign_Off",
    "FIL_CLAIM_NBR___Tenure_in_Current_Role as Tenure_in_Current_Role",
    "FIL_CLAIM_NBR___Unsafe_Act_Contributed as Unsafe_Act_Contributed",
    "FIL_CLAIM_NBR___Unsafe_Condition_Act_Prior_to_Incident as Unsafe_Condition_Act_Prior_to_Incident",
    "FIL_CLAIM_NBR___Unsafe_Condition_Contributed as Unsafe_Condition_Contributed",
    "FIL_CLAIM_NBR___Was_Personal_Protective_Equip_Worn as Was_Personal_Protective_Equip_Worn",
    "FIL_CLAIM_NBR___Witness_1_First_Name as Witness_1_First_Name",
    "FIL_CLAIM_NBR___Witness_1_Job_Title as Witness_1_Job_Title",
    "FIL_CLAIM_NBR___Witness_1_Last_Name as Witness_1_Last_Name",
    "FIL_CLAIM_NBR___Witness_2_First_Name as Witness_2_First_Name",
    "FIL_CLAIM_NBR___Witness_2_Job_Title as Witness_2_Job_Title",
    "FIL_CLAIM_NBR___Witness_2_Last_Name as Witness_2_Last_Name",
    "FIL_CLAIM_NBR___Witness_3_First_Name as Witness_3_First_Name",
    "FIL_CLAIM_NBR___Witness_3_Job_Title as Witness_3_Job_Title",
    "FIL_CLAIM_NBR___Witness_3_Last_Name as Witness_3_Last_Name",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)

# COMMAND ----------

# Processing node Shortcut_to_INCIDENT_INVESTIGATIONS_PRE, type TARGET
# COLUMN COUNT: 69


Shortcut_to_INCIDENT_INVESTIGATIONS_PRE = EXP_LOAD_TSTMP.selectExpr(
    "CAST(Claim_Number AS STRING) as CLAIM_NUMBER",
    "CAST(Action_Plan_Deadline_1 AS STRING) as ACTION_PLAN_DEADLINE_1",
    "CAST(Action_Plan_Deadline_2 AS STRING) as ACTION_PLAN_DEADLINE_2",
    "CAST(Action_Plan_Deadline_3 AS STRING) as ACTION_PLAN_DEADLINE_3",
    "CAST(Action_Plan_Owner_1 AS STRING) as ACTION_PLAN_OWNER_1",
    "CAST(Action_Plan_Owner_2 AS STRING) as ACTION_PLAN_OWNER_2",
    "CAST(Action_Plan_Owner_3 AS STRING) as ACTION_PLAN_OWNER_3",
    "CAST(Action_Plan_Owner_Job_Title_1 AS STRING) as ACTION_PLAN_OWNER_JOB_TITLE_1",
    "CAST(Action_Plan_Owner_Job_Title_2 AS STRING) as ACTION_PLAN_OWNER_JOB_TITLE_2",
    "CAST(Action_Plan_Owner_Job_Title_3 AS STRING) as ACTION_PLAN_OWNER_JOB_TITLE_3",
    "CAST(Action_Plan_Owner_Phone_1 AS STRING) as ACTION_PLAN_OWNER_PHONE_1",
    "CAST(Action_Plan_Owner_Phone_2 AS STRING) as ACTION_PLAN_OWNER_PHONE_2",
    "CAST(Action_Plan_Owner_Phone_3 AS STRING) as ACTION_PLAN_OWNER_PHONE_3",
    "CAST(Action_Plan_Status_1 AS STRING) as ACTION_PLAN_STATUS_1",
    "CAST(Action_Plan_Status_2 AS STRING) as ACTION_PLAN_STATUS_2",
    "CAST(Action_Plan_Status_3 AS STRING) as ACTION_PLAN_STATUS_3",
    "CAST(Additional_Contributing_Conditions AS STRING) as ADDITIONAL_CONTRIBUTING_CONDITIONS",
    "CAST(Associate_s_Shift_End AS STRING) as ASSOCIATE_S_SHIFT_END",
    "CAST(Date_of_Associate_re_training AS STRING) as DATE_OF_ASSOCIATE_RE_TRAINING",
    "CAST(Date_of_Completion_1 AS STRING) as DATE_OF_COMPLETION_1",
    "CAST(Date_of_Completion_2 AS STRING) as DATE_OF_COMPLETION_2",
    "CAST(Date_of_Completion_3 AS STRING) as DATE_OF_COMPLETION_3",
    "CAST(Date_of_Team_Re_Training AS STRING) as DATE_OF_TEAM_RE_TRAINING",
    "CAST(Describe_Unsafe_Condition AS STRING) as DESCRIBE_UNSAFE_CONDITION",
    "CAST(Description_Deviation AS STRING) as DESCRIPTION_DEVIATION",
    "CAST(Description_Equipment AS STRING) as DESCRIPTION_EQUIPMENT",
    "CAST(Description_If_Functioning_Improperly AS STRING) as DESCRIPTION_IF_FUNCTIONING_IMPROPERLY",
    "CAST(Description_Machin_Tool_Substance_Object AS STRING) as DESCRIPTION_MACHIN_TOOL_SUBSTANCE_OBJECT",
    "CAST(Description_of_Corrective_Action_1 AS STRING) as DESCRIPTION_OF_CORRECTIVE_ACTION_1",
    "CAST(Description_of_Corrective_Action_2 AS STRING) as DESCRIPTION_OF_CORRECTIVE_ACTION_2",
    "CAST(Description_of_Corrective_Action_3 AS STRING) as DESCRIPTION_OF_CORRECTIVE_ACTION_3",
    "CAST(Description_of_Incident AS STRING) as DESCRIPTION_OF_INCIDENT",
    "CAST(Description_Unsafe_Act AS STRING) as DESCRIPTION_UNSAFE_ACT",
    "CAST(Deviation_from_Current_Policy_Procedure AS STRING) as DEVIATION_FROM_CURRENT_POLICY_PROCEDURE",
    "CAST(Did_Associate_Go_to_Hospital AS STRING) as DID_ASSOCIATE_GO_TO_HOSPITAL",
    "CAST(Employee_Job_Title AS STRING) as EMPLOYEE_JOB_TITLE",
    "CAST(Equip_Functioning_Properly AS STRING) as EQUIP_FUNCTIONING_PROPERLY",
    "CAST(HOP_Completed_By AS STRING) as HOP_COMPLETED_BY",
    "CAST(How_Incident_Could_Have_Been_Prevented AS STRING) as HOW_INCIDENT_COULD_HAVE_BEEN_PREVENTED",
    "CAST(How_Will_Store_Leadership_Support AS STRING) as HOW_WILL_STORE_LEADERSHIP_SUPPORT",
    "CAST(Leader_Completing AS STRING) as LEADER_COMPLETING",
    "CAST(Leader_Job_Title AS STRING) as LEADER_JOB_TITLE",
    "CAST(Machine_Tool_Substance_Object_Involved AS STRING) as MACHINE_TOOL_SUBSTANCE_OBJECT_INVOLVED",
    "CAST(Personal_Protective_Equip_Required AS STRING) as PERSONAL_PROTECTIVE_EQUIP_REQUIRED",
    "CAST(Pet_Incident_Case_Number AS STRING) as PET_INCIDENT_CASE_NUMBER",
    "CAST(Prior_Incidents AS STRING) as PRIOR_INCIDENTS",
    "CAST(Responsible_Associate_ID AS STRING) as RESPONSIBLE_ASSOCIATE_ID",
    "CAST(Services_Leader AS STRING) as SERVICES_LEADER",
    "CAST(Services_Leader_Present AS STRING) as SERVICES_LEADER_PRESENT",
    "CAST(Services_Leader_Sign_Off AS STRING) as SERVICES_LEADER_SIGN_OFF",
    "CAST(Sign_Off_Date_Services_Leader AS STRING) as SIGN_OFF_DATE_SERVICES_LEADER",
    "CAST(Sign_Off_Date_Store_Leader AS STRING) as SIGN_OFF_DATE_STORE_LEADER",
    "CAST(Similar_Incidents_Last_12_Months AS STRING) as SIMILAR_INCIDENTS_LAST_12_MONTHS",
    "CAST(Store_Leader_Sign_Off AS STRING) as STORE_LEADER_SIGN_OFF",
    "CAST(Tenure_in_Current_Role AS STRING) as TENURE_IN_CURRENT_ROLE",
    "CAST(Unsafe_Act_Contributed AS STRING) as UNSAFE_ACT_CONTRIBUTED",
    "CAST(Unsafe_Condition_Act_Prior_to_Incident AS STRING) as UNSAFE_CONDITION_ACT_PRIOR_TO_INCIDENT",
    "CAST(Unsafe_Condition_Contributed AS STRING) as UNSAFE_CONDITION_CONTRIBUTED",
    "CAST(Was_Personal_Protective_Equip_Worn AS STRING) as WAS_PERSONAL_PROTECTIVE_EQUIP_WORN",
    "CAST(Witness_1_First_Name AS STRING) as WITNESS_1_FIRST_NAME",
    "CAST(Witness_1_Job_Title AS STRING) as WITNESS_1_JOB_TITLE",
    "CAST(Witness_1_Last_Name AS STRING) as WITNESS_1_LAST_NAME",
    "CAST(Witness_2_First_Name AS STRING) as WITNESS_2_FIRST_NAME",
    "CAST(Witness_2_Job_Title AS STRING) as WITNESS_2_JOB_TITLE",
    "CAST(Witness_2_Last_Name AS STRING) as WITNESS_2_LAST_NAME",
    "CAST(Witness_3_First_Name AS STRING) as WITNESS_3_FIRST_NAME",
    "CAST(Witness_3_Job_Title AS STRING) as WITNESS_3_JOB_TITLE",
    "CAST(Witness_3_Last_Name AS STRING) as WITNESS_3_LAST_NAME",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)

Shortcut_to_INCIDENT_INVESTIGATIONS_PRE.write.mode("overwrite").saveAsTable(
    f"{raw}.INCIDENT_INVESTIGATIONS_PRE"
)
