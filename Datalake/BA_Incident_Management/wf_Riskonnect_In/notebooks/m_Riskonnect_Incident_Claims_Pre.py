# Databricks notebook source
# Code converted on 2023-08-24 12:12:53
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

# Processing node SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT, type SOURCE
# COLUMN COUNT: 114

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-qa-raw-p1-gcs-gbl/sap/riskonnect_in/')
# source_bucket = dbutils.widgets.get('source_bucket')

_bucket = getParameterValue(
    raw,
    "BA_Incident_Management_Parameter.prm",
    "BA_Incident_Management.WF:wf_Riskonnect_In:incident_claim",
    "source_bucket",
)
source_bucket = _bucket

def get_source_file(key, _bucket):
    import builtins

    lst = dbutils.fs.ls(_bucket)
    fldr = builtins.max(lst, key=lambda x: x.name).name
    _path = os.path.join(_bucket, fldr)
    lst = dbutils.fs.ls(_path)
    files = [x.path for x in lst if x.name.startswith(key)]
    return files[0] if files else None


source_file = get_source_file("Riskonnect_Claim", source_bucket)

SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT = spark.read.csv(
    source_file, sep="|", header=True, quote='"', escape="\n", multiLine=True
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_LOAD_TSTMP, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 116

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT_temp = SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT.toDF(
    *[
        "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___" + col
        for col in SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT.columns
    ]
)

EXP_LOAD_TSTMP = SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT_temp.selectExpr(
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Claim_Number as Claim_Number",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Academy_Trained as Academy_Trained",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Accident_Description as Accident_Description",
    "regexp_replace(SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Accident_Description, '\\n|\\r|\\|', '') as Accident_Description_new",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Accident_Time as Accident_Time",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Activity_Engaged_In as Activity_Engaged_In",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Animal_Care_Custody as Animal_Care_Custody",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Animal_Inj_Type as Animal_Inj_Type",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Animal_involved_in_the_incident as Animal_involved_in_the_incident",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Associate_Name as Associate_Name",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Banfield_Amount as Banfield_Amount",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Brand_Risk_Dept as Brand_Risk_Dept",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Brand_Risk_Escalation_Factors as Brand_Risk_Escalation_Factors",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Cash_Amount as Cash_Amount",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Cause as Cause",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Cause_General as Cause_General",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Cause_SRC as Cause_SRC",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Claim_SubStatus as Claim_SubStatus",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Claimant_Length_Service as Claimant_Length_Service",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Claimant_Name as Claimant_Name",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Comped_Products as Comped_Products",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Comped_Service_s as Comped_Service_s",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Coverage_Major as Coverage_Major",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Date_Closed as Date_Closed",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Date_Hired as Date_Hired",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Date_of_Loss as Date_of_Loss",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Date_Opened as Date_Opened",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Date_Product_Purch as Date_Product_Purch",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Date_Reported_to_Client as Date_Reported_to_Client",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Date_Safety_Certified as Date_Safety_Certified",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Day_Camp_Capacity as Day_Camp_Capacity",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Department as Department",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Did_Associate_Go_to_Hospital as Did_Associate_Go_to_Hospital",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Employee_Id as Employee_Id",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Flowers as Flowers",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Full_Recovery_Expected as Full_Recovery_Expected",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Gift_Card_s as Gift_Cards_s",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Has_the_pet_been_neutered_spayed as Has_the_pet_been_neutered_spayed",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Header_Code as Header_Code",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Health_Chart_Completed_During_Stay as Health_Chart_Completed_During_Stay",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___How_Did_Store_Find_Out as How_Did_Store_Find_Out",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___How_Many_Pets_in_Playtime as How_Many_Pets_in_Playtime",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___How_Many_Pets_in_Training_Class as How_Many_Pets_in_Training_Class",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Human_Injury_Escalation as Human_Injury_Escalation",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Incident_Occur_During_PetSmart_Academy as Incident_Occur_During_PetSmart_Academy",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Injury_Illness_confirmed_by_Veterinarian as Injury_Illness_confirmed_by_Veterinarian",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Lost_Time_Indicator as Lost_Time_Indicator",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Matted_Pet_Release as Matted_Pet_Release",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Nature as Nature",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Nature_General as Nature_General",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Gross_Incurred as Net_Incurred",  # this column is missing in the source file
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Node_Code as Node_Code",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Other as Other",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Other_Policy_Violation as Other_Policy_Violation",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___P_Card_Amount as P_Card_Amount",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Part as Part",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Part_General as Part_General",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Part_Position as Part_Position",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Age as Pet_Age",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Breed as Pet_Breed",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Breed_Type as Pet_Breed_Type",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Expense_Type as Pet_Expense_Type",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Gender as Pet_Gender",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Incident_Type as Pet_Incident_Type",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Origin as Pet_Origin",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Parent_Vet_Exp as Pet_Parent_Vet_Exp",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Safety_Cause as Pet_Safety_Cause",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Safety_Cause_General as Pet_Safety_Cause_General",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Safety_Equip as Pet_Safety_Equip",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Safety_Nature as Pet_Safety_Nature",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Safety_Nature_General as Pet_Safety_Nature_General",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Safety_Part as Pet_Safety_Part",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Safety_Part_General as Pet_Safety_Part_General",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Safety_Priority as Pet_Safety_Priority",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Symptoms as Pet_Symptoms",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Taken_to_Vet as Pet_Taken_to_Vet",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_UPC as Pet_UPC",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Pet_Weight as Pet_Weight",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___PetSmart_Incident_Type as PetSmart_Incident_Type",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Policy_Violation as Policy_Violation",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Product_Best_By_Date as Product_Best_By_Date",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Product_Lot_Code as Product_Lot_Code",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Product_SKU_short_product_code as Product_SKU_short_product_code",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Product_UPC_long_product_code as Product_UPC_long_product_code",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Reason_Code as Reason_Code",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Service_Performed as Service_Performed",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Shave_Authorization as Shave_Authorization",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Specific_Store_Area as Specific_Store_Area",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Srvc_Card_Signed as Srvc_Card_Signed",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Status as Status",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Store_DC_Area as Store_DC_Area",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Time_Employee_Began_Work as Time_Employee_Began_Work",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___TPA_Carrier as TPA_Carrier",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___TPA_Claim_Number as TPA_Claim_Number",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Treatment_Intent as Treatment_Intent",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Was_911_Called as Was_911_Called",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Was_Associate_Admitted_to_Hospital as Was_Associate_Admitted_to_Hospital",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Was_Associate_Transported_by_Ambulance as Was_Associate_Transported_by_Ambulance",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___What_Point_did_the_Incident_Occur as What_Point_did_the_Incident_Occur",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___What_Point_During_Service_Occur_Hotel as What_Point_During_Service_Occur_Hotel",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Work_Related as Work_Related",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Type_of_Incident_Reported as Type_of_Incident_Reported",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Scorecard_Exception_Flag as Scorecard_Exception_Flag",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Responsible_Associate_ID as Responsible_Associate_ID",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Check_Amount as Check_Amount",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Claim_Type_SRC as Claim_Type_SRC",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Cause_Legacy as Cause_Legacy",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Reason_Legacy as Reason_Legacy",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Date_Reported_to_TPA as Date_Reported_to_TPA",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Adoption_Group as Adoption_Group",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Closed_Issue_Date as Closed_Issue_Date",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Employee_Job_Title as Employee_Job_Title",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Received as Received",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Responded as Responded",
    "SQ_Shortcut_to_RISKONNECT_CLAIMS_FLAT___Response_Due as Response_Due",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)

# COMMAND ----------

# Processing node Shortcut_to_INCIDENT_CLAIMS_PRE, type TARGET
# COLUMN COUNT: 115

Shortcut_to_INCIDENT_CLAIMS_PRE = EXP_LOAD_TSTMP.selectExpr(
    "CAST(Claim_Number AS STRING) as CLAIM_NUMBER",
    "CAST(Academy_Trained AS STRING) as ACADEMY_TRAINED",
    "CAST(Accident_Description_new AS STRING) as ACCIDENT_DESCRIPTION",
    "CAST(Accident_Time AS STRING) as ACCIDENT_TIME",
    "CAST(Activity_Engaged_In AS STRING) as ACTIVITY_ENGAGED_IN",
    "CAST(Animal_Care_Custody AS STRING) as ANIMAL_CARE_CUSTODY",
    "CAST(Animal_Inj_Type AS STRING) as ANIMAL_INJ_TYPE",
    "CAST(Animal_involved_in_the_incident AS STRING) as ANIMAL_INVOLVED_IN_THE_INCIDENT",
    "CAST(Associate_Name AS STRING) as ASSOCIATE_NAME",
    "CAST(Banfield_Amount AS STRING) as BANFIELD_AMOUNT",
    "CAST(Brand_Risk_Dept AS STRING) as BRAND_RISK_DEPT",
    "CAST(Brand_Risk_Escalation_Factors AS STRING) as BRAND_RISK_ESCALATION_FACTORS",
    "CAST(Cash_Amount AS STRING) as CASH_AMOUNT",
    "CAST(Cause AS STRING) as CAUSE",
    "CAST(Cause_General AS STRING) as CAUSE_GENERAL",
    "CAST(Cause_SRC AS STRING) as CAUSE_SRC",
    "CAST(Claim_SubStatus AS STRING) as CLAIM_SUBSTATUS",
    "CAST(Claimant_Length_Service AS STRING) as CLAIMANT_LENGTH_SERVICE",
    "CAST(Claimant_Name AS STRING) as CLAIMANT_NAME",
    "CAST(Comped_Products AS STRING) as COMPED_PRODUCTS",
    "CAST(Comped_Service_s AS STRING) as COMPED_SERVICE_S",
    "CAST(Coverage_Major AS STRING) as COVERAGE_MAJOR",
    "CAST(Date_Closed AS STRING) as DATE_CLOSED",
    "CAST(Date_Hired AS STRING) as DATE_HIRED",
    "CAST(Date_of_Loss AS STRING) as DATE_OF_LOSS",
    "CAST(Date_Opened AS STRING) as DATE_OPENED",
    "CAST(Date_Product_Purch AS STRING) as DATE_PRODUCT_PURCH",
    "CAST(Date_Reported_to_Client AS STRING) as DATE_REPORTED_TO_CLIENT",
    "CAST(Date_Safety_Certified AS STRING) as DATE_SAFETY_CERTIFIED",
    "CAST(Day_Camp_Capacity AS STRING) as DAY_CAMP_CAPACITY",
    "CAST(Department AS STRING) as DEPARTMENT",
    "CAST(Did_Associate_Go_to_Hospital AS STRING) as DID_ASSOCIATE_GO_TO_HOSPITAL",
    "CAST(Employee_Id AS STRING) as EMPLOYEE_ID",
    "CAST(Flowers AS STRING) as FLOWERS",
    "CAST(Full_Recovery_Expected AS STRING) as FULL_RECOVERY_EXPECTED",
    "CAST(Gift_Cards_s AS STRING) as GIFT_CARDS_S",
    "CAST(Has_the_pet_been_neutered_spayed AS STRING) as HAS_THE_PET_BEEN_NEUTERED_SPAYED",
    "CAST(Header_Code AS STRING) as HEADER_CODE",
    "CAST(Health_Chart_Completed_During_Stay AS STRING) as HEALTH_CHART_COMPLETED_DURING_STAY",
    "CAST(How_Did_Store_Find_Out AS STRING) as HOW_DID_STORE_FIND_OUT",
    "CAST(How_Many_Pets_in_Playtime AS STRING) as HOW_MANY_PETS_IN_PLAYTIME",
    "CAST(How_Many_Pets_in_Training_Class AS STRING) as HOW_MANY_PETS_IN_TRAINING_CLASS",
    "CAST(Human_Injury_Escalation AS STRING) as HUMAN_INJURY_ESCALATION",
    "CAST(Incident_Occur_During_PetSmart_Academy AS STRING) as INCIDENT_OCCUR_DURING_PETSMART_ACADEMY",
    "CAST(Injury_Illness_confirmed_by_Veterinarian AS STRING) as INJURY_ILLNESS_CONFIRMED_BY_VETERINARIAN",
    "CAST(Lost_Time_Indicator AS STRING) as LOST_TIME_INDICATOR",
    "CAST(Matted_Pet_Release AS STRING) as MATTED_PET_RELEASE",
    "CAST(Nature AS STRING) as NATURE",
    "CAST(Nature_General AS STRING) as NATURE_GENERAL",
    "CAST(Net_Incurred AS STRING) as NET_INCURRED",
    "CAST(Node_Code AS STRING) as NODE_CODE",
    "CAST(Other AS STRING) as OTHER",
    "CAST(Other_Policy_Violation AS STRING) as OTHER_POLICY_VIOLATION",
    "CAST(P_Card_Amount AS STRING) as P_CARD_AMOUNT",
    "CAST(Part AS STRING) as PART",
    "CAST(Part_General AS STRING) as PART_GENERAL",
    "CAST(Part_Position AS STRING) as PART_POSITION",
    "CAST(Pet_Age AS STRING) as PET_AGE",
    "CAST(Pet_Breed AS STRING) as PET_BREED",
    "CAST(Pet_Breed_Type AS STRING) as PET_BREED_TYPE",
    "CAST(Pet_Expense_Type AS STRING) as PET_EXPENSE_TYPE",
    "CAST(Pet_Gender AS STRING) as PET_GENDER",
    "CAST(Pet_Incident_Type AS STRING) as PET_INCIDENT_TYPE",
    "CAST(Pet_Origin AS STRING) as PET_ORIGIN",
    "CAST(Pet_Parent_Vet_Exp AS STRING) as PET_PARENT_VET_EXP",
    "CAST(Pet_Safety_Cause AS STRING) as PET_SAFETY_CAUSE",
    "CAST(Pet_Safety_Cause_General AS STRING) as PET_SAFETY_CAUSE_GENERAL",
    "CAST(Pet_Safety_Equip AS STRING) as PET_SAFETY_EQUIP",
    "CAST(Pet_Safety_Nature AS STRING) as PET_SAFETY_NATURE",
    "CAST(Pet_Safety_Nature_General AS STRING) as PET_SAFETY_NATURE_GENERAL",
    "CAST(Pet_Safety_Part AS STRING) as PET_SAFETY_PART",
    "CAST(Pet_Safety_Part_General AS STRING) as PET_SAFETY_PART_GENERAL",
    "CAST(Pet_Safety_Priority AS STRING) as PET_SAFETY_PRIORITY",
    "CAST(Pet_Symptoms AS STRING) as PET_SYMPTOMS",
    "CAST(Pet_Taken_to_Vet AS STRING) as PET_TAKEN_TO_VET",
    "CAST(Pet_UPC AS STRING) as PET_UPC",
    "CAST(Pet_Weight AS STRING) as PET_WEIGHT",
    "CAST(PetSmart_Incident_Type AS STRING) as PETSMART_INCIDENT_TYPE",
    "CAST(Policy_Violation AS STRING) as POLICY_VIOLATION",
    "CAST(Product_Best_By_Date AS STRING) as PRODUCT_BEST_BY_DATE",
    "CAST(Product_Lot_Code AS STRING) as PRODUCT_LOT_CODE",
    "CAST(Product_SKU_short_product_code AS STRING) as PRODUCT_SKU_SHORT_PRODUCT_CODE",
    "CAST(Product_UPC_long_product_code AS STRING) as PRODUCT_UPC_LONG_PRODUCT_CODE",
    "CAST(Reason_Code AS STRING) as REASON_CODE",
    "CAST(Service_Performed AS STRING) as SERVICE_PERFORMED",
    "CAST(Shave_Authorization AS STRING) as SHAVE_AUTHORIZATION",
    "CAST(Specific_Store_Area AS STRING) as SPECIFIC_STORE_AREA",
    "CAST(Srvc_Card_Signed AS STRING) as SRVC_CARD_SIGNED",
    "CAST(Status AS STRING) as STATUS",
    "CAST(Store_DC_Area AS STRING) as STORE_DC_AREA",
    "CAST(Time_Employee_Began_Work AS STRING) as TIME_EMPLOYEE_BEGAN_WORK",
    "CAST(TPA_Carrier AS STRING) as TPA_CARRIER",
    "CAST(TPA_Claim_Number AS STRING) as TPA_CLAIM_NUMBER",
    "CAST(Treatment_Intent AS STRING) as TREATMENT_INTENT",
    "CAST(Was_911_Called AS STRING) as WAS_911_CALLED",
    "CAST(Was_Associate_Admitted_to_Hospital AS STRING) as WAS_ASSOCIATE_ADMITTED_TO_HOSPITAL",
    "CAST(Was_Associate_Transported_by_Ambulance AS STRING) as WAS_ASSOCIATE_TRANSPORTED_BY_AMBULANCE",
    "CAST(What_Point_did_the_Incident_Occur AS STRING) as WHAT_POINT_DID_THE_INCIDENT_OCCUR",
    "CAST(What_Point_During_Service_Occur_Hotel AS STRING) as WHAT_POINT_DURING_SERVICE_OCCUR_HOTEL",
    "CAST(Work_Related AS STRING) as WORK_RELATED",
    "CAST(Type_of_Incident_Reported AS STRING) as TYPE_OF_INCIDENT_REPORTED",
    "CAST(Scorecard_Exception_Flag AS STRING) as SCORECARD_EXCEPTION_FLAG",
    "CAST(Responsible_Associate_ID AS STRING) as RESPONSIBLE_ASSOCIATE_ID",
    "CAST(Check_Amount AS STRING) as CHECK_AMOUNT",
    "CAST(Claim_Type_SRC AS STRING) as CLAIM_TYPE_SRC",
    "CAST(Cause_Legacy AS STRING) as CAUSE_LEGACY",
    "CAST(Reason_Legacy AS STRING) as REASON_LEGACY",
    "CAST(Date_Reported_to_TPA AS STRING) as DATE_REPORTED_TO_TPA",
    "CAST(Adoption_Group AS STRING) as ADOPTION_GROUP",
    "CAST(Closed_Issue_Date AS STRING) as CLOSED_ISSUE_DATE",
    "CAST(Employee_Job_Title AS STRING) as EMPLOYEE_JOB_TITLE",
    "CAST(Received AS STRING) as RECEIVED_DATE",
    "CAST(Responded AS STRING) as RESPONDED_DATE",
    "CAST(Response_Due AS STRING) as RESPONSE_DUE_DATE",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)

Shortcut_to_INCIDENT_CLAIMS_PRE.write.mode("overwrite").saveAsTable(
    f"{raw}.INCIDENT_CLAIMS_PRE"
)
