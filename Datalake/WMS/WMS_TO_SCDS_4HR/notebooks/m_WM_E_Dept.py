from logging import getLogger, INFO
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import (
    col,
    lit,
    when,
    current_timestamp,
    monotonically_increasing_id,
)
from pyspark.sql.types import DecimalType, TimestampType, StringType
from pyspark.sql.session import SparkSession
from Datalake.utils.genericUtilities import getEnvPrefix
from Datalake.utils.logger import logPrevRunDt
from Datalake.utils.mergeUtils import executeMerge
import argparse

parser = argparse.ArgumentParser()

spark: SparkSession = SparkSession.getActiveSession()
dbutils: DBUtils = DBUtils(spark)


parser.add_argument("env", type=str, help="Env Variable")
args = parser.parse_args()
env = args.env
# env = dbutils.widgets.get('env')

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"

pre_dept_table = f"{raw}.WM_E_DEPT_PRE"
refined_dept_table = f"{refine}.WM_E_DEPT"
site_profile_table = f"{legacy}.SITE_PROFILE"

logger = getLogger()
logger.setLevel(INFO)

dept_pre_query = (
    """SELECT
WM_E_DEPT_PRE.DC_NBR,
WM_E_DEPT_PRE.DEPT_ID,
WM_E_DEPT_PRE.DEPT_CODE,
WM_E_DEPT_PRE.DESCRIPTION,
WM_E_DEPT_PRE.CREATE_DATE_TIME,
WM_E_DEPT_PRE.MOD_DATE_TIME,
WM_E_DEPT_PRE.USER_ID,
WM_E_DEPT_PRE.WHSE,
WM_E_DEPT_PRE.MISC_TXT_1,
WM_E_DEPT_PRE.MISC_TXT_2,
WM_E_DEPT_PRE.MISC_NUM_1,
WM_E_DEPT_PRE.MISC_NUM_2,
WM_E_DEPT_PRE.PERF_GOAL,
WM_E_DEPT_PRE.VERSION_ID,
WM_E_DEPT_PRE.CREATED_DTTM,
WM_E_DEPT_PRE.LAST_UPDATED_DTTM,
WM_E_DEPT_PRE.LOAD_TSTMP
FROM """
    + pre_dept_table
)

SQ_Shortcut_to_WM_E_DEPT_PRE = spark.sql(dept_pre_query).withColumn(
    "sys_row_id", monotonically_increasing_id()
)
logger.info("Query to extract data from" + pre_dept_table + " executed successfully")

dept_query = (
    """SELECT
WM_E_DEPT.LOCATION_ID,
WM_E_DEPT.WM_DEPT_ID,
WM_E_DEPT.WM_CREATED_TSTMP,
WM_E_DEPT.WM_LAST_UPDATED_TSTMP,
WM_E_DEPT.WM_CREATE_TSTMP,
WM_E_DEPT.WM_MOD_TSTMP,
WM_E_DEPT.LOAD_TSTMP
FROM """
    + refined_dept_table
    + """
WHERE WM_DEPT_ID IN (SELECT DEPT_ID FROM """
    + pre_dept_table
    + """)"""
)


SQ_Shortcut_to_WM_E_DEPT = spark.sql(dept_query).withColumn(
    "sys_row_id", monotonically_increasing_id()
)
logger.info("SQ_Shortcut_to_WM_E_DEPT created successfully")

EXP_INT_CONV = SQ_Shortcut_to_WM_E_DEPT_PRE.select(
    SQ_Shortcut_to_WM_E_DEPT_PRE.sys_row_id.alias("sys_row_id"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.DC_NBR.alias("in_DC_NBR"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.DEPT_ID.alias("DEPT_ID"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.DEPT_CODE.alias("DEPT_CODE"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.DESCRIPTION.alias("DESCRIPTION"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.CREATE_DATE_TIME.alias("CREATE_DATE_TIME"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.MOD_DATE_TIME.alias("MOD_DATE_TIME"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.USER_ID.alias("USER_ID"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.WHSE.alias("WHSE"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.MISC_TXT_1.alias("MISC_TXT_1"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.MISC_TXT_2.alias("MISC_TXT_2"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.MISC_NUM_1.alias("MISC_NUM_1"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.MISC_NUM_2.alias("MISC_NUM_2"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.PERF_GOAL.alias("PERF_GOAL"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.VERSION_ID.alias("VERSION_ID"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.CREATED_DTTM.alias("CREATED_DTTM"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.LAST_UPDATED_DTTM.alias("LAST_UPDATED_DTTM"),
    SQ_Shortcut_to_WM_E_DEPT_PRE.LOAD_TSTMP.alias("LOAD_TSTMP"),
).select(
    (col("sys_row_id")).alias("sys_row_id"),
    ((col("in_DC_NBR").cast(DecimalType(3, 0)))).alias("DC_NBR"),
    col("DEPT_ID"),
    col("DEPT_CODE"),
    col("DESCRIPTION"),
    col("CREATE_DATE_TIME"),
    col("MOD_DATE_TIME"),
    col("USER_ID"),
    col("WHSE"),
    col("MISC_TXT_1"),
    col("MISC_TXT_2"),
    col("MISC_NUM_1"),
    col("MISC_NUM_2"),
    col("PERF_GOAL"),
    col("VERSION_ID"),
    col("CREATED_DTTM"),
    col("LAST_UPDATED_DTTM"),
    col("LOAD_TSTMP"),
)
logger.info("EXP_INT_CONV created successfully")

site_profile = f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM {site_profile_table}"""

SQ_Shortcut_to_SITE_PROFILE = spark.sql(site_profile).withColumn(
    "sys_row_id", monotonically_increasing_id()
)
logger.info("Site profile table query executed successfully!")

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(
    EXP_INT_CONV,
    [SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.DC_NBR],
    "inner",
)
logger.info("JNR_SITE_PROFILE dataframe created successfully")

JNR_WM_E_DEPT = SQ_Shortcut_to_WM_E_DEPT.join(
    JNR_SITE_PROFILE,
    [
        SQ_Shortcut_to_WM_E_DEPT.LOCATION_ID == JNR_SITE_PROFILE.LOCATION_ID,
        SQ_Shortcut_to_WM_E_DEPT.WM_DEPT_ID == JNR_SITE_PROFILE.DEPT_ID,
    ],
    "right_outer",
).select(
    SQ_Shortcut_to_WM_E_DEPT.sys_row_id.alias("sys_row_id"),
    JNR_SITE_PROFILE.LOCATION_ID.alias("LOCATION_ID"),
    JNR_SITE_PROFILE.DEPT_ID.alias("DEPT_ID"),
    JNR_SITE_PROFILE.DEPT_CODE.alias("DEPT_CODE"),
    JNR_SITE_PROFILE.DESCRIPTION.alias("DESCRIPTION"),
    JNR_SITE_PROFILE.CREATE_DATE_TIME.alias("CREATE_DATE_TIME"),
    JNR_SITE_PROFILE.MOD_DATE_TIME.alias("MOD_DATE_TIME"),
    JNR_SITE_PROFILE.USER_ID.alias("USER_ID"),
    JNR_SITE_PROFILE.WHSE.alias("WHSE"),
    JNR_SITE_PROFILE.MISC_TXT_1.alias("MISC_TXT_1"),
    JNR_SITE_PROFILE.MISC_TXT_2.alias("MISC_TXT_2"),
    JNR_SITE_PROFILE.MISC_NUM_1.alias("MISC_NUM_1"),
    JNR_SITE_PROFILE.MISC_NUM_2.alias("MISC_NUM_2"),
    JNR_SITE_PROFILE.PERF_GOAL.alias("PERF_GOAL"),
    JNR_SITE_PROFILE.VERSION_ID.alias("VERSION_ID"),
    JNR_SITE_PROFILE.CREATED_DTTM.alias("CREATED_DTTM"),
    JNR_SITE_PROFILE.LAST_UPDATED_DTTM.alias("LAST_UPDATED_DTTM"),
    JNR_SITE_PROFILE.LOAD_TSTMP.alias("LOAD_TSTMP"),
    SQ_Shortcut_to_WM_E_DEPT.LOCATION_ID.alias("in_LOCATION_ID"),
    SQ_Shortcut_to_WM_E_DEPT.WM_DEPT_ID.alias("in_WM_DEPT_ID"),
    SQ_Shortcut_to_WM_E_DEPT.LOAD_TSTMP.alias("in_LOAD_TSTMP"),
    SQ_Shortcut_to_WM_E_DEPT.WM_CREATE_TSTMP.alias("in_WM_CREATE_TSTMP"),
    SQ_Shortcut_to_WM_E_DEPT.WM_MOD_TSTMP.alias("in_WM_MOD_TSTMP"),
    SQ_Shortcut_to_WM_E_DEPT.WM_CREATED_TSTMP.alias("in_WM_CREATED_TSTMP"),
    SQ_Shortcut_to_WM_E_DEPT.WM_LAST_UPDATED_TSTMP.alias("in_WM_LAST_UPDATED_TSTMP"),
)

logger.info("JNR_WM_E_DEPT dataframe created successfully")

FIL_NO_CHANGE_REC = (
    JNR_WM_E_DEPT.select(
        JNR_WM_E_DEPT.LOCATION_ID.alias("LOCATION_ID"),
        JNR_WM_E_DEPT.DEPT_ID.alias("DEPT_ID"),
        JNR_WM_E_DEPT.DEPT_CODE.alias("DEPT_CODE"),
        JNR_WM_E_DEPT.DESCRIPTION.alias("DESCRIPTION"),
        JNR_WM_E_DEPT.CREATE_DATE_TIME.alias("CREATE_DATE_TIME"),
        JNR_WM_E_DEPT.MOD_DATE_TIME.alias("MOD_DATE_TIME"),
        JNR_WM_E_DEPT.USER_ID.alias("USER_ID"),
        JNR_WM_E_DEPT.WHSE.alias("WHSE"),
        JNR_WM_E_DEPT.MISC_TXT_1.alias("MISC_TXT_1"),
        JNR_WM_E_DEPT.MISC_TXT_2.alias("MISC_TXT_2"),
        JNR_WM_E_DEPT.MISC_NUM_1.alias("MISC_NUM_1"),
        JNR_WM_E_DEPT.MISC_NUM_2.alias("MISC_NUM_2"),
        JNR_WM_E_DEPT.PERF_GOAL.alias("PERF_GOAL"),
        JNR_WM_E_DEPT.VERSION_ID.alias("VERSION_ID"),
        JNR_WM_E_DEPT.CREATED_DTTM.alias("CREATED_DTTM"),
        JNR_WM_E_DEPT.LAST_UPDATED_DTTM.alias("LAST_UPDATED_DTTM"),
        JNR_WM_E_DEPT.in_WM_DEPT_ID.alias("in_WM_DEPT_ID"),
        JNR_WM_E_DEPT.in_LOAD_TSTMP.alias("in_LOAD_TSTMP"),
        JNR_WM_E_DEPT.in_WM_CREATE_TSTMP.alias("in_WM_CREATE_TSTMP"),
        JNR_WM_E_DEPT.in_WM_MOD_TSTMP.alias("in_WM_MOD_TSTMP"),
        JNR_WM_E_DEPT.in_WM_CREATED_TSTMP.alias("in_WM_CREATED_TSTMP"),
        JNR_WM_E_DEPT.in_WM_LAST_UPDATED_TSTMP.alias("in_WM_LAST_UPDATED_TSTMP"),
    )
    .filter(
        """(in_WM_DEPT_ID is null)
        OR ( NOT (in_WM_DEPT_ID is null)
        AND (((case when CREATE_DATE_TIME is null
        then TO_DATE('01/01/1900','M/d/y') else CREATE_DATE_TIME end )
        != (case when in_WM_CREATE_TSTMP is null
         then TO_DATE ('01/01/1900','M/d/y')
        else in_WM_CREATE_TSTMP end))OR(( case when MOD_DATE_TIME is null
         then TO_DATE ('01/01/1900' , 'M/d/y')
        else MOD_DATE_TIME end) != (case when in_WM_MOD_TSTMP is null
         then TO_DATE ( '01/01/1900' , 'M/d/y' )
        else in_WM_MOD_TSTMP end))
        OR((case when CREATED_DTTM  is null then TO_DATE ( '01/01/1900' , 'M/d/y' )
        else CREATED_DTTM end) != (case when in_WM_CREATED_TSTMP is null
         then TO_DATE ( '01/01/1900' , 'M/d/y' )
        else in_WM_CREATED_TSTMP end))OR((case when LAST_UPDATED_DTTM is null
         then TO_DATE( '01/01/1900' , 'M/d/y' )
        else LAST_UPDATED_DTTM end) != (case when in_WM_LAST_UPDATED_TSTMP is null
         then TO_DATE ( '01/01/1900' , 'M/d/y')
        else  in_WM_LAST_UPDATED_TSTMP end) ) ))"""
    )
    .withColumn("sys_row_id", monotonically_increasing_id())
)

logger.info("FIL_NO_CHANGE_REC dataframe created successfully")

EXP_EVAL_VALUES = FIL_NO_CHANGE_REC.select(
    FIL_NO_CHANGE_REC.sys_row_id.alias("sys_row_id"),
    FIL_NO_CHANGE_REC.LOCATION_ID.alias("LOCATION_ID"),
    FIL_NO_CHANGE_REC.DEPT_ID.alias("DEPT_ID"),
    FIL_NO_CHANGE_REC.DEPT_CODE.alias("DEPT_CODE"),
    FIL_NO_CHANGE_REC.DESCRIPTION.alias("DESCRIPTION"),
    FIL_NO_CHANGE_REC.CREATE_DATE_TIME.alias("CREATE_DATE_TIME"),
    FIL_NO_CHANGE_REC.MOD_DATE_TIME.alias("MOD_DATE_TIME"),
    FIL_NO_CHANGE_REC.USER_ID.alias("USER_ID"),
    FIL_NO_CHANGE_REC.WHSE.alias("WHSE"),
    FIL_NO_CHANGE_REC.MISC_TXT_1.alias("MISC_TXT_1"),
    FIL_NO_CHANGE_REC.MISC_TXT_2.alias("MISC_TXT_2"),
    FIL_NO_CHANGE_REC.MISC_NUM_1.alias("MISC_NUM_1"),
    FIL_NO_CHANGE_REC.MISC_NUM_2.alias("MISC_NUM_2"),
    FIL_NO_CHANGE_REC.PERF_GOAL.alias("PERF_GOAL"),
    FIL_NO_CHANGE_REC.VERSION_ID.alias("VERSION_ID"),
    FIL_NO_CHANGE_REC.CREATED_DTTM.alias("CREATED_DTTM"),
    FIL_NO_CHANGE_REC.LAST_UPDATED_DTTM.alias("LAST_UPDATED_DTTM"),
    (
        when((col("in_LOAD_TSTMP").isNull()), (current_timestamp())).otherwise(
            col("in_LOAD_TSTMP")
        )
    ).alias("LOAD_TSTMP"),
    (current_timestamp()).alias("UPDATE_TSTMP"),
    FIL_NO_CHANGE_REC.in_WM_DEPT_ID.alias("in_WM_DEPT_ID"),
)

logger.info("EXP_EVAL_VALUES dataframe created successfully")

UPD_VALIDATE = EXP_EVAL_VALUES.select(
    EXP_EVAL_VALUES.LOCATION_ID.alias("LOCATION_ID"),
    EXP_EVAL_VALUES.DEPT_ID.alias("DEPT_ID"),
    EXP_EVAL_VALUES.DEPT_CODE.alias("DEPT_CODE"),
    EXP_EVAL_VALUES.DESCRIPTION.alias("DESCRIPTION"),
    EXP_EVAL_VALUES.CREATE_DATE_TIME.alias("CREATE_DATE_TIME"),
    EXP_EVAL_VALUES.MOD_DATE_TIME.alias("MOD_DATE_TIME"),
    EXP_EVAL_VALUES.USER_ID.alias("USER_ID"),
    EXP_EVAL_VALUES.WHSE.alias("WHSE"),
    EXP_EVAL_VALUES.MISC_TXT_1.alias("MISC_TXT_1"),
    EXP_EVAL_VALUES.MISC_TXT_2.alias("MISC_TXT_2"),
    EXP_EVAL_VALUES.MISC_NUM_1.alias("MISC_NUM_1"),
    EXP_EVAL_VALUES.MISC_NUM_2.alias("MISC_NUM_2"),
    EXP_EVAL_VALUES.PERF_GOAL.alias("PERF_GOAL"),
    EXP_EVAL_VALUES.VERSION_ID.alias("VERSION_ID"),
    EXP_EVAL_VALUES.CREATED_DTTM.alias("CREATED_DTTM"),
    EXP_EVAL_VALUES.LAST_UPDATED_DTTM.alias("LAST_UPDATED_DTTM"),
    EXP_EVAL_VALUES.LOAD_TSTMP.alias("LOAD_TSTMP"),
    EXP_EVAL_VALUES.UPDATE_TSTMP.alias("UPDATE_TSTMP"),
    EXP_EVAL_VALUES.in_WM_DEPT_ID.alias("in_WM_DEPT_ID"),
)

UPD_VALIDATE = UPD_VALIDATE.withColumn(
    "pyspark_data_action",
    when((UPD_VALIDATE.in_WM_DEPT_ID.isNull()), (lit(0))).otherwise(lit(1)),
)

logger.info("UPD_VALIDATE dataframe created successfully")

Shortcut_to_WM_E_DEPT = UPD_VALIDATE.select(
    UPD_VALIDATE.LOCATION_ID.cast(DecimalType(10, 0)).alias("LOCATION_ID"),
    UPD_VALIDATE.DEPT_ID.cast(DecimalType(9, 0)).alias("WM_DEPT_ID"),
    UPD_VALIDATE.WHSE.cast(StringType()).alias("WM_WHSE"),
    UPD_VALIDATE.DEPT_CODE.cast(StringType()).alias("WM_DEPT_CD"),
    UPD_VALIDATE.DESCRIPTION.cast(StringType()).alias("WM_DEPT_DESC"),
    UPD_VALIDATE.PERF_GOAL.cast(DecimalType(9, 2)).alias("PERF_GOAL"),
    UPD_VALIDATE.MISC_TXT_1.cast(StringType()).alias("MISC_TXT_1"),
    UPD_VALIDATE.MISC_TXT_2.cast(StringType()).alias("MISC_TXT_2"),
    UPD_VALIDATE.MISC_NUM_1.cast(DecimalType(20, 7)).alias("MISC_NUM_1"),
    UPD_VALIDATE.MISC_NUM_2.cast(DecimalType(20, 7)).alias("MISC_NUM_2"),
    UPD_VALIDATE.USER_ID.cast(StringType()).alias("WM_USER_ID"),
    UPD_VALIDATE.VERSION_ID.cast(DecimalType(6, 0)).alias("WM_VERSION_ID"),
    UPD_VALIDATE.CREATED_DTTM.cast(TimestampType()).alias("WM_CREATED_TSTMP"),
    UPD_VALIDATE.LAST_UPDATED_DTTM.cast(TimestampType()).alias("WM_LAST_UPDATED_TSTMP"),
    UPD_VALIDATE.CREATE_DATE_TIME.cast(TimestampType()).alias("WM_CREATE_TSTMP"),
    UPD_VALIDATE.MOD_DATE_TIME.cast(TimestampType()).alias("WM_MOD_TSTMP"),
    UPD_VALIDATE.LOAD_TSTMP.cast(TimestampType()).alias("UPDATE_TSTMP"),
    UPD_VALIDATE.UPDATE_TSTMP.cast(TimestampType()).alias("LOAD_TSTMP"),
    UPD_VALIDATE.pyspark_data_action.alias("pyspark_data_action"),
)
logger.info("Shortcut_to_WM_E_DEPT dataframe created successfully")

try:
    primary_key = "source.LOCATION_ID = target.LOCATION_ID AND source.WM_DEPT_ID = target.WM_DEPT_ID"
    executeMerge(Shortcut_to_WM_E_DEPT, refined_dept_table, primary_key)
    logger.info("Merge with" + refined_dept_table + "completed")
    logPrevRunDt("WM_E_DEPT", "WM_E_DEPT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
    logPrevRunDt("WM_E_DEPT", "WM_E_DEPT", "Failed", str(e), f"{raw}.log_run_details")
    raise e
