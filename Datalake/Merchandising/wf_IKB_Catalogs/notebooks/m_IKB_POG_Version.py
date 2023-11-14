# Databricks notebook source
# Code converted on 2023-08-30 11:26:01
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

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

(username, password, connection_string) = ckb_prd_sqlServer(env)

# COMMAND ----------

# Processing node LKP_POG_FIXTURE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_POG_FIXTURE_SRC = spark.sql(f"""SELECT MIN(POG_FIXTURE.FIXTURE_TYPE) as FIXTURE_TYPE, RTRIM(LTRIM(POG_FIXTURE.FIXTURE_DESC)) as FIXTURE_DESC FROM {legacy}.POG_FIXTURE GROUP BY RTRIM(LTRIM(POG_FIXTURE.FIXTURE_DESC))""")
# Conforming fields names to the component layout
LKP_POG_FIXTURE_SRC = LKP_POG_FIXTURE_SRC \
	.withColumnRenamed(LKP_POG_FIXTURE_SRC.columns[0],'FIXTURE_TYPE')

# COMMAND ----------

# Processing node SQ_Shortcut_to_ix_spc_planogram, type SOURCE 
# COLUMN COUNT: 59

SQ_Shortcut_to_ix_spc_planogram = jdbcSqlServerConnection(f"""(SELECT
ix_spc_planogram.DBTime,
ix_spc_planogram.DBUser,
ix_spc_planogram.DBStatus,
ix_spc_planogram.DBDateEffectiveFrom,
ix_spc_planogram.DBKey,
ix_spc_planogram.Width,
ix_spc_planogram.Height,
ix_spc_planogram.Depth,
ix_spc_planogram.Desc2,
ix_spc_planogram.Desc3,
ix_spc_planogram.Desc4,
ix_spc_planogram.Desc6,
ix_spc_planogram.Desc7,
ix_spc_planogram.Desc8,
ix_spc_planogram.Desc11,
ix_spc_planogram.Desc12,
ix_spc_planogram.Desc14,
ix_spc_planogram.Desc16,
ix_spc_planogram.Desc18,
ix_spc_planogram.Desc19,
ix_spc_planogram.Desc20,
ix_spc_planogram.Desc21,
ix_spc_planogram.Desc22,
ix_spc_planogram.Desc46,
ix_spc_planogram.Desc47,
ix_spc_planogram.Desc48,
ix_spc_planogram.Desc49,
ix_spc_planogram.Value2,
ix_spc_planogram.Value6,
ix_spc_planogram.Value25,
ix_spc_planogram.Value26,
ix_spc_planogram.Value27,
ix_spc_planogram.Value28,
ix_spc_planogram.Flag1,
ix_spc_planogram.Flag2,
ix_spc_planogram.Flag3,
ix_spc_planogram.Flag4,
ix_spc_planogram.Flag5,
ix_spc_planogram.Flag6,
ix_spc_planogram.Flag7,
ix_spc_planogram.Flag10,
ix_spc_planogram.DateCreated,
ix_spc_planogram.DateModified,
ix_spc_planogram.DatePending,
ix_spc_planogram.DateEffective,
ix_spc_planogram.Date1,
ix_spc_planogram.Date2,
ix_spc_planogram.DBDateEffectiveTo,
ix_spc_planogram.DBVersionKey,
ix_spc_planogram.PGStatus,
ix_spc_planogram.PGScorePercent,
ix_spc_planogram.PGScoreNote,
ix_spc_planogram.PGWarningsCount,
ix_spc_planogram.PGErrorsCount,
ix_spc_planogram.PGActionList,
ix_spc_planogram.DBParentPGTemplateKey,
ix_spc_planogram.AbbrevName,
ix_spc_planogram.AllocationGroup,
ix_spc_planogram.AllocationSequence,
inlinepoggroups.groupid,
plannerpoggroups.plannerpoggroupid
FROM CKB_PRD.dbo.ix_spc_planogram
LEFT OUTER JOIN (SELECT description, max(groupid) groupid from CKB_PRD.space_ddd.inlinepoggroups group by description) inlinepoggroups on ix_spc_planogram.Desc6 = inlinepoggroups.description
LEFT OUTER JOIN (SELECT description, max(plannerpoggroupid) plannerpoggroupid from CKB_PRD.space_ddd.plannerpoggroups group by description) plannerpoggroups on ix_spc_planogram.Desc6 = plannerpoggroups.description) src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_POG_VERSION, type SOURCE 
# COLUMN COUNT: 62

SQ_Shortcut_to_POG_VERSION = spark.sql(f"""SELECT
POG_VERSION.POG_DBKEY as POG_DB_KEY ,
POG_VERSION.POG_NM,
POG_VERSION.POG_EFFECTIVE_FROM_DT,
POG_VERSION.POG_EFFECTIVE_TO_DT,
POG_VERSION.DB_STATUS,
POG_VERSION.DB_TIME,
POG_VERSION.DB_USER,
POG_VERSION.POG_IMPLEMENT_DT,
POG_VERSION.POG_REVIEW_DT,
POG_VERSION.US_FLAG,
POG_VERSION.CA_FLAG,
POG_VERSION.PR_FLAG,
POG_VERSION.POG_TYPE_CD,
POG_VERSION.POG_DIVISION,
POG_VERSION.POG_DEPT,
POG_VERSION.WIDTH,
POG_VERSION.HEIGHT,
POG_VERSION.DEPTH,
POG_VERSION.ABBREV_NM,
POG_VERSION.DATE_CREATED,
POG_VERSION.DATE_MODIFIED,
POG_VERSION.DATE_PENDING,
POG_VERSION.DATE_EFFECTIVE,
POG_VERSION.POG_GROUP_DESC as POG_GROUP ,
POG_VERSION.PG_STATUS,
POG_VERSION.PG_SCORE_PERCENT,
POG_VERSION.PG_SCORE_NOTE,
POG_VERSION.DB_VERSION_KEY,
POG_VERSION.PG_WARNINGS_COUNT,
POG_VERSION.PG_ERRORS_COUNT,
POG_VERSION.PG_ACTION_LIST,
POG_VERSION.ALLOCATION_GROUP,
POG_VERSION.ALLOCATION_SEQUENCE,
POG_VERSION.DBPARENT_PGTEMPLATE_KEY,
POG_VERSION.LISTING_ONLY_FLAG,
POG_VERSION.NEW_STORE_FLAG,
POG_VERSION.NOT_GOING_FOWARD_FLAG,
POG_VERSION.PLANNER_PRINT_FLAG,
POG_VERSION.OBSOLETE_FLAG,
POG_VERSION.FIXTURE_TYPE_ID,
POG_VERSION.FIXTURE_TYPE_DESC as FIXTURE_TYPE ,
POG_VERSION.DISPLAY_LOCATION,
POG_VERSION.CATEGORY_ROLE,
POG_VERSION.POG_CHANGE_TYPE,
POG_VERSION.CLUSTER_NBR,
POG_VERSION.ASO,
POG_VERSION.POG_VERSION_REASON as VERSION_REASON ,
POG_VERSION.POG_VERSION_COMMENTS as VERSION_COMMENTS ,
POG_VERSION.DRIVE_AISLE,
POG_VERSION.CLUSTER_NM,
POG_VERSION.PRESENTATION_MGR_NM,
POG_VERSION.PLANNER_DESC,
POG_VERSION.PLANNER_YEAR,
POG_VERSION.POG_GROUP_ID as GRPID ,
POG_VERSION.COMBO1,
POG_VERSION.COMBO2,
POG_VERSION.COMBO3,
POG_VERSION.PG_SCENARIO_NM,
POG_VERSION.PG_EQUIPEMENT_SOURCE,
POG_VERSION.PG_ASSORTMENT_SOURCE,
POG_VERSION.PG_PERFORMANCE_SOURCE,
POG_VERSION.LOAD_DT
FROM {legacy}.POG_VERSION""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_target, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 122

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_POG_VERSION_temp = SQ_Shortcut_to_POG_VERSION.toDF(*["SQ_Shortcut_to_POG_VERSION___" + col for col in SQ_Shortcut_to_POG_VERSION.columns])
SQ_Shortcut_to_ix_spc_planogram_temp = SQ_Shortcut_to_ix_spc_planogram.toDF(*["SQ_Shortcut_to_ix_spc_planogram___" + col for col in SQ_Shortcut_to_ix_spc_planogram.columns])

JNR_target = SQ_Shortcut_to_POG_VERSION_temp.join(SQ_Shortcut_to_ix_spc_planogram_temp,[SQ_Shortcut_to_POG_VERSION_temp.SQ_Shortcut_to_POG_VERSION___POG_DB_KEY == SQ_Shortcut_to_ix_spc_planogram_temp.SQ_Shortcut_to_ix_spc_planogram___DBKey],'right_outer').selectExpr(
	"SQ_Shortcut_to_ix_spc_planogram___DBKey as POG_DB_KEY",
	"SQ_Shortcut_to_ix_spc_planogram___AbbrevName as POG_NM",
	"SQ_Shortcut_to_ix_spc_planogram___DBDateEffectiveFrom as POG_EFFECTIVE_FROM_DT",
	"SQ_Shortcut_to_ix_spc_planogram___DBDateEffectiveTo as POG_EFFECTIVE_TO_DT",
	"SQ_Shortcut_to_ix_spc_planogram___DBStatus as DB_STATUS",
	"SQ_Shortcut_to_ix_spc_planogram___DBTime as DB_TIME",
	"SQ_Shortcut_to_ix_spc_planogram___DBUser as DB_USER",
	"SQ_Shortcut_to_ix_spc_planogram___Date2 as POG_IMPLEMENT_DT",
	"SQ_Shortcut_to_ix_spc_planogram___Date1 as POG_REVIEW_DT",
	"SQ_Shortcut_to_ix_spc_planogram___Flag1 as US_FLAG",
	"SQ_Shortcut_to_ix_spc_planogram___Flag2 as CA_FLAG",
	"SQ_Shortcut_to_ix_spc_planogram___Flag3 as PR_FLAG",
	"SQ_Shortcut_to_ix_spc_planogram___Desc2 as Flag1",
	"SQ_Shortcut_to_ix_spc_planogram___Desc3 as POG_DIVISION",
	"SQ_Shortcut_to_ix_spc_planogram___Desc4 as POG_DEPT",
	"round(SQ_Shortcut_to_ix_spc_planogram___Width,2) as WIDTH",
	"round(SQ_Shortcut_to_ix_spc_planogram___Height,2) as HEIGHT",
	"round(SQ_Shortcut_to_ix_spc_planogram___Depth,2) as DEPTH",
	"SQ_Shortcut_to_ix_spc_planogram___AbbrevName as ABBREV_NM",
	"SQ_Shortcut_to_ix_spc_planogram___DateCreated as DATE_CREATED",
	"SQ_Shortcut_to_ix_spc_planogram___DateModified as DATE_MODIFIED",
	"SQ_Shortcut_to_ix_spc_planogram___DatePending as DATE_PENDING",
	"SQ_Shortcut_to_ix_spc_planogram___DateEffective as DATE_EFFECTIVE",
	"SQ_Shortcut_to_ix_spc_planogram___Desc6 as POG_GROUP",
	"SQ_Shortcut_to_ix_spc_planogram___PGStatus as PG_STATUS",
	"SQ_Shortcut_to_ix_spc_planogram___PGScorePercent as PG_SCORE_PERCENT",
	"SQ_Shortcut_to_ix_spc_planogram___PGScoreNote as PG_SCORE_NOTE",
	"SQ_Shortcut_to_ix_spc_planogram___DBVersionKey as DB_VERSION_KEY",
	"SQ_Shortcut_to_ix_spc_planogram___PGWarningsCount as PG_WARNINGS_COUNT",
	"SQ_Shortcut_to_ix_spc_planogram___PGErrorsCount as PG_ERRORS_COUNT",
	"SQ_Shortcut_to_ix_spc_planogram___PGActionList as PG_ACTION_LIST",
	"SQ_Shortcut_to_ix_spc_planogram___AllocationGroup as ALLOCATION_GROUP",
	"SQ_Shortcut_to_ix_spc_planogram___AllocationSequence as ALLOCATION_SEQUENCE",
	"SQ_Shortcut_to_ix_spc_planogram___DBParentPGTemplateKey as DBPARENT_PGTEMPLATE_KEY",
	"SQ_Shortcut_to_ix_spc_planogram___Flag4 as LISTING_ONLY_FLAG",
	"SQ_Shortcut_to_ix_spc_planogram___Flag5 as NEW_STORE_FLAG",
	"SQ_Shortcut_to_ix_spc_planogram___Flag6 as NOT_GOING_FOWARD_FLAG",
	"SQ_Shortcut_to_ix_spc_planogram___Flag7 as PLANNER_PRINT_FLAG",
	"SQ_Shortcut_to_ix_spc_planogram___Flag10 as OBSOLETE_FLAG",
	"SQ_Shortcut_to_POG_VERSION___FIXTURE_TYPE_ID as FIXTURE_TYPE_ID",
	"SQ_Shortcut_to_ix_spc_planogram___Desc7 as FIXTURE_TYPE",
	"SQ_Shortcut_to_ix_spc_planogram___Desc18 as DISPLAY_LOCATION",
	"SQ_Shortcut_to_ix_spc_planogram___Desc14 as CATEGORY_ROLE",
	"SQ_Shortcut_to_ix_spc_planogram___Desc12 as POG_CHANGE_TYPE",
	"SQ_Shortcut_to_ix_spc_planogram___Value2 as CLUSTER_NBR",
	"SQ_Shortcut_to_ix_spc_planogram___Value6 as ASO",
	"SQ_Shortcut_to_ix_spc_planogram___Desc21 as VERSION_REASON",
	"SQ_Shortcut_to_ix_spc_planogram___Desc11 as VERSION_COMMENTS",
	"SQ_Shortcut_to_ix_spc_planogram___Desc22 as DRIVE_AISLE",
	"SQ_Shortcut_to_ix_spc_planogram___Desc8 as CLUSTER_NM",
	"SQ_Shortcut_to_ix_spc_planogram___Desc16 as PRESENTATION_MGR_NM",
	"SQ_Shortcut_to_ix_spc_planogram___Desc19 as PLANNER_DESC",
	"SQ_Shortcut_to_ix_spc_planogram___Desc20 as PLANNER_YEAR",
	"SQ_Shortcut_to_ix_spc_planogram___Value25 as GRPID",
	"SQ_Shortcut_to_ix_spc_planogram___Value26 as COMBO1",
	"SQ_Shortcut_to_ix_spc_planogram___Value27 as COMBO2",
	"SQ_Shortcut_to_ix_spc_planogram___Value28 as COMBO3",
	"SQ_Shortcut_to_ix_spc_planogram___Desc46 as PG_SCENARIO_NM",
	"SQ_Shortcut_to_ix_spc_planogram___Desc47 as PG_EQUIPEMENT_SOURCE",
	"SQ_Shortcut_to_ix_spc_planogram___Desc48 as PG_ASSORTMENT_SOURCE",
	"SQ_Shortcut_to_ix_spc_planogram___Desc49 as PG_PERFORMANCE_SOURCE",
	"SQ_Shortcut_to_ix_spc_planogram___groupid as INLINE_POG_GROUP_ID",
	"SQ_Shortcut_to_ix_spc_planogram___plannerpoggroupid as PLANNER_POG_GROUP_ID",
	"SQ_Shortcut_to_POG_VERSION___POG_DB_KEY as PV_POG_DB_KEY",
	"SQ_Shortcut_to_POG_VERSION___POG_NM as PV_POG_NM",
	"SQ_Shortcut_to_POG_VERSION___POG_EFFECTIVE_FROM_DT as PV_POG_EFFECTIVE_FROM_DT",
	"SQ_Shortcut_to_POG_VERSION___POG_EFFECTIVE_TO_DT as PV_POG_EFFECTIVE_TO_DT",
	"SQ_Shortcut_to_POG_VERSION___DB_STATUS as PV_DB_STATUS",
	"SQ_Shortcut_to_POG_VERSION___DB_TIME as PV_DB_TIME",
	"SQ_Shortcut_to_POG_VERSION___DB_USER as PV_DB_USER",
	"SQ_Shortcut_to_POG_VERSION___POG_IMPLEMENT_DT as PV_POG_IMPLEMENT_DT",
	"SQ_Shortcut_to_POG_VERSION___POG_REVIEW_DT as PV_POG_REVIEW_DT",
	"SQ_Shortcut_to_POG_VERSION___US_FLAG as PV_US_FLAG",
	"SQ_Shortcut_to_POG_VERSION___CA_FLAG as PV_CA_FLAG",
	"SQ_Shortcut_to_POG_VERSION___PR_FLAG as PV_PR_FLAG",
	"SQ_Shortcut_to_POG_VERSION___POG_TYPE_CD as PV_POG_TYPE_CD",
	"SQ_Shortcut_to_POG_VERSION___POG_DIVISION as PV_POG_DIVISION",
	"SQ_Shortcut_to_POG_VERSION___POG_DEPT as PV_POG_DEPT",
	"SQ_Shortcut_to_POG_VERSION___WIDTH as PV_WIDTH",
	"SQ_Shortcut_to_POG_VERSION___HEIGHT as PV_HEIGHT",
	"SQ_Shortcut_to_POG_VERSION___DEPTH as PV_DEPTH",
	"SQ_Shortcut_to_POG_VERSION___ABBREV_NM as PV_ABBREV_NM",
	"SQ_Shortcut_to_POG_VERSION___DATE_CREATED as PV_DATE_CREATED",
	"SQ_Shortcut_to_POG_VERSION___DATE_MODIFIED as PV_DATE_MODIFIED",
	"SQ_Shortcut_to_POG_VERSION___DATE_PENDING as PV_DATE_PENDING",
	"SQ_Shortcut_to_POG_VERSION___DATE_EFFECTIVE as PV_DATE_EFFECTIVE",
	"SQ_Shortcut_to_POG_VERSION___POG_GROUP as PV_POG_GROUP",
	"SQ_Shortcut_to_POG_VERSION___PG_STATUS as PV_PG_STATUS",
	"SQ_Shortcut_to_POG_VERSION___PG_SCORE_PERCENT as PV_PG_SCORE_PERCENT",
	"SQ_Shortcut_to_POG_VERSION___PG_SCORE_NOTE as PV_PG_SCORE_NOTE",
	"SQ_Shortcut_to_POG_VERSION___DB_VERSION_KEY as PV_DB_VERSION_KEY",
	"SQ_Shortcut_to_POG_VERSION___PG_WARNINGS_COUNT as PV_PG_WARNINGS_COUNT",
	"SQ_Shortcut_to_POG_VERSION___PG_ERRORS_COUNT as PV_PG_ERRORS_COUNT",
	"SQ_Shortcut_to_POG_VERSION___PG_ACTION_LIST as PV_PG_ACTION_LIST",
	"SQ_Shortcut_to_POG_VERSION___ALLOCATION_GROUP as PV_ALLOCATION_GROUP",
	"SQ_Shortcut_to_POG_VERSION___ALLOCATION_SEQUENCE as PV_ALLOCATION_SEQUENCE",
	"SQ_Shortcut_to_POG_VERSION___DBPARENT_PGTEMPLATE_KEY as PV_DBPARENT_PGTEMPLATE_KEY",
	"SQ_Shortcut_to_POG_VERSION___LISTING_ONLY_FLAG as PV_LISTING_ONLY_FLAG",
	"SQ_Shortcut_to_POG_VERSION___NEW_STORE_FLAG as PV_NEW_STORE_FLAG",
	"SQ_Shortcut_to_POG_VERSION___NOT_GOING_FOWARD_FLAG as PV_NOT_GOING_FOWARD_FLAG",
	"SQ_Shortcut_to_POG_VERSION___PLANNER_PRINT_FLAG as PV_PLANNER_PRINT_FLAG",
	"SQ_Shortcut_to_POG_VERSION___OBSOLETE_FLAG as PV_OBSOLETE_FLAG",
	"SQ_Shortcut_to_POG_VERSION___FIXTURE_TYPE as PV_FIXTURE_TYPE",
	"SQ_Shortcut_to_POG_VERSION___DISPLAY_LOCATION as PV_DISPLAY_LOCATION",
	"SQ_Shortcut_to_POG_VERSION___CATEGORY_ROLE as PV_CATEGORY_ROLE",
	"SQ_Shortcut_to_POG_VERSION___POG_CHANGE_TYPE as PV_POG_CHANGE_TYPE",
	"SQ_Shortcut_to_POG_VERSION___CLUSTER_NBR as PV_CLUSTER_NBR",
	"SQ_Shortcut_to_POG_VERSION___ASO as PV_ASO",
	"SQ_Shortcut_to_POG_VERSION___VERSION_REASON as PV_VERSION_REASON",
	"SQ_Shortcut_to_POG_VERSION___VERSION_COMMENTS as PV_VERSION_COMMENTS",
	"SQ_Shortcut_to_POG_VERSION___DRIVE_AISLE as PV_DRIVE_AISLE",
	"SQ_Shortcut_to_POG_VERSION___CLUSTER_NM as PV_CLUSTER_NM",
	"SQ_Shortcut_to_POG_VERSION___PRESENTATION_MGR_NM as PV_PRESENTATION_MGR_NM",
	"SQ_Shortcut_to_POG_VERSION___PLANNER_DESC as PV_PLANNER_DESC",
	"SQ_Shortcut_to_POG_VERSION___PLANNER_YEAR as PV_PLANNER_YEAR",
	"SQ_Shortcut_to_POG_VERSION___GRPID as PV_GRPID",
	"SQ_Shortcut_to_POG_VERSION___COMBO1 as PV_COMBO1",
	"SQ_Shortcut_to_POG_VERSION___COMBO2 as PV_COMBO2",
	"SQ_Shortcut_to_POG_VERSION___COMBO3 as PV_COMBO3",
	"SQ_Shortcut_to_POG_VERSION___PG_SCENARIO_NM as PV_PG_SCENARIO_NM",
	"SQ_Shortcut_to_POG_VERSION___PG_EQUIPEMENT_SOURCE as PV_PG_EQUIPEMENT_SOURCE",
	"SQ_Shortcut_to_POG_VERSION___PG_ASSORTMENT_SOURCE as PV_PG_ASSORTMENT_SOURCE",
	"SQ_Shortcut_to_POG_VERSION___PG_PERFORMANCE_SOURCE as PV_PG_PERFORMANCE_SOURCE",
	"SQ_Shortcut_to_POG_VERSION___LOAD_DT as PV_LOAD_DT")

# COMMAND ----------

# Processing node EXP_ChangeFlag, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 66


EXP_ChangeFlag_temp = JNR_target.toDF(*["EXP_ChangeFlag_joined___" + col for col in JNR_target.columns])
 
EXP_ChangeFlag = EXP_ChangeFlag_temp \
	.withColumn("v_POG_TYPE_CD", expr("""IF (ltrim ( rtrim ( EXP_ChangeFlag_joined___Flag1 ) ) = 'INLINE', '0', IF (ltrim ( rtrim ( EXP_ChangeFlag_joined___Flag1 ) ) = 'PLANNER', '1', null))""")) \
	.withColumn("v_pog_ty_cd", expr("""IF (ltrim ( rtrim ( EXP_ChangeFlag_joined___Flag1 ) ) = 'INLINE', 'I', IF (ltrim ( rtrim ( EXP_ChangeFlag_joined___Flag1 ) ) = 'PLANNER', 'P', 'D'))""")) \
	.withColumn("v_POG_GROUP_ID", expr("""IF (EXP_ChangeFlag_joined___Flag1 = 'INLINE', EXP_ChangeFlag_joined___INLINE_POG_GROUP_ID, IF (EXP_ChangeFlag_joined___Flag1 = 'PLANNER', EXP_ChangeFlag_joined___PLANNER_POG_GROUP_ID, NULL))""")) \
	.selectExpr(
	# "EXP_ChangeFlag_joined___sys_row_id as sys_row_id",
	"EXP_ChangeFlag_joined___POG_DB_KEY as POG_DB_KEY",
	"EXP_ChangeFlag_joined___POG_NM as POG_NM",
	"EXP_ChangeFlag_joined___POG_EFFECTIVE_FROM_DT as POG_EFFECTIVE_FROM_DT",
	"EXP_ChangeFlag_joined___POG_EFFECTIVE_TO_DT as POG_EFFECTIVE_TO_DT",
	"EXP_ChangeFlag_joined___DB_STATUS as DB_STATUS",
	"EXP_ChangeFlag_joined___DB_TIME as DB_TIME",
	"EXP_ChangeFlag_joined___DB_USER as DB_USER",
	"EXP_ChangeFlag_joined___POG_IMPLEMENT_DT as POG_IMPLEMENT_DT",
	"EXP_ChangeFlag_joined___POG_REVIEW_DT as POG_REVIEW_DT",
	"EXP_ChangeFlag_joined___US_FLAG as US_FLAG",
	"EXP_ChangeFlag_joined___CA_FLAG as CA_FLAG",
	"EXP_ChangeFlag_joined___PR_FLAG as PR_FLAG",
	"v_pog_ty_cd as POG_TYPE_CD",
	"EXP_ChangeFlag_joined___POG_DIVISION as POG_DIVISION",
	"EXP_ChangeFlag_joined___POG_DEPT as POG_DEPT",
	"EXP_ChangeFlag_joined___WIDTH as WIDTH",
	"EXP_ChangeFlag_joined___HEIGHT as HEIGHT",
	"EXP_ChangeFlag_joined___DEPTH as DEPTH",
	"EXP_ChangeFlag_joined___ABBREV_NM as ABBREV_NM",
	"EXP_ChangeFlag_joined___DATE_CREATED as DATE_CREATED",
	"EXP_ChangeFlag_joined___DATE_MODIFIED as DATE_MODIFIED",
	"EXP_ChangeFlag_joined___DATE_PENDING as DATE_PENDING",
	"EXP_ChangeFlag_joined___DATE_EFFECTIVE as DATE_EFFECTIVE",
	"EXP_ChangeFlag_joined___POG_GROUP as POG_GROUP",
	"EXP_ChangeFlag_joined___PG_STATUS as PG_STATUS",
	"EXP_ChangeFlag_joined___PG_SCORE_PERCENT as PG_SCORE_PERCENT",
	"EXP_ChangeFlag_joined___PG_SCORE_NOTE as PG_SCORE_NOTE",
	"EXP_ChangeFlag_joined___DB_VERSION_KEY as DB_VERSION_KEY",
	"EXP_ChangeFlag_joined___PG_WARNINGS_COUNT as PG_WARNINGS_COUNT",
	"EXP_ChangeFlag_joined___PG_ERRORS_COUNT as PG_ERRORS_COUNT",
	"EXP_ChangeFlag_joined___PG_ACTION_LIST as PG_ACTION_LIST",
	"EXP_ChangeFlag_joined___ALLOCATION_GROUP as ALLOCATION_GROUP",
	"EXP_ChangeFlag_joined___ALLOCATION_SEQUENCE as ALLOCATION_SEQUENCE",
	"EXP_ChangeFlag_joined___DBPARENT_PGTEMPLATE_KEY as DBPARENT_PGTEMPLATE_KEY",
	"EXP_ChangeFlag_joined___LISTING_ONLY_FLAG as LISTING_ONLY_FLAG",
	"EXP_ChangeFlag_joined___NEW_STORE_FLAG as NEW_STORE_FLAG",
	"EXP_ChangeFlag_joined___NOT_GOING_FOWARD_FLAG as NOT_GOING_FOWARD_FLAG",
	"EXP_ChangeFlag_joined___PLANNER_PRINT_FLAG as PLANNER_PRINT_FLAG",
	"EXP_ChangeFlag_joined___OBSOLETE_FLAG as OBSOLETE_FLAG",
	"EXP_ChangeFlag_joined___FIXTURE_TYPE as FIXTURE_TYPE_JOIN",
	"EXP_ChangeFlag_joined___DISPLAY_LOCATION as DISPLAY_LOCATION",
	"EXP_ChangeFlag_joined___CATEGORY_ROLE as CATEGORY_ROLE",
	"EXP_ChangeFlag_joined___POG_CHANGE_TYPE as POG_CHANGE_TYPE",
	"EXP_ChangeFlag_joined___CLUSTER_NBR as CLUSTER_NBR",
	"EXP_ChangeFlag_joined___ASO as ASO",
	"EXP_ChangeFlag_joined___VERSION_REASON as VERSION_REASON",
	"EXP_ChangeFlag_joined___VERSION_COMMENTS as VERSION_COMMENTS",
	"EXP_ChangeFlag_joined___DRIVE_AISLE as DRIVE_AISLE",
	"EXP_ChangeFlag_joined___CLUSTER_NM as CLUSTER_NM",
	"EXP_ChangeFlag_joined___PRESENTATION_MGR_NM as PRESENTATION_MGR_NM",
	"EXP_ChangeFlag_joined___PLANNER_DESC as PLANNER_DESC",
	"EXP_ChangeFlag_joined___PLANNER_YEAR as PLANNER_YEAR",
	"EXP_ChangeFlag_joined___INLINE_POG_GROUP_ID as INLINE_POG_GROUP_ID",
	"EXP_ChangeFlag_joined___PLANNER_POG_GROUP_ID as PLANNER_POG_GROUP_ID",
	"v_POG_GROUP_ID as o_POG_GROUP_ID",
	"EXP_ChangeFlag_joined___COMBO1 as COMBO1",
	"EXP_ChangeFlag_joined___COMBO2 as COMBO2",
	"EXP_ChangeFlag_joined___COMBO3 as COMBO3",
	"EXP_ChangeFlag_joined___PG_SCENARIO_NM as PG_SCENARIO_NM",
	"EXP_ChangeFlag_joined___PG_EQUIPEMENT_SOURCE as PG_EQUIPEMENT_SOURCE",
	"EXP_ChangeFlag_joined___PG_ASSORTMENT_SOURCE as PG_ASSORTMENT_SOURCE",
	"EXP_ChangeFlag_joined___PG_PERFORMANCE_SOURCE as PG_PERFORMANCE_SOURCE",
	"EXP_ChangeFlag_joined___FIXTURE_TYPE_ID as FIXTURE_TYPE_ID",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (EXP_ChangeFlag_joined___PV_POG_DB_KEY IS NULL, CURRENT_TIMESTAMP, EXP_ChangeFlag_joined___PV_LOAD_DT) as LOAD_DT",
	"IF (EXP_ChangeFlag_joined___PV_POG_DB_KEY IS NULL, 2, IF (MD5 ( concat ( EXP_ChangeFlag_joined___POG_NM , EXP_ChangeFlag_joined___POG_EFFECTIVE_FROM_DT , EXP_ChangeFlag_joined___POG_EFFECTIVE_TO_DT , EXP_ChangeFlag_joined___DB_STATUS , EXP_ChangeFlag_joined___DB_TIME , EXP_ChangeFlag_joined___DB_USER , EXP_ChangeFlag_joined___POG_IMPLEMENT_DT , EXP_ChangeFlag_joined___POG_REVIEW_DT , EXP_ChangeFlag_joined___US_FLAG , EXP_ChangeFlag_joined___CA_FLAG , EXP_ChangeFlag_joined___PR_FLAG , v_pog_ty_cd , EXP_ChangeFlag_joined___POG_DIVISION , EXP_ChangeFlag_joined___POG_DEPT , EXP_ChangeFlag_joined___WIDTH , EXP_ChangeFlag_joined___HEIGHT , EXP_ChangeFlag_joined___DEPTH , EXP_ChangeFlag_joined___ABBREV_NM , EXP_ChangeFlag_joined___DATE_CREATED , EXP_ChangeFlag_joined___DATE_MODIFIED , EXP_ChangeFlag_joined___DATE_PENDING , EXP_ChangeFlag_joined___DATE_EFFECTIVE , EXP_ChangeFlag_joined___POG_GROUP , EXP_ChangeFlag_joined___PG_STATUS , EXP_ChangeFlag_joined___PG_SCORE_PERCENT , EXP_ChangeFlag_joined___PG_SCORE_NOTE , EXP_ChangeFlag_joined___DB_VERSION_KEY , EXP_ChangeFlag_joined___PG_WARNINGS_COUNT , EXP_ChangeFlag_joined___PG_ERRORS_COUNT , EXP_ChangeFlag_joined___PG_ACTION_LIST , EXP_ChangeFlag_joined___ALLOCATION_GROUP , EXP_ChangeFlag_joined___ALLOCATION_SEQUENCE , EXP_ChangeFlag_joined___DBPARENT_PGTEMPLATE_KEY , EXP_ChangeFlag_joined___LISTING_ONLY_FLAG , EXP_ChangeFlag_joined___NEW_STORE_FLAG , EXP_ChangeFlag_joined___NOT_GOING_FOWARD_FLAG , EXP_ChangeFlag_joined___PLANNER_PRINT_FLAG , EXP_ChangeFlag_joined___OBSOLETE_FLAG , EXP_ChangeFlag_joined___FIXTURE_TYPE , EXP_ChangeFlag_joined___DISPLAY_LOCATION , EXP_ChangeFlag_joined___CATEGORY_ROLE , EXP_ChangeFlag_joined___POG_CHANGE_TYPE , EXP_ChangeFlag_joined___CLUSTER_NBR , EXP_ChangeFlag_joined___ASO , EXP_ChangeFlag_joined___VERSION_REASON , EXP_ChangeFlag_joined___VERSION_COMMENTS , EXP_ChangeFlag_joined___DRIVE_AISLE , EXP_ChangeFlag_joined___CLUSTER_NM , EXP_ChangeFlag_joined___PRESENTATION_MGR_NM , EXP_ChangeFlag_joined___PLANNER_DESC , EXP_ChangeFlag_joined___PLANNER_YEAR , v_POG_GROUP_ID , EXP_ChangeFlag_joined___COMBO1 , EXP_ChangeFlag_joined___COMBO2 , EXP_ChangeFlag_joined___COMBO3 , EXP_ChangeFlag_joined___PG_SCENARIO_NM , EXP_ChangeFlag_joined___PG_EQUIPEMENT_SOURCE , EXP_ChangeFlag_joined___PG_ASSORTMENT_SOURCE , EXP_ChangeFlag_joined___PG_PERFORMANCE_SOURCE ) ) != MD5 (concat( EXP_ChangeFlag_joined___PV_POG_NM , EXP_ChangeFlag_joined___PV_POG_EFFECTIVE_FROM_DT , EXP_ChangeFlag_joined___PV_POG_EFFECTIVE_TO_DT , EXP_ChangeFlag_joined___PV_DB_STATUS , EXP_ChangeFlag_joined___PV_DB_TIME , EXP_ChangeFlag_joined___PV_DB_USER , EXP_ChangeFlag_joined___PV_POG_IMPLEMENT_DT , EXP_ChangeFlag_joined___PV_POG_REVIEW_DT , EXP_ChangeFlag_joined___PV_US_FLAG , EXP_ChangeFlag_joined___PV_CA_FLAG , EXP_ChangeFlag_joined___PV_PR_FLAG , EXP_ChangeFlag_joined___PV_POG_TYPE_CD , EXP_ChangeFlag_joined___PV_POG_DIVISION , EXP_ChangeFlag_joined___PV_POG_DEPT , EXP_ChangeFlag_joined___PV_WIDTH  , EXP_ChangeFlag_joined___PV_HEIGHT  , EXP_ChangeFlag_joined___PV_DEPTH  , EXP_ChangeFlag_joined___PV_ABBREV_NM , EXP_ChangeFlag_joined___PV_DATE_CREATED , EXP_ChangeFlag_joined___PV_DATE_MODIFIED , EXP_ChangeFlag_joined___PV_DATE_PENDING , EXP_ChangeFlag_joined___PV_DATE_EFFECTIVE , EXP_ChangeFlag_joined___PV_POG_GROUP , EXP_ChangeFlag_joined___PV_PG_STATUS , EXP_ChangeFlag_joined___PV_PG_SCORE_PERCENT , EXP_ChangeFlag_joined___PV_PG_SCORE_NOTE , EXP_ChangeFlag_joined___PV_DB_VERSION_KEY , EXP_ChangeFlag_joined___PV_PG_WARNINGS_COUNT , EXP_ChangeFlag_joined___PV_PG_ERRORS_COUNT , EXP_ChangeFlag_joined___PV_PG_ACTION_LIST , EXP_ChangeFlag_joined___PV_ALLOCATION_GROUP , EXP_ChangeFlag_joined___PV_ALLOCATION_SEQUENCE , EXP_ChangeFlag_joined___PV_DBPARENT_PGTEMPLATE_KEY , EXP_ChangeFlag_joined___PV_LISTING_ONLY_FLAG , EXP_ChangeFlag_joined___PV_NEW_STORE_FLAG , EXP_ChangeFlag_joined___PV_NOT_GOING_FOWARD_FLAG , EXP_ChangeFlag_joined___PV_PLANNER_PRINT_FLAG , EXP_ChangeFlag_joined___PV_OBSOLETE_FLAG , EXP_ChangeFlag_joined___PV_FIXTURE_TYPE , EXP_ChangeFlag_joined___PV_DISPLAY_LOCATION , EXP_ChangeFlag_joined___PV_CATEGORY_ROLE , EXP_ChangeFlag_joined___PV_POG_CHANGE_TYPE , EXP_ChangeFlag_joined___PV_CLUSTER_NBR , EXP_ChangeFlag_joined___PV_ASO , EXP_ChangeFlag_joined___PV_VERSION_REASON , EXP_ChangeFlag_joined___PV_VERSION_COMMENTS , EXP_ChangeFlag_joined___PV_DRIVE_AISLE , EXP_ChangeFlag_joined___PV_CLUSTER_NM , EXP_ChangeFlag_joined___PV_PRESENTATION_MGR_NM , EXP_ChangeFlag_joined___PV_PLANNER_DESC , EXP_ChangeFlag_joined___PV_PLANNER_YEAR , EXP_ChangeFlag_joined___PV_GRPID , EXP_ChangeFlag_joined___PV_COMBO1 , EXP_ChangeFlag_joined___PV_COMBO2 , EXP_ChangeFlag_joined___PV_COMBO3 , EXP_ChangeFlag_joined___PV_PG_SCENARIO_NM , EXP_ChangeFlag_joined___PV_PG_EQUIPEMENT_SOURCE , EXP_ChangeFlag_joined___PV_PG_ASSORTMENT_SOURCE , EXP_ChangeFlag_joined___PV_PG_PERFORMANCE_SOURCE )), 1, 0)) as ChangeFlag",
	"null as NEWFIELD",
	"null as NEWFIELD1",
	"null as NEWFIELD2",
	"null as NEWFIELD3",
	"null as NEWFIELD4",
	"null as NEWFIELD5"
)

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 70

# for each involved DataFrame, append the dataframe name to each column
FIL_Existing_No_Changes_temp = EXP_ChangeFlag.toDF(*["FIL_Existing_No_Changes___" + col for col in EXP_ChangeFlag.columns])
LKP_POG_FIXTURE_temp = LKP_POG_FIXTURE_SRC.toDF(*["LKP_POG_FIXTURE___" + col for col in LKP_POG_FIXTURE_SRC.columns])

# Joining dataframes FIL_Existing_No_Changes, LKP_POG_FIXTURE to form EXPTRANS
EXPTRANS_joined = FIL_Existing_No_Changes_temp.join(LKP_POG_FIXTURE_temp, lower(FIL_Existing_No_Changes_temp.FIL_Existing_No_Changes___FIXTURE_TYPE_JOIN) == lower(LKP_POG_FIXTURE_temp.LKP_POG_FIXTURE___FIXTURE_DESC), 'left')
EXPTRANS = EXPTRANS_joined.selectExpr(
	"FIL_Existing_No_Changes___POG_DB_KEY as POG_DB_KEY",
	"FIL_Existing_No_Changes___POG_NM as POG_NM",
	"FIL_Existing_No_Changes___POG_EFFECTIVE_FROM_DT as POG_EFFECTIVE_FROM_DT",
	"FIL_Existing_No_Changes___POG_EFFECTIVE_TO_DT as POG_EFFECTIVE_TO_DT",
	"FIL_Existing_No_Changes___DB_STATUS as DB_STATUS",
	"FIL_Existing_No_Changes___DB_TIME as DB_TIME",
	"FIL_Existing_No_Changes___DB_USER as DB_USER",
	"FIL_Existing_No_Changes___POG_IMPLEMENT_DT as POG_IMPLEMENT_DT",
	"FIL_Existing_No_Changes___POG_REVIEW_DT as POG_REVIEW_DT",
	"FIL_Existing_No_Changes___US_FLAG as US_FLAG",
	"FIL_Existing_No_Changes___CA_FLAG as CA_FLAG",
	"FIL_Existing_No_Changes___PR_FLAG as PR_FLAG",
	"FIL_Existing_No_Changes___POG_TYPE_CD as POG_TYPE_CD",
	"FIL_Existing_No_Changes___POG_DIVISION as POG_DIVISION",
	"FIL_Existing_No_Changes___POG_DEPT as POG_DEPT",
	"FIL_Existing_No_Changes___WIDTH as WIDTH",
	"FIL_Existing_No_Changes___HEIGHT as HEIGHT",
	"FIL_Existing_No_Changes___DEPTH as DEPTH",
	"FIL_Existing_No_Changes___ABBREV_NM as ABBREV_NM",
	"FIL_Existing_No_Changes___DATE_CREATED as DATE_CREATED",
	"FIL_Existing_No_Changes___DATE_MODIFIED as DATE_MODIFIED",
	"FIL_Existing_No_Changes___DATE_PENDING as DATE_PENDING",
	"FIL_Existing_No_Changes___DATE_EFFECTIVE as DATE_EFFECTIVE",
	"FIL_Existing_No_Changes___POG_GROUP as POG_GROUP",
	"FIL_Existing_No_Changes___PG_STATUS as PG_STATUS",
	"FIL_Existing_No_Changes___PG_SCORE_PERCENT as PG_SCORE_PERCENT",
	"FIL_Existing_No_Changes___PG_SCORE_NOTE as PG_SCORE_NOTE",
	"FIL_Existing_No_Changes___DB_VERSION_KEY as DB_VERSION_KEY",
	"FIL_Existing_No_Changes___PG_WARNINGS_COUNT as PG_WARNINGS_COUNT",
	"FIL_Existing_No_Changes___PG_ERRORS_COUNT as PG_ERRORS_COUNT",
	"FIL_Existing_No_Changes___PG_ACTION_LIST as PG_ACTION_LIST",
	"FIL_Existing_No_Changes___ALLOCATION_GROUP as ALLOCATION_GROUP",
	"FIL_Existing_No_Changes___ALLOCATION_SEQUENCE as ALLOCATION_SEQUENCE",
	"FIL_Existing_No_Changes___DBPARENT_PGTEMPLATE_KEY as DBPARENT_PGTEMPLATE_KEY",
	"FIL_Existing_No_Changes___LISTING_ONLY_FLAG as LISTING_ONLY_FLAG",
	"FIL_Existing_No_Changes___NEW_STORE_FLAG as NEW_STORE_FLAG",
	"FIL_Existing_No_Changes___NOT_GOING_FOWARD_FLAG as NOT_GOING_FOWARD_FLAG",
	"FIL_Existing_No_Changes___PLANNER_PRINT_FLAG as PLANNER_PRINT_FLAG",
	"FIL_Existing_No_Changes___OBSOLETE_FLAG as OBSOLETE_FLAG",
	"FIL_Existing_No_Changes___FIXTURE_TYPE_JOIN as FIXTURE_TYPE_DESC",
	"FIL_Existing_No_Changes___DISPLAY_LOCATION as DISPLAY_LOCATION",
	"FIL_Existing_No_Changes___CATEGORY_ROLE as CATEGORY_ROLE",
	"FIL_Existing_No_Changes___POG_CHANGE_TYPE as POG_CHANGE_TYPE",
	"FIL_Existing_No_Changes___CLUSTER_NBR as CLUSTER_NBR",
	"FIL_Existing_No_Changes___ASO as ASO",
	"FIL_Existing_No_Changes___VERSION_REASON as VERSION_REASON",
	"FIL_Existing_No_Changes___VERSION_COMMENTS as VERSION_COMMENTS",
	"FIL_Existing_No_Changes___DRIVE_AISLE as DRIVE_AISLE",
	"FIL_Existing_No_Changes___CLUSTER_NM as CLUSTER_NM",
	"FIL_Existing_No_Changes___PRESENTATION_MGR_NM as PRESENTATION_MGR_NM",
	"FIL_Existing_No_Changes___PLANNER_DESC as PLANNER_DESC",
	"FIL_Existing_No_Changes___PLANNER_YEAR as PLANNER_YEAR",
	"FIL_Existing_No_Changes___o_POG_GROUP_ID as GRPID",
	"FIL_Existing_No_Changes___COMBO1 as COMBO1",
	"FIL_Existing_No_Changes___COMBO2 as COMBO2",
	"FIL_Existing_No_Changes___COMBO3 as COMBO3",
	"FIL_Existing_No_Changes___PG_SCENARIO_NM as PG_SCENARIO_NM",
	"FIL_Existing_No_Changes___PG_EQUIPEMENT_SOURCE as PG_EQUIPEMENT_SOURCE",
	"FIL_Existing_No_Changes___PG_ASSORTMENT_SOURCE as PG_ASSORTMENT_SOURCE",
	"FIL_Existing_No_Changes___PG_PERFORMANCE_SOURCE as PG_PERFORMANCE_SOURCE",
	"FIL_Existing_No_Changes___UPDATE_DT as UPDATE_DT",
	"FIL_Existing_No_Changes___LOAD_DT as LOAD_DT",
	"FIL_Existing_No_Changes___ChangeFlag as ChangeFlag",
	"FIL_Existing_No_Changes___NEWFIELD as NEWFIELD",
	"FIL_Existing_No_Changes___NEWFIELD1 as NEWFIELD1",
	"FIL_Existing_No_Changes___NEWFIELD2 as NEWFIELD2",
	"FIL_Existing_No_Changes___NEWFIELD3 as NEWFIELD3",
	"FIL_Existing_No_Changes___NEWFIELD4 as NEWFIELD4",
	"FIL_Existing_No_Changes___NEWFIELD5 as NEWFIELD5",
	"LKP_POG_FIXTURE___FIXTURE_TYPE as FIXTURE_TYPE_ID",
    "if(FIL_Existing_No_Changes___ChangeFlag == 2, 0, 1) as pyspark_data_action")

# COMMAND ----------

# Processing node Shortcut_to_POG_VERSION, type TARGET 
# COLUMN COUNT: 63


Shortcut_to_POG_VERSION = EXPTRANS.selectExpr(
    "CAST(POG_DB_KEY AS INT) as POG_DBKEY",
    "CAST(POG_NM AS STRING) as POG_NM",
    "CAST(POG_EFFECTIVE_FROM_DT AS TIMESTAMP) as POG_EFFECTIVE_FROM_DT",
    "CAST(POG_EFFECTIVE_TO_DT AS TIMESTAMP) as POG_EFFECTIVE_TO_DT",
    "CAST(DB_STATUS AS INT) as DB_STATUS",
    "CAST(DB_TIME AS TIMESTAMP) as DB_TIME",
    "CAST(DB_USER AS STRING) as DB_USER",
    "CAST(POG_IMPLEMENT_DT AS TIMESTAMP) as POG_IMPLEMENT_DT",
    "CAST(POG_REVIEW_DT AS TIMESTAMP) as POG_REVIEW_DT",
    "CAST(US_FLAG AS STRING) as US_FLAG",
    "CAST(CA_FLAG AS STRING) as CA_FLAG",
    "CAST(PR_FLAG AS STRING) as PR_FLAG",
    "CAST(POG_TYPE_CD AS STRING) as POG_TYPE_CD",
    "CAST(POG_DIVISION AS STRING) as POG_DIVISION",
    "CAST(POG_DEPT AS STRING) as POG_DEPT",
    "CAST(WIDTH AS FLOAT) as WIDTH",
    "CAST(HEIGHT AS FLOAT) as HEIGHT",
    "CAST(DEPTH AS FLOAT) as DEPTH",
    "CAST(ABBREV_NM AS STRING) as ABBREV_NM",
    "CAST(DATE_CREATED AS TIMESTAMP) as DATE_CREATED",
    "CAST(DATE_MODIFIED AS TIMESTAMP) as DATE_MODIFIED",
    "CAST(DATE_PENDING AS TIMESTAMP) as DATE_PENDING",
    "CAST(DATE_EFFECTIVE AS TIMESTAMP) as DATE_EFFECTIVE",
    "CAST(GRPID AS DOUBLE) as POG_GROUP_ID",
    "CAST(POG_GROUP AS STRING) as POG_GROUP_DESC",
    "CAST(PG_STATUS AS BIGINT) as PG_STATUS",
    "CAST(PG_SCORE_PERCENT AS BIGINT) as PG_SCORE_PERCENT",
    "CAST(PG_SCORE_NOTE AS STRING) as PG_SCORE_NOTE",
    "CAST(DB_VERSION_KEY AS BIGINT) as DB_VERSION_KEY",
    "CAST(PG_WARNINGS_COUNT AS BIGINT) as PG_WARNINGS_COUNT",
    "CAST(PG_ERRORS_COUNT AS BIGINT) as PG_ERRORS_COUNT",
    "CAST(PG_ACTION_LIST AS STRING) as PG_ACTION_LIST",
    "CAST(ALLOCATION_GROUP AS STRING) as ALLOCATION_GROUP",
    "CAST(ALLOCATION_SEQUENCE AS BIGINT) as ALLOCATION_SEQUENCE",
    "CAST(DBPARENT_PGTEMPLATE_KEY AS BIGINT) as DBPARENT_PGTEMPLATE_KEY",
    "CAST(LISTING_ONLY_FLAG AS BIGINT) as LISTING_ONLY_FLAG",
    "CAST(NEW_STORE_FLAG AS BIGINT) as NEW_STORE_FLAG",
    "CAST(NOT_GOING_FOWARD_FLAG AS BIGINT) as NOT_GOING_FOWARD_FLAG",
    "CAST(PLANNER_PRINT_FLAG AS BIGINT) as PLANNER_PRINT_FLAG",
    "CAST(OBSOLETE_FLAG AS BIGINT) as OBSOLETE_FLAG",
    "CAST(FIXTURE_TYPE_ID AS BIGINT) as FIXTURE_TYPE_ID",
    "CAST(FIXTURE_TYPE_DESC AS STRING) as FIXTURE_TYPE_DESC",
    "CAST(DISPLAY_LOCATION AS STRING) as DISPLAY_LOCATION",
    "CAST(CATEGORY_ROLE AS STRING) as CATEGORY_ROLE",
    "CAST(POG_CHANGE_TYPE AS STRING) as POG_CHANGE_TYPE",
    "CAST(CLUSTER_NBR AS DOUBLE) as CLUSTER_NBR",
    "CAST(ASO AS DOUBLE) as ASO",
    "CAST(VERSION_REASON AS STRING) as POG_VERSION_REASON",
    "CAST(VERSION_COMMENTS AS STRING) as POG_VERSION_COMMENTS",
    "CAST(DRIVE_AISLE AS STRING) as DRIVE_AISLE",
    "CAST(CLUSTER_NM AS STRING) as CLUSTER_NM",
    "CAST(PRESENTATION_MGR_NM AS STRING) as PRESENTATION_MGR_NM",
    "CAST(PLANNER_DESC AS STRING) as PLANNER_DESC",
    "CAST(PLANNER_YEAR AS STRING) as PLANNER_YEAR",
    "CAST(COMBO1 AS DOUBLE) as COMBO1",
    "CAST(COMBO2 AS DOUBLE) as COMBO2",
    "CAST(COMBO3 AS DOUBLE) as COMBO3",
    "CAST(PG_SCENARIO_NM AS STRING) as PG_SCENARIO_NM",
    "CAST(PG_EQUIPEMENT_SOURCE AS STRING) as PG_EQUIPEMENT_SOURCE",
    "LTRIM(RTRIM(CAST(PG_ASSORTMENT_SOURCE AS STRING))) as PG_ASSORTMENT_SOURCE",
    "CAST(PG_PERFORMANCE_SOURCE AS STRING) as PG_PERFORMANCE_SOURCE",
    "CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
    "CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
    "pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.POG_DBKEY = target.POG_DBKEY"""
	refined_perf_table = f"{legacy}.POG_VERSION"
	executeMerge(Shortcut_to_POG_VERSION, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("POG_VERSION", "POG_VERSION", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("POG_VERSION", "POG_VERSION","Failed",str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


